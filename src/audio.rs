use std::{
    mem::ManuallyDrop,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rodio::cpal::{self, DeviceId, traits::DeviceTrait};
use tokio::{
    sync::{mpsc, oneshot},
    task::{self, JoinHandle},
};

use crate::{
    error::Result,
    player::{AudioOutput, AudioStreamEvent, SinkEpoch, default_output_device_id},
};

/// A lossless snapshot of one attempt to identify the system default output.
///
/// `Unavailable` and `Failed` are deliberately distinct: a transient identity
/// lookup failure must not tear down an otherwise healthy output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultOutputProbe {
    Available(DeviceId),
    Unavailable,
    Failed(String),
}

impl DefaultOutputProbe {
    fn query() -> Self {
        match default_output_device_id() {
            Ok(Some(device_id)) => Self::Available(device_id),
            Ok(None) => Self::Unavailable,
            Err(error) => Self::Failed(error.to_string()),
        }
    }
}

/// A best-effort identity query against the device that backs an open stream.
///
/// Unlike [`DefaultOutputProbe`], this cannot be `Unavailable`: the probe owns
/// a clone of the exact device handle selected while the stream was opened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputDeviceProbe {
    Available(DeviceId),
    Failed(String),
}

impl OutputDeviceProbe {
    fn query(device: &cpal::Device) -> Self {
        match device.id() {
            Ok(device_id) => Self::Available(device_id),
            Err(error) => Self::Failed(error.to_string()),
        }
    }
}

pub enum AudioWorkerEvent {
    OpenFinished {
        attempt_id: u64,
        result: Result<AudioOutput>,
        observed_default: DefaultOutputProbe,
    },
    ProbeFinished {
        probe_id: u64,
        sink_epoch: Option<SinkEpoch>,
        opened_device: Option<OutputDeviceProbe>,
        result: DefaultOutputProbe,
    },
    Stream(AudioStreamEvent),
}

enum AudioWorkerCommand {
    Open {
        attempt_id: u64,
        previous: Option<AudioOutput>,
    },
    Probe {
        probe_id: u64,
        sink_epoch: Option<SinkEpoch>,
        opened_device: Option<cpal::Device>,
    },
    Dispose(AudioOutput),
    Shutdown {
        current: Option<AudioOutput>,
        acknowledged: oneshot::Sender<()>,
    },
}

/// Handle to the one blocking worker that owns all runtime output teardown,
/// probing, and opening operations.
///
/// Keeping these operations on one worker prevents overlapping WASAPI opens
/// and guarantees that the old stream is fully dropped before its replacement
/// is created.
pub struct AudioWorker {
    commands: mpsc::UnboundedSender<AudioWorkerCommand>,
    shutting_down: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

impl AudioWorker {
    pub fn start() -> (Self, mpsc::UnboundedReceiver<AudioWorkerEvent>) {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let shutting_down = Arc::new(AtomicBool::new(false));
        let worker_shutting_down = Arc::clone(&shutting_down);
        let task = task::spawn_blocking(move || {
            let mut next_sink_epoch: SinkEpoch = 1;
            // A successfully sent candidate is leased to the App until it is
            // returned through Open/Dispose/Shutdown. This is chiefly needed
            // to make shutdown wait for an OpenFinished event already in
            // flight instead of dropping its stream on the UI thread.
            let mut leased_epoch: Option<SinkEpoch> = None;
            let mut shutdown_acknowledgement: Option<oneshot::Sender<()>> = None;
            while let Some(command) = command_rx.blocking_recv() {
                match command {
                    AudioWorkerCommand::Open {
                        attempt_id,
                        previous,
                    } => {
                        // Dropping a platform stream can join its audio thread;
                        // keep that work off the TUI thread and complete it
                        // before opening a replacement.
                        if let Some(previous) = previous {
                            return_leased_output(&mut leased_epoch, previous);
                        }

                        if worker_shutting_down.load(Ordering::Acquire) {
                            continue;
                        }

                        let sink_epoch = next_sink_epoch;
                        next_sink_epoch = next_sink_epoch.wrapping_add(1).max(1);
                        let stream_event_tx = event_tx.clone();
                        let result = AudioOutput::open_default(sink_epoch, move |event| {
                            let _ = stream_event_tx.send(AudioWorkerEvent::Stream(event));
                        });

                        // Shutdown is published atomically before its command
                        // is queued. If opening was blocking when it began,
                        // destroy the candidate here and never lease it out.
                        if worker_shutting_down.load(Ordering::Acquire) {
                            drop(result);
                            continue;
                        }

                        // Re-check after opening. If A changed to B while A was
                        // opening, the application rejects this candidate and
                        // sends it back through the worker before retrying.
                        let observed_default = DefaultOutputProbe::query();
                        let candidate_epoch = result.as_ref().ok().map(AudioOutput::sink_epoch);
                        match event_tx.send(AudioWorkerEvent::OpenFinished {
                            attempt_id,
                            result,
                            observed_default,
                        }) {
                            Ok(()) => leased_epoch = candidate_epoch,
                            Err(error) => {
                                // The failed event (and any candidate it owns)
                                // is dropped here on the blocking worker.
                                drop(error);
                                break;
                            }
                        }
                    }
                    AudioWorkerCommand::Probe {
                        probe_id,
                        sink_epoch,
                        opened_device,
                    } => {
                        let opened_device = opened_device.as_ref().map(OutputDeviceProbe::query);
                        let result = DefaultOutputProbe::query();
                        if event_tx
                            .send(AudioWorkerEvent::ProbeFinished {
                                probe_id,
                                sink_epoch,
                                opened_device,
                                result,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    AudioWorkerCommand::Dispose(output) => {
                        return_leased_output(&mut leased_epoch, output);
                    }
                    AudioWorkerCommand::Shutdown {
                        current,
                        acknowledged,
                    } => {
                        worker_shutting_down.store(true, Ordering::Release);
                        if let Some(current) = current {
                            return_leased_output(&mut leased_epoch, current);
                        }
                        shutdown_acknowledgement = Some(acknowledged);
                    }
                }

                if shutdown_can_finish(shutdown_acknowledgement.is_some(), leased_epoch) {
                    let _ = shutdown_acknowledgement
                        .take()
                        .expect("shutdown acknowledgement checked above")
                        .send(());
                    break;
                }
            }
        });

        (
            Self {
                commands: command_tx,
                shutting_down,
                _task: task,
            },
            event_rx,
        )
    }

    pub fn open(&self, attempt_id: u64, previous: Option<AudioOutput>) {
        // The worker lives as long as App. If it has unexpectedly exited,
        // playback cannot recover and panicking is preferable to silently
        // dropping a platform stream on the UI thread.
        if !send_or_drop_off_thread(
            &self.commands,
            AudioWorkerCommand::Open {
                attempt_id,
                previous,
            },
        ) {
            panic!("audio worker unexpectedly stopped");
        }
    }

    pub fn probe(
        &self,
        probe_id: u64,
        sink_epoch: Option<SinkEpoch>,
        opened_device: Option<cpal::Device>,
    ) {
        if !send_or_drop_off_thread(
            &self.commands,
            AudioWorkerCommand::Probe {
                probe_id,
                sink_epoch,
                opened_device,
            },
        ) {
            panic!("audio worker unexpectedly stopped");
        }
    }

    pub fn dispose(&self, output: AudioOutput) {
        if !send_or_drop_off_thread(&self.commands, AudioWorkerCommand::Dispose(output)) {
            panic!("audio worker unexpectedly stopped");
        }
    }

    /// Starts an orderly worker shutdown and returns a completion signal.
    ///
    /// If an `OpenFinished` candidate was already sent, acknowledgement is
    /// delayed until the application receives and returns that candidate with
    /// [`Self::dispose`].
    pub fn shutdown(&self, current: Option<AudioOutput>) -> oneshot::Receiver<()> {
        self.shutting_down.store(true, Ordering::Release);
        let (acknowledged, receiver) = oneshot::channel();
        if let Err(error) = self.commands.send(AudioWorkerCommand::Shutdown {
            current,
            acknowledged,
        }) {
            // Close the acknowledgement on this thread so a failure to create
            // the fallback drop thread cannot leave shutdown waiting forever.
            // Only the platform output itself needs off-thread destruction.
            if let AudioWorkerCommand::Shutdown {
                current,
                acknowledged,
            } = error.0
            {
                drop(acknowledged);
                if let Some(output) = current {
                    drop_off_thread(output);
                }
            }
        }
        receiver
    }
}

impl Drop for AudioWorker {
    fn drop(&mut self) {
        // Covers construction failures or unwinding paths that cannot await
        // the orderly App shutdown. An in-progress open will keep its result
        // on this blocking worker instead of publishing a new candidate.
        self.shutting_down.store(true, Ordering::Release);
    }
}

/// Sends an owned command, keeping destruction of a rejected value off the
/// caller thread when the worker has unexpectedly stopped.
fn send_or_drop_off_thread<T>(sender: &mpsc::UnboundedSender<T>, value: T) -> bool
where
    T: Send + 'static,
{
    match sender.send(value) {
        Ok(()) => true,
        Err(error) => {
            drop_off_thread(error.0);
            false
        }
    }
}

fn drop_off_thread<T>(value: T)
where
    T: Send + 'static,
{
    // If the OS refuses to create the fallback thread, dropping its closure
    // must still not destroy a platform stream on the caller thread.
    // `ManuallyDrop` deliberately leaks the rejected value in that exceptional
    // case.
    let value = ManuallyDrop::new(value);
    if let Err(error) = std::thread::Builder::new()
        .name("audio-output-drop".to_owned())
        .spawn(move || drop(ManuallyDrop::into_inner(value)))
    {
        tracing::error!(
            error = %error,
            "could not start fallback audio drop thread; leaking rejected value"
        );
    }
}

fn return_leased_output(leased_epoch: &mut Option<SinkEpoch>, output: AudioOutput) {
    clear_returned_lease(leased_epoch, output.sink_epoch());
    drop(output);
}

fn clear_returned_lease(leased_epoch: &mut Option<SinkEpoch>, returned_epoch: SinkEpoch) {
    if *leased_epoch == Some(returned_epoch) {
        *leased_epoch = None;
    }
}

fn shutdown_can_finish(shutdown_requested: bool, leased_epoch: Option<SinkEpoch>) -> bool {
    shutdown_requested && leased_epoch.is_none()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc as std_mpsc,
        thread::{self, ThreadId},
        time::Duration,
    };

    use super::*;

    struct DropProbe {
        dropped: std_mpsc::Sender<(ThreadId, Option<String>)>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let current = thread::current();
            let _ = self
                .dropped
                .send((current.id(), current.name().map(str::to_owned)));
        }
    }

    #[test]
    fn shutdown_waits_until_the_exact_candidate_lease_is_returned() {
        let mut leased_epoch = Some(41);

        assert!(!shutdown_can_finish(true, leased_epoch));
        clear_returned_lease(&mut leased_epoch, 40);
        assert_eq!(leased_epoch, Some(41));
        assert!(!shutdown_can_finish(true, leased_epoch));

        clear_returned_lease(&mut leased_epoch, 41);
        assert_eq!(leased_epoch, None);
        assert!(shutdown_can_finish(true, leased_epoch));
        assert!(!shutdown_can_finish(false, leased_epoch));
    }

    #[tokio::test]
    async fn idle_worker_acknowledges_shutdown_without_querying_a_device() {
        let (worker, _events) = AudioWorker::start();
        let acknowledged = worker.shutdown(None);

        tokio::time::timeout(std::time::Duration::from_secs(1), acknowledged)
            .await
            .expect("audio worker shutdown timed out")
            .expect("audio worker dropped its acknowledgement");
    }

    #[tokio::test]
    async fn closed_worker_cannot_leave_shutdown_acknowledgement_pending() {
        let (commands, command_rx) = mpsc::unbounded_channel();
        drop(command_rx);
        let worker = AudioWorker {
            commands,
            shutting_down: Arc::new(AtomicBool::new(false)),
            _task: tokio::spawn(async {}),
        };

        let result = tokio::time::timeout(Duration::from_secs(1), worker.shutdown(None))
            .await
            .expect("closed worker left shutdown acknowledgement pending");
        assert!(result.is_err());
    }

    #[test]
    fn failed_send_drops_the_rejected_value_on_the_named_fallback_thread() {
        let caller = thread::current().id();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        drop(command_rx);
        let (dropped_tx, dropped_rx) = std_mpsc::channel();

        assert!(!send_or_drop_off_thread(
            &command_tx,
            DropProbe {
                dropped: dropped_tx,
            },
        ));

        let (drop_thread, drop_thread_name) = dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("rejected value was not dropped");
        assert_ne!(drop_thread, caller);
        assert_eq!(drop_thread_name.as_deref(), Some("audio-output-drop"));
    }
}
