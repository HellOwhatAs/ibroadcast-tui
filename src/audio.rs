use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use rodio::cpal::DeviceId;
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

pub enum AudioWorkerEvent {
    OpenFinished {
        attempt_id: u64,
        result: Result<AudioOutput>,
        observed_default: DefaultOutputProbe,
    },
    ProbeFinished {
        probe_id: u64,
        sink_epoch: Option<SinkEpoch>,
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
                    } => {
                        let result = DefaultOutputProbe::query();
                        if event_tx
                            .send(AudioWorkerEvent::ProbeFinished {
                                probe_id,
                                sink_epoch,
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
        if self
            .commands
            .send(AudioWorkerCommand::Open {
                attempt_id,
                previous,
            })
            .is_err()
        {
            panic!("audio worker unexpectedly stopped");
        }
    }

    pub fn probe(&self, probe_id: u64, sink_epoch: Option<SinkEpoch>) {
        if self
            .commands
            .send(AudioWorkerCommand::Probe {
                probe_id,
                sink_epoch,
            })
            .is_err()
        {
            panic!("audio worker unexpectedly stopped");
        }
    }

    pub fn dispose(&self, output: AudioOutput) {
        if self
            .commands
            .send(AudioWorkerCommand::Dispose(output))
            .is_err()
        {
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
            // An unexpected worker exit must still avoid destroying a stream
            // on the caller/UI thread.
            if let AudioWorkerCommand::Shutdown {
                current: Some(output),
                ..
            } = error.0
            {
                let _ = std::thread::Builder::new()
                    .name("audio-output-drop".to_owned())
                    .spawn(move || drop(output));
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
    use super::*;

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
}
