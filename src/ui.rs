use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, ListState, Paragraph, Row, Table, TableState, Tabs,
        Wrap,
    },
};
use std::{fmt, time::Duration};

use crate::{
    config::Bitrate,
    downloads::DownloadManager,
    library::Library,
    oauth::DeviceCode,
    queue::{PlaybackMode, PlaybackQueue},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum View {
    Library,
    Queue,
}

/// Read-only snapshot of everything the ready screen renders.
pub struct ReadyScreen<'a> {
    pub library: &'a Library,
    pub filtered_track_ids: &'a [u64],
    pub selected: usize,
    pub queue: &'a PlaybackQueue,
    pub queue_selected: usize,
    pub active_view: View,
    pub downloads: &'a DownloadManager,
    pub status_line: &'a str,
    /// Dynamic audio-device status, independent from playback intent.
    pub audio_warning: Option<&'a str>,
    pub playback: PlaybackSummary,
    pub playback_bitrate: Bitrate,
    pub playback_mode: PlaybackMode,
    pub download_bitrate: Bitrate,
    /// `Some` while the search prompt is open.
    pub search_input: Option<&'a str>,
}

pub struct PlaybackSummary {
    pub label: String,
    pub state: PlaybackState,
    pub elapsed: Option<Duration>,
    pub duration: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaybackState {
    Stopped,
    WaitingForAudio,
    Loading,
    Playing,
    Paused,
}

impl fmt::Display for PlaybackState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Stopped => "Stopped",
            Self::WaitingForAudio => "Waiting for audio",
            Self::Loading => "Loading",
            Self::Playing => "Playing",
            Self::Paused => "Paused",
        };
        formatter.write_str(label)
    }
}

pub fn login_screen(frame: &mut Frame<'_>, input: &str, status_line: &str) {
    let text = vec![
        Line::from("Enter your iBroadcast OAuth client_id"),
        Line::from("Create one in media.ibroadcast.com: side menu -> Apps -> developer"),
        Line::from("No code is required; copy the generated client_id here."),
        Line::from(""),
        Line::from(input),
        Line::from(""),
        Line::from(status_line),
        Line::from(""),
        Line::from("Enter to continue, q to quit"),
    ];
    frame.render_widget(
        centered_paragraph("Login", text),
        centered_rect(78, 13, frame.area()),
    );
}

pub fn message_screen(frame: &mut Frame<'_>, title: &'static str, message: &'static str) {
    frame.render_widget(
        centered_paragraph(title, vec![Line::from(message)]),
        centered_rect(70, 7, frame.area()),
    );
}

pub fn authorizing_screen(frame: &mut Frame<'_>, device_code: &DeviceCode) {
    let mut lines = vec![
        Line::from("Open this URL and approve the app:"),
        Line::from(device_code.verification_uri.as_str()),
        Line::from(""),
        Line::from(vec![
            Span::raw("Code: "),
            Span::styled(
                &device_code.user_code,
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];
    if let Some(complete) = &device_code.verification_uri_complete {
        lines.push(Line::from(""));
        lines.push(Line::from("Direct URL:"));
        lines.push(Line::from(complete.as_str()));
    }
    lines.push(Line::from(""));
    lines.push(Line::from("Waiting for authorization... q quits"));
    frame.render_widget(
        centered_paragraph("Authorize iBroadcast", lines),
        centered_rect(80, 13, frame.area()),
    );
}

pub fn error_screen(frame: &mut Frame<'_>, message: &str) {
    let lines = vec![
        Line::from("Something failed:"),
        Line::from(""),
        Line::from(message),
        Line::from(""),
        Line::from("Press l to log in again, q to quit"),
    ];
    frame.render_widget(
        centered_paragraph("Error", lines),
        centered_rect(80, 11, frame.area()),
    );
}

pub fn ready_screen(frame: &mut Frame<'_>, screen: &ReadyScreen<'_>) {
    let status_height = if screen.audio_warning.is_some() { 7 } else { 6 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(status_height),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let selected_tab = match screen.active_view {
        View::Library => 0,
        View::Queue => 1,
    };
    let tabs = Tabs::new(vec!["Library", "Queue"])
        .select(selected_tab)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("iBroadcast TUI"),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, chunks[0]);

    match screen.active_view {
        View::Library => draw_library(frame, chunks[1], screen),
        View::Queue => draw_queue(frame, chunks[1], screen),
    }

    let mut status_lines = vec![Line::from(screen.status_line)];
    if let Some(warning) = screen.audio_warning {
        status_lines.push(Line::from(Span::styled(
            format!("Audio: {warning}"),
            Style::default().fg(Color::Yellow),
        )));
    }
    status_lines.extend([
        Line::from(playback_status_line(&screen.playback)),
        Line::from(format!(
            "Mode: {} | Playback bitrate: {} | Download bitrate: {}",
            screen.playback_mode, screen.playback_bitrate, screen.download_bitrate
        )),
        Line::from(format!(
            "Local files: {} | Downloads running: {}",
            screen.downloads.local_count(),
            screen.downloads.running_count()
        )),
    ]);
    let status =
        Paragraph::new(status_lines).block(Block::default().borders(Borders::ALL).title("Status"));
    frame.render_widget(Clear, chunks[2]);
    frame.render_widget(status, chunks[2]);

    let help = if let Some(search) = screen.search_input {
        format!("Search: {search}")
    } else {
        help_for_view(screen.active_view).to_owned()
    };
    frame.render_widget(Clear, chunks[3]);
    frame.render_widget(Paragraph::new(help), chunks[3]);

    if let Some(search) = screen.search_input {
        draw_search_popup(frame, search);
    }
}

fn help_for_view(view: View) -> &'static str {
    match view {
        View::Library => {
            "/ search | Enter play | a/A add | d/D download | x delete | b/B bitrate | q quit"
        }
        View::Queue => {
            "Enter play | n/p next/prev | x remove | [/ ] move | C clear | m mode | q quit"
        }
    }
}

fn draw_library(frame: &mut Frame<'_>, area: Rect, screen: &ReadyScreen<'_>) {
    let library = screen.library;
    let rows = screen.filtered_track_ids.iter().filter_map(|track_id| {
        let track = library.tracks.get(track_id)?;
        Some(Row::new(vec![
            screen.downloads.storage_label(*track_id).to_owned(),
            track.track.to_string(),
            track.title.clone(),
            library.artist_name(track.artist_id).to_owned(),
            library.album_name(track.album_id).to_owned(),
            track.duration_label(),
        ]))
    });
    let mut state = TableState::default();
    if !screen.filtered_track_ids.is_empty() {
        state.select(Some(
            screen.selected.min(screen.filtered_track_ids.len() - 1),
        ));
    }
    let table = Table::new(
        rows,
        [
            Constraint::Length(11),
            Constraint::Length(5),
            Constraint::Percentage(30),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Length(7),
        ],
    )
    .header(
        Row::new(vec!["State", "#", "Title", "Artist", "Album", "Time"])
            .style(Style::default().fg(Color::Yellow)),
    )
    .block(Block::default().borders(Borders::ALL).title(format!(
        "Library ({}/{})",
        screen.filtered_track_ids.len(),
        library.tracks.len()
    )))
    .row_highlight_style(Style::default().bg(Color::DarkGray));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_queue(frame: &mut Frame<'_>, area: Rect, screen: &ReadyScreen<'_>) {
    let items = screen
        .queue
        .tracks()
        .iter()
        .enumerate()
        .map(|(index, track_id)| {
            let marker = if screen.queue.current_index() == Some(index) {
                ">"
            } else {
                " "
            };
            ListItem::new(format!(
                "{marker} {}",
                screen.library.track_label(*track_id)
            ))
        });
    let mut state = ListState::default();
    if !screen.queue.is_empty() {
        state.select(Some(screen.queue_selected.min(screen.queue.len() - 1)));
    }
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Playback Queue"),
        )
        .highlight_style(Style::default().bg(Color::DarkGray));
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_search_popup(frame: &mut Frame<'_>, search: &str) {
    let area = centered_rect(60, 5, frame.area());
    frame.render_widget(Clear, area);
    let paragraph = Paragraph::new(search)
        .block(Block::default().borders(Borders::ALL).title("Search"))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn playback_status_line(playback: &PlaybackSummary) -> String {
    let mut line = format!("Now: {} | {}", playback.label, playback.state);
    if let (Some(elapsed), Some(duration)) = (playback.elapsed, playback.duration) {
        line.push_str(&format!(
            " | {} / {}",
            duration_label(elapsed),
            duration_label(duration)
        ));
    }
    line
}

fn duration_label(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

fn centered_paragraph<'a>(title: &'static str, lines: Vec<Line<'a>>) -> Paragraph<'a> {
    Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true })
}

fn centered_rect(width_percent: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(height),
            Constraint::Min(0),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{PlaybackState, PlaybackSummary, playback_status_line};

    #[test]
    fn playback_line_includes_state_and_time() {
        let playback = PlaybackSummary {
            label: "Artist - Song".to_owned(),
            state: PlaybackState::Playing,
            elapsed: Some(Duration::from_secs(65)),
            duration: Some(Duration::from_secs(189)),
        };

        assert_eq!(
            playback_status_line(&playback),
            "Now: Artist - Song | Playing | 1:05 / 3:09"
        );
    }

    #[test]
    fn playback_line_handles_missing_track() {
        let playback = PlaybackSummary {
            label: "Nothing playing".to_owned(),
            state: PlaybackState::Stopped,
            elapsed: None,
            duration: None,
        };

        assert_eq!(
            playback_status_line(&playback),
            "Now: Nothing playing | Stopped"
        );
    }
}
