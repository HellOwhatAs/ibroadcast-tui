# ibroadcast-tui

Cross-platform Rust TUI client for [iBroadcast](https://ibroadcast.com/).

<img alt="image" src="https://github.com/user-attachments/assets/270eee1f-20b4-43b6-848b-13c18bf15d02" />

## Features

- OAuth 2 device-code login.
- Token persistence through the system keyring, with a local fallback token file.
- iBroadcast library sync and compressed `map` decoding.
- Track browsing and search.
- Playback queue with play, pause, previous, next, volume controls, sequential/repeat/shuffle modes, and automatic advance. Tracks are queued at most once; re-adding one jumps to its existing entry.
- Low-latency progressive playback through an in-memory buffer; playback does not write audio to disk.
- All streaming bitrates: 128 kbps and `orig` stream as plain files, while 96/192/256/320 kbps use the server's HLS endpoints (MPEG-TS segments demuxed to AAC in-process), the same way the official web player requests them.
- Single-track and visible-list downloads integrated into the Library view.
- Streaming URL generation from official iBroadcast `Streaming` API rules.
- Network work runs in background tasks; the UI never blocks on the network, and network errors surface in the status line instead of exiting the app.

## Run

```powershell
cargo run -- --client-id <your_ibroadcast_client_id>
```

You can also set `IBROADCAST_CLIENT_ID`. If your iBroadcast developer app
requires a secret, set `IBROADCAST_CLIENT_SECRET` as well. The secret is used
only at runtime and is not written to `config.toml`.

Useful options:

```powershell
cargo run -- --download-dir C:\Music\iBroadcast --bitrate 320 --log-level info
```

The first launch asks you to authorize the app in a browser. Configuration is saved under the system config directory in `ibroadcast-tui/config.toml`.
Warnings/errors, or the level set by `--log-level`, are written to `ibroadcast-tui/ibroadcast-tui.log` in that same config directory so background decoder logs cannot corrupt the terminal UI. The file is capped at roughly 1 MiB.
Tokens are saved to the system keyring when possible and also to `ibroadcast-tui/tokens` as a fallback so login survives keyring read failures. The fallback token file is sensitive; use `L` in the TUI to log out and delete it.

## Getting a Client ID

You do not need to write code to get a `client_id`:

1. Open `https://media.ibroadcast.com/` and sign in.
2. Open the side menu.
3. Click `Apps`.
4. Click `developer` near the bottom.
5. Create a new app and copy the generated `client_id`.

The TUI uses OAuth device-code login after that, so your account password is entered only on iBroadcast's website, not in this terminal app.
For token polling, the client tries the RFC 8628 device-code grant type and falls back to iBroadcast's documented `device_code` value if the server rejects the first form.
The developer page also generates a `client_secret`; most device-code setups only need the `client_id`, but this client can send the secret when `IBROADCAST_CLIENT_SECRET` or `--client-secret` is provided. The secret is used only at runtime and is never written to `config.toml`.

## Configuration

`config.toml` lives in the system config directory under `ibroadcast-tui/`. All fields are optional:

- `client_id`: saved after the first login.
- `download_dir`: defaults to your Music (or Downloads) folder under `iBroadcast/`.
- `playback_bitrate`: `"96"` / `"128"` / `"192"` / `"256"` / `"320"` / `"orig"`. When omitted, the account preference reported by the iBroadcast server is used. Bitrates other than 128 kbps and `orig` require an iBroadcast Premium account.
- `playback_mode`: `"sequential"` / `"repeat_one"` / `"repeat_all"` / `"shuffle"`; defaults to `"sequential"`.
- `download_bitrate`: `"128"` or `"orig"` (the only formats the server stores as complete files); defaults to `"orig"`. Other values fall back to `"128"`.
- `plain_token_file`: set `true` to skip the keyring and use only the fallback token file.

## Keys

- `/`: search
- `Enter`: play selected item; local files are preferred when present
- `a`: add selected Library track to Queue
- `A`: add all visible Library tracks to Queue
- `x` / `Delete`: delete selected local Library file, or remove selected Queue item
- `[` / `]`: move selected Queue item up / down
- `C`: clear Queue
- `m`: cycle playback mode: sequential, repeat one, repeat all, shuffle
- `Space`: pause/resume
- `n` / `p`: next / previous
- `b`: cycle playback bitrate
- `B`: cycle download bitrate
- `d`: download selected track
- `D`: download all visible tracks
- `L`: logout and revoke the stored token
- `+` / `-`: volume
- `Tab`: switch between Library and Queue
- `q`: quit

## Scope

This v1 is read-only for the music library. Uploads, ratings, tag editing, playlist editing, remote queue sync, and in-terminal artwork are intentionally left for later.

Playback is progressive and diskless, but the current decoder path keeps already-downloaded bytes in memory so `rodio` can satisfy its `Read + Seek` requirement. This avoids cache files and lowers first-play latency, at the cost of using up to roughly one track's worth of RAM while that track is playing.

## Architecture

- `app.rs`: controller. Owns the phase state machine (login -> authorize -> ready), maps keys to `Action`s, and reacts to `BackendEvent`s from background tasks. Input handling is fully synchronous; anything that touches the network is spawned and reports back through a channel. Stale playback events are discarded via a generation counter.
- `ui.rs`: pure rendering over read-only view structs.
- `session.rs`: an authenticated session (API client + account settings + user id). The single owner of token persistence: any call that may refresh the token syncs the token store afterwards. Shared with background tasks as `Arc<Mutex<Session>>`.
- `api.rs` / `oauth.rs` / `storage.rs`: iBroadcast JSON API, device-code OAuth flow, and token storage (keyring with plain-file fallback).
- `library.rs`: library model and compressed `map` decoding.
- `downloads.rs`: download transfers plus a `DownloadManager` that keeps the local-file index in memory, so rendering never touches the filesystem.
- `player.rs` / `progressive.rs`: rodio output wrapper with end-of-track detection, and the blocking in-memory buffer that adapts async downloads to the decoder's `Read + Seek`. Container probing for streams runs on a blocking thread, so a stalled connection never freezes the UI.
- `hls.rs`: minimal HLS client for the transcoded bitrates: playlist parsing, variant selection, and MPEG-TS demuxing into the raw ADTS AAC stream that feeds the progressive buffer. Polls the playlist while the server is still transcoding (`EXT-X-PLAYLIST-TYPE: EVENT`).
