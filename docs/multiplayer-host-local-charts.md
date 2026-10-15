# Host-provided charts in Phira multiplayer (experimental)

This fork adds **private, room-scoped local charts** to its existing multiplayer flow. It does not change the Phira login requirement.

## Run the matching server

Both the client and the server must use **protocol version 2** from `vendor/phira-mp`. Update both together. The server rejects an unsupported version byte before decoding commands and closes the connection; there is no version-1 fallback. **Unmodified TeamFlos/Phira MP servers and unmodified Phira clients are not compatible.** Authentication is unchanged. Existing command discriminants are preserved by appending variants.

```sh
cargo run --release --manifest-path vendor/phira-mp/Cargo.toml -p phira-mp-server -- --port 12346
```

Build the modified Phira app from the repository root as usual. Set **Multiplayer Server Address** to the host/port of your own server, then connect with your regular Phira account.

## Host a local chart

1. Import a chart into Phira's **Local** library first. Its chart directory must include a valid `info.yml` and referenced media.
2. Create or join a room on the matching custom MP server.
3. As **room host**, tap the imported local chart in the library. This uploads a compressed ZIP to the room's server and selects it, without publishing to the Phira website.
4. Start the room. Players request the selected ZIP's metadata and check `mp-local/<sha256>` before downloading. The host already has a validated installation of its exact uploaded ZIP snapshot, so it does not download its own chart. Cache misses stream the archive before reporting Ready.
5. Late joiners can download the selected chart while the room is still in chart-selection state.

Official Phira chart IDs remain supported for catalogue selection and downloading. However, these public-source Windows and Android builds cannot submit official scores, including catalogue chart scores: they lack the closed official score-record/attestation implementation. Use the official client for official scores and RKS. Login/authentication is unchanged.

## Limits and behavior

- Chart ZIP limit: **32 MiB** compressed, nonzero. Streaming chunk size: **256 KiB**. Window: **2 MiB** per transfer. The packet cap remains **2 MiB**.
- Extraction limit: **128 MiB** total unpacked files and at most **4096** ZIP entries. Both declared and actual extracted lengths are checked. Paths must stay inside the new installation directory; nesting is capped at 64 components.
- Each chunk uses an ordered offset. The completed archive is checked with **SHA-256** by the server and by each receiver before extraction.
- The chart is stored **only in the multiplayer server's room memory**. The server retains it while the room exists; the game caches received files locally. This is *not* a Phira website upload.
- Only the room's host may upload or replace a chart during chart selection. Any authenticated player in the room may request the currently selected room chart.
- Local-chart scores are shown in the room and used to finish the match, but are **not sent to the official Phira website or ranked leaderboard**. They are not independently validated by the server.
- Multiplayer account authentication remains unchanged; this feature does not bypass website authentication for joining a room.
- You need to share the modified server address and distribute a compatible build to every participant. Opening the port externally depends on your network setup.

## Development checks

```sh
cargo fmt --all --check
cargo test --workspace
cargo check -p phira
cargo test --manifest-path vendor/phira-mp/Cargo.toml --workspace
```

The streaming refactor has automated Windows TCP tests, not a new manual Android/Linux crossplay certification. Only `x86_64-pc-windows-msvc` was installed in the implementation environment. The transfer implementation uses existing portable Rust/Tokio APIs and no new runtime dependencies. Run the matching server and app on Android/Linux before deployment.

## Current cache policy

Transferred charts are cached under `mp-local/<sha256>` and are not added to the visible Local library. The hash identifies the ZIP archive, not a catalogue ID. Metadata is requested before the cache check; a valid cache hit transfers no ZIP packets.

A directory alone is not a cache hit. The atomic installer writes `.mp-install-v2.json` only after extraction, `info.yml` parsing, and validation that referenced chart/music/illustration/video paths stay inside the installation and exist. The manifest records the archive hash and every extracted file's length and SHA-256. Cache checks reject missing, altered, or symlinked files and hash files using a 64 KiB read buffer. Old installations without a manifest are downloaded and rebuilt once. Failed extraction removes its temporary directory and never publishes a completed cache entry.

The host installs the exact ZIP snapshot before uploading it. This avoids relying on a source directory that could have changed since upload. Local Ready and RequestStart packets include the installed archive hash; the server rejects obsolete content even if selection changes between the UI's cache check and its readiness request. Selection epochs plus connection identity prevent a completed task from an old selection, room, or connection from replacing the active path.

Cached files can remain on disk after a match; there is no bounded cache eviction policy. Repacking the same chart can change the archive hash and create a different cache folder, although received charts never create visible Local library entries. See the [README](../README.md#server-structure-and-setup) for the server layout and current build downloads.

## Streaming protocol

All traffic uses the existing length-prefixed binary packets and the **same multiplayer TCP connection**. No HTTP endpoint, additional production connection, or new transfer service is involved.

1. `GetLocalChartInfo { request_id }` returns a correlated `LocalChartInfo { request_id, result }` containing the selected archive's nonzero size and normalized 64-character hexadecimal SHA-256. Request correlation prevents a delayed metadata reply from satisfying a later request.
2. On a cache miss, `StartLocalChartDownload { transfer_id, expected_sha256 }` authenticates room access and checks the selected hash. The connection permits one active download; IDs increase monotonically and are never reused on that connection, including after cancellation or leaving a room.
3. A connection-owned producer task shares the room's immutable `Arc<Vec<u8>>` and sends unsolicited `LocalChartDownloadChunk { transfer_id, offset, data }` packets. It does not run on the command receiver. `sent - acknowledged <= 2 MiB` includes scheduled/queued bytes, not just bytes already written to the socket.
4. The client validates ordered offsets, bounded chunk lengths, and the declared total. It sends cumulative `LocalChartDownloadAck { transfer_id, next_offset }` after approximately 1 MiB, and at the final byte. ACKs must be monotonic, not exceed scheduled bytes or the archive size, and end on chunk boundaries except for the final partial chunk. Duplicates do not reset inactivity timers; stale IDs cannot release another transfer's credit.
5. After the final cumulative ACK, the server sends `LocalChartDownloadFinished`. The client requires the exact byte count and verifies SHA-256 before returning the archive. Errors are correlated `LocalChartDownloadError` packets. Malformed packets cannot panic the multiplayer receiver.
6. `CancelLocalChartDownload` aborts the producer. Leaving the room, dropping the connection receiver, chart replacement, and stalled ACKs also terminate it. Producers use weak room/user references and an owned task handle; they do not retain sessions or hold locks over channel sends. A blocked sender or lack of ACK progress times out after 30 seconds. The client gives metadata/first progress seven seconds, then resets a 30-second inactivity deadline only on validated progress, rather than timing out the entire archive after seven seconds.

Uploads also stream: `StartLocalChartUpload` is accepted with a correlated `LocalChartUploadProgress(0)`, followed by ordered `LocalChartUploadData` packets under the same 2 MiB sender window. The server returns cumulative progress every 1 MiB or final byte. `FinishLocalChartUpload` receives `LocalChartUploadVerified` only after exact-size and SHA-256 checks. One partial upload belongs to the authenticated connection, not the room; cancellation/disconnect releases it and a watchdog discards inactive uploads after 30 seconds. Host and chart-selection-state checks run for every upload packet. Final room archive storage remains shared and immutable.

Legacy command numbers remain reserved. Legacy upload handlers reject requests in v2; `FetchLocalChunk` remains available for controlled before/after comparison, but the app's download API no longer calls it.

## Control traffic and resource bounds

The transport still has one TCP writer. A bounded look-ahead scheduler prioritizes control packets over streaming data, preserves FIFO within each class, and sends a bulk packet after at most eight control packets when bulk is pending. It never preempts an already-writing TCP frame. Completion cannot overtake live data because completion is sent only after the receiver ACKs all bytes. Cancellation/errors may overtake obsolete data, which is safely ignored by transfer ID.

The outgoing channel and writer look-ahead each hold at most 16 packets. A normal active transfer contributes at most one 2 MiB window across them. Rapidly cancelled/replaced transfers can leave stale queued frames, but the 32-packet combined cap prevents unbounded retention (at most 8 MiB of default 256 KiB bulk payloads, plus the current encoded frame). The receiver's download handoff is bounded to nine packets. Each client buffers at most the declared 32 MiB archive; each server upload buffers at most 32 MiB, and download clients share the room's archive rather than copying it per connection.

Trace logs contain packet lengths and queue counts, never raw archive bytes or packet Debug dumps. Info logs report metadata latency, first-byte latency, download/upload duration, throughput, verification time, packet count, peak outstanding bytes, cache hits, and elapsed time to Ready. Existing heartbeat logs report RTT. Queue counts are observable, but exact socket/kernel queued-byte counts are not measured.

## Measured performance

Measurements on 2026-10-15 used Windows, Cargo's test/debug profile, real protocol serialization and TCP framing, and a bounded bidirectional TCP delay shim (half the requested RTT in each direction). The shim pipelines delayed bytes; it does **not** sleep serially per chunk. There is no bandwidth shaping or packet loss simulation. Authentication is replaced by a pre-authenticated room fixture. Transfer timing includes metadata and final SHA-256 verification, excludes ZIP installation, and the benchmark takes the maximum completion time among the concurrent clients. Four heartbeat probes run alongside each benchmark transfer. Separate integration tests issue heartbeats repeatedly throughout concurrent downloads.

| 16 MiB, 120 ms simulated RTT, one client | Seconds | MiB/s | Peak probe RTT |
| --- | ---: | ---: | ---: |
| Legacy 64 KiB stop-and-wait | 37.876 | 0.42 | Not sampled |
| Streaming, initial 256 KiB run | 2.608 | 6.13 | 197.9 ms |
| Streaming, final 256 KiB queue configuration | 2.190 | 7.31 | 160.8 ms |
| Streaming, 512 KiB alternative | 2.610 | 6.13 | 214.7 ms |

These are individual runs, not statistical confidence intervals. Keep the 256 KiB default: the tested 512 KiB alternative did not improve throughput and has larger non-preemptible frames. Receivers accept either size; only the test fixture configures the alternative sender.

The full **64-case** 256 KiB matrix passed. Durations below are seconds, with columns indicating concurrent clients. The largest observed probe RTT in the matrix was 513.4 ms (16 MiB, 200 ms RTT, four clients). These results precede the final reduction of the transport's queue cap; the 120 ms result above was rerun with that cap.

| Archive | RTT | 1 client | 2 clients | 4 clients | 8 clients |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 0 ms | 0.080 | 0.075 | 0.132 | 0.129 |
| 1 MiB | 50 ms | 0.270 | 0.282 | 0.329 | 0.328 |
| 1 MiB | 100 ms | 0.494 | 0.483 | 0.560 | 0.555 |
| 1 MiB | 200 ms | 0.871 | 0.955 | 0.904 | 0.953 |
| 8 MiB | 0 ms | 0.583 | 0.565 | 0.820 | 1.310 |
| 8 MiB | 50 ms | 1.005 | 0.952 | 0.940 | 1.538 |
| 8 MiB | 100 ms | 1.348 | 1.367 | 1.287 | 1.671 |
| 8 MiB | 200 ms | 1.814 | 1.911 | 1.984 | 2.206 |
| 16 MiB | 0 ms | 1.353 | 1.115 | 1.792 | 2.650 |
| 16 MiB | 50 ms | 1.923 | 1.801 | 1.841 | 2.630 |
| 16 MiB | 100 ms | 2.427 | 2.325 | 2.512 | 3.102 |
| 16 MiB | 200 ms | 3.163 | 3.595 | 3.418 | 3.957 |
| 32 MiB | 0 ms | 2.525 | 2.177 | 2.999 | 5.097 |
| 32 MiB | 50 ms | 3.485 | 3.466 | 3.419 | 5.517 |
| 32 MiB | 100 ms | 4.048 | 4.649 | 4.235 | 5.486 |
| 32 MiB | 200 ms | 5.601 | 6.811 | 6.319 | 6.716 |

Run the default 16 MiB/120 ms legacy-versus-streaming comparison:

```sh
cargo test --manifest-path vendor/phira-mp/Cargo.toml -p phira-mp-server benchmark_streaming_matrix -- --ignored --nocapture
```

Set `PHIRA_STREAM_BENCH_FULL=1` for the streaming matrix; add `PHIRA_STREAM_BENCH_LEGACY=1` to measure a one-client legacy baseline for every size/RTT (much slower). Set `PHIRA_STREAM_BENCH_CHUNK_KIB=512` for the alternate download chunk size and `PHIRA_STREAM_BENCH_SKIP_LEGACY=1` to skip the baseline. These environment settings affect only the test fixture, not the production server. The delay shim's additional sockets exist only inside tests; production transfers use the existing single multiplayer connection.

## Validation and follow-ups

Both workspace test suites and the Windows game check passed during implementation. Tests cover real packet round-trips, packet budget, malformed lengths, metadata limits/normalization, exact and partial final chunks, checksum failure, bad offsets/oversized chunks, premature and stale completion, invalid/stale ACKs, duplicate ACKs, overlap rejection, cancellation, chart replacement, disconnect, virtual-time inactivity cleanup, independent concurrent clients, heartbeat responsiveness, v1 rejection/v2 acceptance, and atomic cache reuse/repair/rejection. A readiness regression test rejects obsolete installed hashes after selection changes. Cache tests do not launch the renderer or exercise a full manual lobby/game.

Remaining work: Android/Linux deployment validation, release-build and bandwidth/loss-shaped benchmarks, upload performance measurements, repeated statistical samples, automatic lobby prefetch, and cache eviction. No resumable transfers or automatic prefetch are implemented. SHA-256 and installation work can still consume CPU/disk time; TCP head-of-line blocking and a frame already being written cannot be avoided by packet priority.
