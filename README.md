# Phira — private multiplayer chart sharing

This fork of [TeamFlos/Phira](https://github.com/TeamFlos/phira) lets a room host share an imported chart privately with other players through a matching self-hosted multiplayer server. Windows–Android crossplay has been reported working by the user. This is an experimental community build; login and account authentication are unchanged.

> **Score limitation:** these public-source Android, Windows and Linux builds cannot submit official scores, including scores for Phira catalogue charts. They lack the closed official score-record/attestation implementation. Private multiplayer scores are displayed in the room only. Use the official client for official scores and RKS. See the [upstream Android build guide](https://teamflos.github.io/phira-docs/phira_build_guide/Android.html).

## Faster private sharing (protocol v2)

[PR #3](https://github.com/shinova55/phira-custom-mp/pull/3) is merged into `main`. Chart uploads and downloads now stream over the existing multiplayer connection, and clients check a validated cache before downloading. The host reuses its uploaded chart snapshot instead of downloading it again.

**User-reported testing on October 17, 2026:** the PR #3 builds worked, with chart loading reduced from approximately **30 seconds to 2–4 seconds** in the tested setup. This is an observed result, not a guaranteed loading time; chart size, network conditions, server load and cache state affect performance.

> **Update every client and the multiplayer server together.** Current `main` uses protocol **v2**, with no v1 fallback. The older `v0.8.2-private-mp.1` release below uses the previous protocol and does not include these improvements.

## Download this fork

### Protocol-v2 test builds

These packages were built from the tested PR commit `63353cb`, now merged into `main`:

| Package | Download | How to use |
| --- | --- | --- |
| Android ARM64, Android 8+ | [Android artifact](https://github.com/shinova55/phira-custom-mp/actions/runs/38999499315/artifacts/12088184551) | Extract the artifact ZIP and install the APK. Back up data before reinstalling with its new test certificate. |
| Windows x64 | [Windows artifact](https://github.com/shinova55/phira-custom-mp/actions/runs/38999499315/artifacts/12088244640) | Extract the entire ZIP and run `Start-Phira.cmd`. Keep `assets` beside the executable, including `assets/phigros.ttf`. |
| Linux x64 multiplayer server | [Matching server artifact](https://github.com/shinova55/phira-custom-mp/actions/runs/38999499315/artifacts/12088458188) | Extract the artifact ZIP, then its tar.gz. On Ubuntu 24.04 x64, run `./phira-mp-server --port 12346` from the extracted folder. |

GitHub sign-in may be required for Actions downloads. These artifacts expire **November 16, 2026**. There is no protocol-v2 Linux game-client package linked here; the Linux package above is the server only. A new tagged release containing PR #3 has not yet been published.

### Previous release (protocol v1)

**[v0.8.2-private-mp.1 — previous testing prerelease](https://github.com/shinova55/phira-custom-mp/releases/tag/v0.8.2-private-mp.1)** remains available for older installations. These packages were built from `5a4516e` and must be used with their matching older server. They do not include PR #3 and must not be mixed with the protocol-v2 packages above. Release downloads do not have the 30-day expiry of Actions artifacts.

| Platform / package | Download | Installation |
| --- | --- | --- |
| Android ARM64, Android 8+ | [APK](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/phira-arm64-test.apk) | Install the APK; read the signing note below first. |
| Windows x64 | [Portable ZIP](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/phira-windows-x64.zip) | Extract the whole ZIP into a writable folder and run `Start-Phira.cmd`. Keep the executable beside `assets`. |
| Linux x64 client | [Client tar.gz](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/phira-linux-x64.tar.gz) | Targets Ubuntu 24.04 (glibc 2.39) with a desktop/OpenGL environment. Extract and run `./Start-Phira.sh`; see [dependencies and setup](docs/linux-test-build.md). |
| Linux x64 multiplayer server | [Server tar.gz](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/phira-mp-server-only-linux-x64.tar.gz) | For hosting, not playing. Targets Ubuntu 24.04; extract and run `./phira-mp-server --port 12346`. |

**Android signing:** the APK uses an ephemeral test certificate and the official package ID. It cannot update an official installation or an earlier test build signed with another key. Back up/export local data before reinstalling, or use a separate Android user/profile. No release-signing secrets are needed. Windows is unsigned. Linux is not an AppImage and requires system libraries; other distributions and older systems are not verified.

The [release page](https://github.com/shinova55/phira-custom-mp/releases/tag/v0.8.2-private-mp.1) also includes **Source code (ZIP / tar.gz)** for the tagged client and modified server, [SHA-256 checksums](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/SHA256SUMS.txt), and [verification reports](https://github.com/shinova55/phira-custom-mp/releases/download/v0.8.2-private-mp.1/verification-reports.zip). Source archives do not contain closed official code or signing keys. Building still downloads external dependencies and upstream assets; Android also uses the upstream APK shell.

The checksum manifest retains the original build filenames; match entries by platform when checking the renamed downloads. A whole-file `sha256sum --check` against the current release filenames will therefore need those filename entries adjusted first.

### What has been verified

- **Protocol v2 (`63353cb`):** formatting, Clippy on Linux/macOS/iOS, workspace tests and server checks passed. Android and Windows packages and the Linux server were built successfully; downloaded source-commit records, checksums and binary architectures were verified. Android signature/alignment checks passed.
- The Windows build passed all **6 client tests**; the vendored multiplayer workspace passed **17 tests**. One controlled latency benchmark is intentionally ignored by normal test runs, not a failed test.
- The user confirmed that the PR #3 builds work and reported the 30-second to 2–4-second loading improvement above. Earlier Windows–Android crossplay was also user-tested. This does not establish coverage of every device, network condition or disconnect/rejoin scenario.
- **Previous release (`5a4516e`):** all three platform builds and package checks passed, and uploaded files were downloaded back and checksum-verified. Linux game startup was checked under a virtual display; real-device Linux gameplay/crossplay remains unverified. These older release checks are not Linux game validation for protocol v2.

See [private multiplayer details](docs/multiplayer-host-local-charts.md) for the current protocol and cache behavior, and the [Windows](docs/windows-test-build.md), [Android](docs/android-test-build.md) and [Linux](docs/linux-test-build.md) notes for platform setup. The score-upload limitation remains unchanged.

## Share a private chart

1. Run the matching server described below and give its address to every player.
2. Each player uses this modified client, logs in with their normal Phira account, and connects to the same server and room.
3. The room host imports a chart into **Local**, then selects that imported chart in the room. Imported charts use private transfer even if their metadata contains a catalogue ID.
4. Selection streams the chart to the room server and installs the host’s exact uploaded snapshot locally. Starting/readying first checks the selected archive’s metadata and validated cache; only players without a valid matching cache download and verify the ZIP.

Charts are shared with members of that room without publishing them to the Phira catalogue. Downloaded catalogue charts retain normal catalogue selection; built-in charts must first be imported as a chart ZIP to use private sharing.

## Cache and practical limitations

Currently, transferred charts are cached under `mp-local/<SHA-256>`. They aren’t added to the visible Local library. The hash identifies the archive bytes. With protocol v2, a validated cache hit skips downloading the archive; the host can reuse its uploaded snapshot.

- There is no **Save to Local** feature. Receiving a chart does not add a duplicate entry to the visible library or make it available there for solo play.
- The hash identifies archive bytes, not a catalogue level ID. Repacking a chart can create a different cache folder. Cached files can remain on disk after a match; there is no bounded cache eviction policy.
- Clients request chart metadata before downloading and validate cached file contents against a recorded manifest. Missing, modified or legacy cache entries without a valid manifest trigger a download. A valid cache hit avoids the archive transfer, but metadata checks and room readiness coordination still take time.
- Archives are limited to **32 MiB** compressed, **128 MiB** extracted, and **4096** ZIP entries. Protocol-v2 transfers use ordered **256 KiB** chunks, a **2 MiB** flow-control window and SHA-256 verification.
- Private scores are room-only and are not independently validated by the server. Official score submission is unavailable in these builds, including for catalogue charts.
- All players and the server must use the matching **protocol-v2** implementation. Older private-sharing builds and normal upstream MP servers are not supported. Successful room entry alone is not proof that a differently modified server implements the required upload commands.

## Server structure and setup

The repository includes the modified multiplayer components in a separate Rust workspace:

| Directory | Role |
| --- | --- |
| `phira/` | Game client, chart selection and multiplayer UI |
| `vendor/phira-mp/phira-mp-client/` | Client connection and transfer handling |
| `vendor/phira-mp/phira-mp-common/` | Shared protocol and commands |
| `vendor/phira-mp/phira-mp-server/` | Multiplayer rooms and private chart relay |

```text
Host imports/selects a chart
    -> uploads ZIP to the custom MP server's room memory
    -> host reuses its uploaded snapshot
    -> other room members check metadata and validate their cache
    -> cache misses stream/download and verify the ZIP
    -> clients use mp-local/<SHA-256> and play
```

This is a server-relayed transfer. The server holds the selected archive in room memory while the room exists; it does not publish it to the catalogue or keep a permanent chart library. Only the room host can upload/replace it during chart selection. Authenticated room members can download the current chart. Normal Phira account authentication remains required.

For the prebuilt Linux server, extract the **protocol-v2 server artifact** above into a writable folder, enter that folder, and run:

```sh
./phira-mp-server --port 12346
```

The server writes logs in its working directory; the package does not install a background service. To build from source instead, install the repository's Rust toolchain and build dependencies, then run from the repository root:

```sh
cargo run --locked --release --manifest-path vendor/phira-mp/Cargo.toml -p phira-mp-server -- --port 12346
```

Allow access to the server port as appropriate for your network, then set **Multiplayer Server Address** in each client to your server's host and port. Use the matching protocol-v2 test server or build current `main` from this repository’s `vendor/phira-mp`; installing an upstream MP server separately will not provide this feature. The server check workflow checks/tests the code; the three-platform prerelease workflow also packages the Linux x64 server.

See [private multiplayer details](docs/multiplayer-host-local-charts.md) for limits and development checks. Basic private sharing and Windows–Android crossplay have been user-tested, but disconnect recovery, late joins and all device configurations are not comprehensively verified.

## Upstream Phira

![Phira Showcase](https://phira.5wyxi.com/files/github-showcase.png)

[中文版本](./README-zh_CN.md)

## Official upstream downloads

- [GitHub Release](https://github.com/TeamFlos/phira/releases): For Android, Windows and Linux
- [616.sb mirror](https://616.sb/#phira): Android only
- [App Store](https://apps.apple.com/us/app/phira/id6447435864): For iOS
- ~~[.ipa Release](https://github.com/F-Unction/phira_ipa/releases): For iOS~~

## Contributions

For this fork, use [this repository’s issues](https://github.com/shinova55/phira-custom-mp/issues) and pull requests. The following links and contact information belong to the upstream Phira project.

Issues & pull requests are welcome! Also check these good first [issues](https://github.com/TeamFlos/phira/issues?q=label%3A%22good+first+issue%22) and [pull requests](https://github.com/TeamFlos/phira/issues?q=label%3A%22good+first+issue%22).

If you want to talk with us in depth, email [contact@phira.cn](mailto://contact@phira.cn). Currently we are looking for **Rust** Programmers *with rhythm/music game experience*.

## Translation

See our [Weblate](https://hosted.weblate.org/engage/phira/) page for the latest translation progress. You can easily contribute to translations there.

![Translation Progress](https://hosted.weblate.org/widget/phira/svg-badge.svg)

![Translation Status](https://hosted.weblate.org/widget/phira/multi-auto.svg)

## Star History

[![Stargazers over time](https://starchart.cc/TeamFlos/phira.svg?variant=adaptive)](https://starchart.cc/TeamFlos/phira)
