# Linux x64 testing build

This package targets Ubuntu 24.04 x64 (glibc 2.39) with an OpenGL-capable desktop.
Other distributions and older systems are not verified. This is not an AppImage
and does not bundle all system libraries. On Ubuntu 24.04, install runtime dependencies:

```sh
sudo apt install libasound2t64 libgtk-3-0t64 libgl1 libx11-6 libxi6 libxcursor1 zlib1g libssl3t64
```

Extract the entire tar.gz into a writable folder, open that folder and run
`./Start-Phira.sh`. Keep `phira` beside `assets`. The launcher sets the working
directory. Full assets are supplemented from the checksum-pinned upstream v0.8.2
package; its Windows executable is not used.

The separate `phira-mp-server-only-linux-x64.tar.gz` package is for hosting only.
Extract it into a writable folder and run `./phira-mp-server --port 12346` there.
Allow incoming TCP connections on that port as appropriate for your network.
Players configure Multiplayer Server Address with this host and port.
The server writes logs in its working directory. There is no automatic service
installation. The normal Phira account login remains required.

All players and the server need protocol v2. The first prerelease is incompatible.
Transferred charts remain in `mp-local/<SHA-256>`, outside the visible Local
library; there is no Save to Local. Validated cache hits skip archive transfers,
and the host reuses its uploaded snapshot. Readiness coordination still takes time. Private scores are room-only. Official score submission, including for
catalogue charts, is unavailable because the public build lacks the closed
official score-record/attestation implementation.

CI checks executable format, dynamic dependencies and startup under Xvfb with
software rendering and a null audio sink. It also checks that the server opens
its TCP port. These checks do not verify audio, performance, interactive gameplay
or Linux crossplay on real devices. Test those before relying on this prerelease.
