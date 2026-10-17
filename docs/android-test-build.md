# Android private-chart testing build

The `Android test APK` workflow builds the branch's Rust client for ARM64 with
video enabled, using the pinned Rust toolchain, Cargo.lock, NDK r28c and API 26.
It replaces libphira.so and tracked assets in the SHA-256-pinned official v0.8.2
APK shell (the Java Android project is not included in this repository).
This follows the packaging approach described in the official Android guide:
https://teamflos.github.io/phira-docs/en/phira_build_guide/Android.html

Install `phira-arm64-test.apk` from the release, or extract
`phira-private-charts-arm64-test.apk` from an Actions artifact.
It is optimized for gameplay but signed with an ephemeral Android test key,
not an official release certificate. No signing secrets are required.
Signature, ZIP alignment, package metadata, source commit and SHA-256 reports
are included. The workflow does not run gameplay or an Android emulator.

The package ID is retained from the official shell. It cannot update an official
installation because the certificate differs. Back up/export local charts and
settings before uninstalling an existing copy, or test in a separate Android
user/profile. Each workflow run creates a new test key, so updates between runs
also require reinstalling. ARM64 Android devices only; the native build targets
Android 8/API 26 or later; the manifest minimum is set to API 26. The packaged
C++ runtime comes from the same NDK as the native build.

All players must use the modified client and a server built from this branch's
`vendor/phira-mp`. Follow `docs/multiplayer-host-local-charts.md`. Login and
authentication requirements are unchanged. A normal upstream MP server cannot
transfer private charts. The public-source build does not include the closed
official score-attestation implementation; official score submission is
unavailable, including for catalogue charts, as noted in the upstream build guide. Private room scores use the
branch's room-only reporting. Full two-client gameplay needs device testing.

This version requires matching protocol-v2 clients and server. The first prerelease
is incompatible. Validated cache hits skip chart downloads; the host reuses its
uploaded snapshot. Received charts remain outside the visible Local library.
