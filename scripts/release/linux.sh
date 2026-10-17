#!/usr/bin/env bash
set -euo pipefail
mkdir -p build/linux dist
curl --fail --location --retry 3 https://github.com/TeamFlos/phira/releases/download/v0.8.2/Phira-windows-x86_64-v0.8.2.zip -o build/linux/assets-template.zip
echo '7a881fd48cd156201d0b1cb700e003fb58cc01ebcfadad1e9c865d0f3282bc41  build/linux/assets-template.zip' | sha256sum --check
unzip -q build/linux/assets-template.zip 'assets/*' -d build/linux/template
client=build/linux/phira-private-charts-linux-x64
server=build/linux/phira-mp-server-linux-x64
mkdir -p "$client" "$server"
cp -a build/linux/template/assets "$client/"
cp -a assets/. "$client/assets/"
install -m 755 target/release/phira-main "$client/phira"
install -m 755 vendor/phira-mp/target/release/phira-mp-server "$server/phira-mp-server"
cp LICENSE "$client/"
cp vendor/phira-mp/LICENSE "$server/"
cp docs/linux-test-build.md "$client/README.md"
cp docs/multiplayer-host-local-charts.md "$server/README.md"
printf '#!/bin/sh\nset -eu\ncd -- "$(dirname -- "$0")"\nexec ./phira "$@"\n' > "$client/Start-Phira.sh"
chmod +x "$client/Start-Phira.sh"
for font in font.ttf bold.ttf phigros.ttf; do test -s "$client/assets/$font"; done
for dir in "$client" "$server"; do git rev-parse HEAD > "$dir/source-commit.txt"; done
for binary in "$client/phira" "$server/phira-mp-server"; do
  file "$binary" | tee -a dist/linux-verification.txt
  file "$binary" | grep -q 'ELF 64-bit.*x86-64'
  ldd "$binary" | tee -a dist/linux-verification.txt
  if ldd "$binary" | grep -q 'not found'; then exit 1; fi
done
# A null audio sink and software-rendered display allow a startup check in CI.
pulseaudio --start --exit-idle-time=-1
pactl load-module module-null-sink sink_name=ci
printf 'pcm.!default { type pulse }\nctl.!default { type pulse }\n' > "$HOME/.asoundrc"
python3 scripts/release/smoke-linux.py "$client" "$server" | tee -a dist/linux-verification.txt
(cd "$client" && sha256sum phira > SHA256SUMS.txt)
(cd "$server" && sha256sum phira-mp-server > SHA256SUMS.txt)
tar -czf dist/phira-private-charts-linux-x64.tar.gz -C build/linux phira-private-charts-linux-x64
tar -czf dist/phira-mp-server-linux-x64.tar.gz -C build/linux phira-mp-server-linux-x64
git rev-parse HEAD > dist/source-commit.txt
