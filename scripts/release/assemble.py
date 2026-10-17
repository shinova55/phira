"""Assemble checked workflow artifacts and verify uploaded release bytes."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


out = Path('release')
if len(sys.argv) > 1:
    uploaded = json.loads(Path(sys.argv[2]).read_text())['assets']
    local = {p.name: p for p in out.iterdir()}
    require({p['name'] for p in uploaded} == set(local), 'Uploaded asset list differs')
    for asset in uploaded:
        require(asset['size'] == local[asset['name']].stat().st_size, 'Uploaded size mismatch')
    with tempfile.TemporaryDirectory() as temp:
        subprocess.run(['gh', 'release', 'download', os.environ['RELEASE_TAG'], '--dir', temp], check=True)
        for name, path in local.items():
            require(digest(Path(temp) / name) == digest(path), f'Uploaded checksum mismatch: {name}')
    print('All release assets downloaded back and SHA-256 verified.')
    sys.exit(0)

sha = os.environ['GITHUB_SHA']
artifacts = Path('artifacts')
windows = artifacts / f'phira-private-charts-windows-x64-{sha}'
android = artifacts / f'phira-private-charts-arm64-test-{sha}'
linux = artifacts / f'phira-private-charts-linux-x64-{sha}'
for folder in (windows, android, linux):
    require((folder / 'source-commit.txt').read_text(encoding='utf-8-sig').strip() == sha,
            f'Wrong source commit: {folder}')
out.mkdir()
for folder, binary in ((windows, 'Phira-Private-Charts.exe'), (android, 'phira-private-charts-arm64-test.apk')):
    expected = (folder / 'SHA256SUMS.txt').read_text(encoding='utf-8-sig').split()[0]
    require(digest(folder / binary) == expected, f'Checksum mismatch: {binary}')
for font in ('font.ttf', 'bold.ttf', 'phigros.ttf'):
    require((windows / 'assets' / font).is_file(), f'Windows font missing: {font}')
require((windows / 'Start-Phira.cmd').is_file(), 'Windows launcher missing')
with zipfile.ZipFile(android / 'phira-private-charts-arm64-test.apk') as apk:
    require(apk.testzip() is None, 'APK ZIP checksum failure')
    require('lib/arm64-v8a/libphira.so' in apk.namelist(), 'Missing ARM64 client')
require('Verified using v2 scheme (APK Signature Scheme v2): true' in (android / 'signature.txt').read_text(),
        'APK signature verification missing')
shutil.make_archive(str(out / 'phira-windows-x64'), 'zip', windows)
shutil.copy2(android / 'phira-private-charts-arm64-test.apk', out / 'phira-arm64-test.apk')
for name in ('phira-private-charts-linux-x64', 'phira-mp-server-linux-x64'):
    archive = linux / f'{name}.tar.gz'
    with tarfile.open(archive) as tar:
        require(tar.extractfile(f'{name}/source-commit.txt').read().decode().strip() == sha, 'Wrong Linux source')
        binary = 'phira' if name.startswith('phira-private') else 'phira-mp-server'
        member = tar.getmember(f'{name}/{binary}')
        require(member.mode & 0o111, 'Linux binary is not executable')
        expected = tar.extractfile(f'{name}/SHA256SUMS.txt').read().decode().split()[0]
        require(hashlib.file_digest(tar.extractfile(member), 'sha256').hexdigest() == expected, 'Linux checksum mismatch')
    public_name = 'phira-linux-x64.tar.gz' if name.startswith('phira-private') else 'phira-mp-server-only-linux-x64.tar.gz'
    shutil.copy2(archive, out / public_name)
with tempfile.TemporaryDirectory() as temp:
    reports = Path(temp)
    for platform, folder in (('android', android), ('linux', linux), ('windows', windows)):
        dest = reports / platform
        dest.mkdir()
        for path in folder.glob('*.txt'):
            shutil.copy2(path, dest)
        if (folder / 'README.md').exists():
            shutil.copy2(folder / 'README.md', dest)
    shutil.make_archive(str(out / 'verification-reports'), 'zip', reports)
(out / 'source-commit.txt').write_text(sha + '\n')
(out / 'SHA256SUMS.txt').write_text(''.join(f'{digest(p)}  {p.name}\n' for p in sorted(out.iterdir())))
print('All three platforms and the matching server validated for source commit', sha)
