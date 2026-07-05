#!/usr/bin/env bash
# [MyPilot] Bring up SSH as early as possible — before the AGNOS check, the on-device compile, and
# the manager — so the device is reachable for troubleshooting at ANY boot stage, even if the
# compile or manager fails. Every step is non-fatal; this can never block or break boot.

KEYS="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIADjTRuhrJePGo64QLpU5yRalnRx0gDo2bMXTIFiBwbs
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEHASukec8SFiEZBPptaF1zpMjeEpemVAXxCl7xQwrW1
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILjiEh2TKbmhpNx+mwN9WXB62mP1m73qY8u9eus4jqmO"

# PRIMARY (works before any compile): write the params AGNOS's sshd authorizes against as RAW
# FILES — no params_pyx / openpilot needed. This is exactly how the comma installer seeds keys.
mkdir -p /data/params/d 2>/dev/null || true
printf '%s' "$KEYS"     > /data/params/d/GithubSshKeys  2>/dev/null || true
printf '1'              > /data/params/d/SshEnabled      2>/dev/null || true
printf 'castanley'      > /data/params/d/GithubUsername  2>/dev/null || true

# SECONDARY (works once params_pyx is compiled): set the same via the API, best-effort.
GITHUB_SSH_KEYS="$KEYS" python3 -c "import os; from openpilot.common.params import Params; p=Params(); p.put_bool('SshEnabled', True); p.put('GithubSshKeys', os.environ['GITHUB_SSH_KEYS']); p.put('GithubUsername', 'castanley')" 2>/dev/null || true

# TERTIARY: drop authorized_keys directly for both comma and root, in case sshd reads the file.
for home in /home/comma /root /data/home/comma; do
  mkdir -p "$home/.ssh" 2>/dev/null || true
  printf '%s\n' "$KEYS" > "$home/.ssh/authorized_keys" 2>/dev/null || true
  chmod 700 "$home/.ssh" 2>/dev/null || true
  chmod 600 "$home/.ssh/authorized_keys" 2>/dev/null || true
done
chown -R comma:comma /home/comma/.ssh 2>/dev/null || true

# make sure sshd is running and rereads config/keys.
sudo systemctl restart ssh 2>/dev/null || sudo systemctl start ssh 2>/dev/null || true
sudo systemctl restart sshd 2>/dev/null || sudo systemctl start sshd 2>/dev/null || true

echo "[mypilot] SSH enable hook ran at $(date)"
exit 0
