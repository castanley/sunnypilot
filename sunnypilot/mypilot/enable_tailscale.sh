#!/usr/bin/env bash
# [MyPilot private] Bring up Tailscale so the device is reachable for troubleshooting over ANY
# network — including cellular/CGNAT, where inbound SSH is impossible (the device dials OUT to the
# tailnet, same as the agent's WebSocket). Gives a stable address (mici-ramhd / 100.x) on wifi AND
# 5G. Mirrors enable_ssh.sh: every step is non-fatal and can never block or break boot.
#
# State + binary live in /data/tailscale (persists across reboots AND OTA updates), so the node
# stays authed after a one-time interactive `tailscale up` — NO auth key is baked into the build.
# If the binary is missing (e.g. fresh flash), it self-heals by downloading it.

TS=/data/tailscale
BIN="$TS/bin"
STATE="$TS/state"
SOCK="$STATE/tailscaled.sock"
TS_VERSION="1.98.4"
TS_ARCH="arm64"
HOSTNAME_TS="mici-ramhd"

mkdir -p "$BIN" "$STATE" 2>/dev/null || true

# 1. Ensure the binary exists (self-heal a wiped /data/tailscale; normally already present).
if [ ! -x "$BIN/tailscaled" ] || [ ! -x "$BIN/tailscale" ]; then
  echo "[mypilot] tailscale binary missing — downloading ${TS_VERSION} ${TS_ARCH}"
  TGZ="/tmp/tailscale_${TS_VERSION}_${TS_ARCH}.tgz"
  if curl -fsSL -o "$TGZ" "https://pkgs.tailscale.com/stable/tailscale_${TS_VERSION}_${TS_ARCH}.tgz" 2>/dev/null; then
    D="$(tar -tzf "$TGZ" 2>/dev/null | head -1 | cut -d/ -f1)"
    tar -xzf "$TGZ" -C /tmp 2>/dev/null \
      && cp "/tmp/$D/tailscaled" "/tmp/$D/tailscale" "$BIN/" 2>/dev/null \
      && chmod +x "$BIN/tailscaled" "$BIN/tailscale" 2>/dev/null
    rm -rf "$TGZ" "/tmp/$D" 2>/dev/null || true
  else
    echo "[mypilot] tailscale download failed (offline?) — skipping, will retry next boot"
  fi
fi

# 2. Start the daemon if it isn't already running (real kernel TUN; state/socket under /data).
if [ -x "$BIN/tailscaled" ] && ! pgrep -f "$BIN/tailscaled" >/dev/null 2>&1; then
  sudo setsid "$BIN/tailscaled" \
    --state="$STATE/tailscaled.state" \
    --socket="$SOCK" \
    --port=41641 \
    </dev/null >"$STATE/tailscaled.log" 2>&1 &
  sleep 3
fi

# 3. Bring the node up. With existing state this re-attaches WITHOUT interactive auth. Client-only:
#      - NO --accept-routes: the device must NOT pull tailnet subnet/exit routes, or its replies to
#        hosts inside an advertised subnet (e.g. the home LAN it physically sits on) get black-holed
#        and SSH banners never arrive ("connects, no banner").
#      - NO --ssh: leave port 22 to the system sshd + the owner's baked keys (enable_ssh.sh), instead
#        of Tailscale SSH (which depends on tailnet ACL check-mode and would hang).
#    The stable hostname means the device is always reachable at "mici-ramhd" / its 100.x IP.
if [ -x "$BIN/tailscale" ]; then
  sudo "$BIN/tailscale" --socket="$SOCK" up \
    --hostname="$HOSTNAME_TS" \
    --accept-routes=false \
    --ssh=false \
    --timeout=20s >/dev/null 2>&1 || true
fi

echo "[mypilot] Tailscale enable hook ran at $(date)"
exit 0
