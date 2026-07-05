"""Device log upload to the MyPilot stack (opt-in) — LOGS-ENABLE-001 (D-0039).

Collects CRASH logs + a BOUNDED SYSTEM-log tail from the device and ships them through the existing
``/api/ingest/logs`` pipeline (uploader.upload_log). Deliberately does NOT touch qlogs — those already
upload as route files via ``drive_video`` (drive_upload); re-collecting them here would double-upload.
rlogs are excluded too (too large / too sensitive for v1).

Human-locked design (D-0039):
  * opt-in boolean ``log_upload`` in /data/mypilot/config.json, DEFAULT OFF (mirrors ``drive_upload``);
  * ARM-ON-DEVICE-ONLY — the web can only turn it OFF; enabling from off requires the device toggle
    (privacy, like the cabin camera). Enforced in the private agent layer / device UI, same as the
    existing arm-on-device gate; this module only READS the resulting config value.
  * NO redaction in v1 — owner-scoped (server ``_owned_device``) + opt-in is the trust model.
  * SAME discipline as drive upload: OFFROAD-ONLY, msgq-free (Hard Rule #2 — plain file reads, NEVER
    the openpilot driving bus), deferred-retry via a mark-uploaded state file, memory-pressure guard.
  * NO api/schema change — the ``crash``/``system`` LogKinds + the /api/ingest/logs endpoints exist.

Paths CONFIRMED by Hardware (D-0040, live device 034474a, source-verified bus-free): crash =
/data/community/crashes (tiny plain-text tracebacks — ship all); system = /data/log rotating swaglog
(recent-window only — it's ~1704 files/81MB live, up to ~640MB, so we ship only the most-recent files,
tail-bounded, deduped by mtime, never the full history). The native tombstone dir /data/tombstones
does NOT exist on the comma 4 and is simply not a source (a missing dir yields [] gracefully).
"""

from __future__ import annotations

import json
import os

CONFIG_FILE = "/data/mypilot/config.json"
STATE_FILE = "/data/mypilot/uploaded_logs.json"  # separate from uploaded_segments.json (drive upload)

# Bound the system-log tail so a rotating swaglog file can't balloon storage/data (§6 of the plan).
# Each swaglog file is already max 256 KB (Hardware recon: swaglog max_bytes=256KB); we tail-bound as
# belt-and-suspenders and ship crash logs (tiny) whole.
_SYSTEM_TAIL_BYTES = 256 * 1024  # last 256 KB of a system log file
_MAX_LOGS_PER_CYCLE = 20         # per-cycle cap so a backlog can't flood a single cycle
# Only the most-RECENT N system-log files are eligible — swaglog rotates to ~2500 files / tens of MB
# (Hardware: 1704 files / 81MB live), so we must NOT march through the whole history. Recent files hold
# the useful "what happened lately"; older rotations are noise and would balloon upload. Crash logs are
# tiny + few (~3 files, ~300-400B) so they are NOT windowed — all crashes ship.
_SYSTEM_RECENT_FILES = 10

# Real on-device paths — CONFIRMED by Hardware (D-0039, live device 034474a, source-verified bus-free):
#   crash  = Paths.crash_log_root() = /data/community/crashes  (plain-text py tracebacks, tiny; ship all)
#   system = Paths.swaglog_root()   = /data/log                (rotating JSON-lines; recent-window only)
# NOT collected (out of scope): /data/media/0/realdata = qlogs (already via drive_upload) + rlogs.
# The native-crash dir /data/tombstones does NOT exist on comma 4 — tolerated by the missing-dir skip.
_LOG_SOURCES: list[dict] = [
    {"kind": "crash", "dir": "/data/community/crashes", "whole": True},   # all crash logs (tiny, few)
    {"kind": "system", "dir": "/data/log", "whole": False, "recent": _SYSTEM_RECENT_FILES},
]


def _read_json(path: str, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return default


def _write_json_atomic(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(data, fh)
    os.replace(tmp, path)


def log_upload_on() -> bool:
    """Whether the opt-in device-log upload toggle is enabled. Default OFF. Mirrors
    ``drive_video.cabin_upload_on`` / the drive_upload read: a plain config.json read, no side effects.
    The ARM-ON-DEVICE-ONLY rule is enforced where the config is WRITTEN (device UI / private layer),
    not here — this only reads the resulting value."""
    return bool((_read_json(CONFIG_FILE, {}) or {}).get("log_upload", False))


def _uploaded_markers() -> set[str]:
    return set(_read_json(STATE_FILE, []) or [])


def mark_uploaded(markers: list[str]) -> None:
    """Persist markers of successfully-uploaded logs so reboots/cycles never re-upload (deferred-retry:
    a log that failed this cycle is simply not marked, so it's retried next cycle). Mirrors
    ``drive_video.mark_uploaded``."""
    done = _uploaded_markers()
    done.update(markers)
    _write_json_atomic(STATE_FILE, sorted(done))


def _marker_for(path: str, kind: str) -> str:
    """Stable per-log id for the mark-uploaded set: kind + path + mtime, so a log is uploaded once but a
    ROTATED/CHANGED file at the same path (new mtime) is treated as new and re-collected."""
    try:
        mtime = int(os.path.getmtime(path))
    except OSError:
        mtime = 0
    return f"{kind}:{path}:{mtime}"


def _tail_bytes(path: str, limit: int) -> bytes:
    """Read at most the last ``limit`` bytes of a file (bounded system-log tail). Whole file if smaller.
    Best-effort: unreadable -> b''. msgq-free plain file read."""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > limit:
                fh.seek(size - limit)
            return fh.read()
    except OSError:
        return b""


def _read_whole(path: str, limit: int) -> bytes:
    """Read a whole (small) log file, capped at ``limit`` as a safety bound. msgq-free plain file read."""
    try:
        with open(path, "rb") as fh:
            return fh.read(limit)
    except OSError:
        return b""


def _collect_source(source: dict, already: set[str]) -> list[dict]:
    """Discover un-uploaded log files for ONE source and build upload_log payloads
    ``{kind, name, route_name, data, _marker}``, skipping any whose marker is already uploaded.

    ``recent`` (system logs) limits eligibility to the most-recent N files by mtime so we never march
    through the whole rotating swaglog history; crash logs (no ``recent``) ship all (tiny + few). A
    missing/unreadable dir yields [] and never raises — this is also how the absent native-tombstone
    dir on the comma 4 is tolerated (Hardware D-0040)."""
    out: list[dict] = []
    directory = source.get("dir")
    if not directory or not os.path.isdir(directory):
        return out
    kind = source["kind"]
    whole = source.get("whole", kind == "crash")
    recent = source.get("recent")  # if set, only the most-recent N files are eligible (swaglog bound)
    try:
        entries = [
            (os.path.join(directory, n), n) for n in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, n))
        ]
    except OSError:
        return out
    if recent is not None:
        # Newest-first by mtime, keep only the recent window — do NOT march through the whole rotating
        # swaglog history (1704 files / 81MB live). Then process oldest-first within the window so a
        # per-cycle cap ships older-of-the-recent first (steady progress, no starvation).
        entries.sort(key=lambda pn: os.path.getmtime(pn[0]) if os.path.exists(pn[0]) else 0, reverse=True)
        entries = entries[:recent]
        entries.reverse()
    else:
        entries.sort(key=lambda pn: pn[1])  # crash dir: stable name order, ship all
    for path, name in entries:
        marker = _marker_for(path, kind)
        if marker in already:
            continue
        data = _read_whole(path, _SYSTEM_TAIL_BYTES * 4) if whole else _tail_bytes(path, _SYSTEM_TAIL_BYTES)
        if not data:
            continue
        out.append({"kind": kind, "name": name, "route_name": None, "data": data, "_marker": marker})
        if len(out) >= _MAX_LOGS_PER_CYCLE:
            break
    return out


def collect_logs() -> list[dict]:
    """Build the list of un-uploaded crash/system log payloads to ship this cycle (across all sources),
    capped at ``_MAX_LOGS_PER_CYCLE`` total. Returns [] when the toggle is off, no sources are
    configured (the current TODO(HARDWARE) state), or nothing new is on disk. Never raises."""
    if not log_upload_on():
        return []
    already = _uploaded_markers()
    collected: list[dict] = []
    for source in _LOG_SOURCES:
        for payload in _collect_source(source, already):
            collected.append(payload)
            if len(collected) >= _MAX_LOGS_PER_CYCLE:
                return collected
    return collected
