"""Drive-video upload to the MyPilot stack (opt-in).

comma records each ~60s segment to ``/data/media/0/realdata/<route>--<seg>/`` containing:
  qcamera.ts   low-res H.264/MPEG-TS preview  -> web-playable, small (the default we ship)
  fcamera.hevc full-res road H.265 (raw)      -> archive, download-only (no in-browser play)
  ecamera/dcamera.hevc                         -> optional

TWO independent toggles in /data/mypilot/config.json:

  ``drive_upload`` — the road/wide cameras:
    "off"     (default) -> do nothing; comma's own upload behavior is untouched
    "qcamera"           -> upload qcamera.ts per segment (web-playable preview)
    "full"              -> also upload fcamera + ecamera (road + wide) as a download-only archive
    "full_wifi"         -> like "full", but the heavy full-res archive (fcamera + ecamera) uploads
                           ONLY when the device is on WiFi/Ethernet (unmetered); the small preview
                           (qcamera + qlog) still uploads on any link so drives stay viewable on
                           cellular. Held archive files retry on a later cycle once WiFi returns.

  ``cabin_upload`` — the in-cabin / driver-facing camera (SEPARATE on/off, privacy-sensitive):
    false     (default) -> cabin camera neither recorded nor uploaded
    true                -> record the cabin camera (RecordFront=1) AND upload dcamera.hevc

The cabin camera is split into its own toggle precisely because it is the most privacy-sensitive
surface: it is NEVER bundled into "full". It records only when ``cabin_upload`` is true, and arming
it remotely is forbidden (see the gate in the private agent layer) — only the physical on-device
toggle can turn any camera ON.

When ``drive_upload`` is NOT "off" we set comma's ``OnroadUploads`` param to 0 so the device stops
uploading qcamera/qlog to comma — MyPilot becomes the upload target — restoring it to 1 when off.
``OnroadUploads`` and ``RecordFront`` are EXISTING declared params, so toggling them is prebuilt-safe
(no new key). Recording of the road/wide cameras is never touched.

Safety: a segment is only read once its ``*.lock`` files are gone (comma removes them when the
segment is closed) — we never read a segment that's still being written. Uploaded files are tracked
in ``/data/mypilot/uploaded_segments.json`` so reboots resume without re-uploading.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

REALDATA = "/data/media/0/realdata"
STATE_FILE = "/data/mypilot/uploaded_segments.json"
CONFIG_FILE = "/data/mypilot/config.json"

# <8 hex>--<10 hex>--<segnum>   e.g. 0000008c--abc1234567--3
SEG_RE = re.compile(r"^([0-9a-f]{8}--[0-9a-f]{10})--(\d+)$")

# files we ship per mode (qlog rides along so the drive has metadata)
QCAMERA_FILES = ["qcamera.ts", "qlog.zst", "qlog"]
FULL_EXTRA = ["fcamera.hevc", "ecamera.hevc"]   # road + wide (NOT the cabin camera)
CABIN_EXTRA = ["dcamera.hevc"]                   # in-cabin / driver camera — only when cabin_upload
KIND = {
    "qcamera.ts": "qcamera", "fcamera.hevc": "fcamera",
    "ecamera.hevc": "ecamera", "dcamera.hevc": "dcamera",
    "qlog.zst": "qlog", "qlog": "qlog",
}

# Closed set of valid drive_upload values (cabin is a SEPARATE boolean key, not a level here).
DRIVE_UPLOAD_MODES = ("off", "qcamera", "full", "full_wifi")

# Modes that ship the heavy full-res archive (fcamera + ecamera). "full_wifi" additionally gates that
# archive on the WiFi check below; "full" ships it on any link.
_FULL_MODES = ("full", "full_wifi")

# The default route table Linux exposes at /proc/net/route. get_default_route_iface() parses it to find
# the interface carrying the default route, WITHOUT touching the openpilot msgq bus (Hard Rule #2).
_PROC_NET_ROUTE = "/proc/net/route"
_RTF_UP = 0x0001  # route is usable (flags column)


def get_default_route_iface() -> str | None:
    """The interface name carrying the default route (dest 0.0.0.0, RTF_UP), or None. Parses
    /proc/net/route directly — routing-based, msgq-free (this is openpilot's own approach). Any read
    error returns None (caller treats unknown as "not WiFi" -> hold)."""
    try:
        with open(_PROC_NET_ROUTE) as fh:
            next(fh, None)  # header row
            for line in fh:
                parts = line.split()
                if len(parts) < 4:
                    continue
                iface, dest, flags = parts[0], parts[1], parts[3]
                # Default route: destination 0.0.0.0 (all-zero hex) and the route is UP.
                if dest == "00000000" and (int(flags, 16) & _RTF_UP):
                    return iface
    except Exception:  # noqa: BLE001 - unreadable/malformed -> unknown iface (fail closed upstream)
        return None
    return None


def wifi_ok_for_archive(params) -> bool:
    """Decide whether the heavy full-res archive may upload RIGHT NOW, msgq-free and FAIL-CLOSED.

    Two independent, AND-ed reads (per the Hardware-confirmed spec, D-0032):
      1. PRIMARY: the ``NetworkMetered`` param (persistent bool, written by hardwared ~every 10s, runs
         offroad too). Proceeds ONLY on an explicit ``False``; ``True`` (metered) OR a missing/``None``
         value -> HOLD. Read via the same Params object real.py holds (a file read under /data/params,
         NOT the driving msgq bus).
      2. POSITIVE CHECK: the default-route interface must be WiFi/Ethernet (``wlan*``/``eth*``).

    allow = (NetworkMetered is False) AND (default iface startswith wlan/eth). Each read fails closed
    on its own — a None/unknown metered value holds on the primary read alone, not merely via the iface
    backstop. ANY exception or unreadable value on EITHER read -> False (hold the archive). Never
    raises. The caller computes this ONCE per offroad upload cycle and reuses it for every file (no
    per-file polling)."""
    if params is None:
        return False
    try:
        metered = params.get_bool("NetworkMetered")
        # PRIMARY read fails closed on its own: proceed ONLY on an explicit False. True (metered) OR
        # None/missing/anything-not-False -> HOLD. This makes the metered read independently
        # money-safe rather than leaning on the iface backstop to catch a None param.
        if metered is not False:
            return False
        iface = get_default_route_iface()
        return bool(iface) and (iface.startswith("wlan") or iface.startswith("eth"))
    except Exception:  # noqa: BLE001 - ANY unreadable/error on EITHER read -> hold the archive
        return False


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


def drive_upload_mode() -> str:
    mode = (_read_json(CONFIG_FILE, {}) or {}).get("drive_upload", "off")
    return mode if mode in DRIVE_UPLOAD_MODES else "off"


def cabin_upload_on() -> bool:
    """Whether the separate cabin (driver-facing) camera toggle is enabled."""
    return bool((_read_json(CONFIG_FILE, {}) or {}).get("cabin_upload", False))


def enforce_comma_upload(params, mode: str, cabin: bool) -> None:
    """Keep comma's params in sync with the two toggles. Best-effort; both keys are declared params,
    so this is prebuilt-safe.

      * OnroadUploads: 0 while MyPilot road/wide upload is on (MyPilot is the upload target), 1 off.
      * RecordFront:   tracks the SEPARATE cabin toggle — 1 iff cabin_upload is true, else 0 — so the
        driver camera is never recorded unless the cabin toggle is explicitly on. Affects future
        drives (camerad reads RecordFront at drive start).
    """
    if params is None:
        return
    try:
        want_comma_off = mode != "off"
        cur = params.get_bool("OnroadUploads")
        if want_comma_off and cur:
            params.put_bool("OnroadUploads", False)
            print("[drive] MyPilot upload on -> disabled comma OnroadUploads", flush=True)
        elif not want_comma_off and not cur:
            params.put_bool("OnroadUploads", True)
            print("[drive] MyPilot upload off -> restored comma OnroadUploads", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[drive] could not set OnroadUploads: {exc}", flush=True)
    try:
        if params.get_bool("RecordFront") != cabin:
            params.put_bool("RecordFront", cabin)
            print(f"[drive] cabin camera recording -> {'on' if cabin else 'off'} (RecordFront)", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[drive] could not set RecordFront: {exc}", flush=True)


def _segment_complete(seg_dir: str) -> bool:
    """A segment is safe to read only when it has no .lock files (comma removes them on close)."""
    try:
        return not any(n.endswith(".lock") for n in os.listdir(seg_dir))
    except OSError:
        return False


def _wanted_files(mode: str, cabin: bool) -> list[str]:
    files = list(QCAMERA_FILES)
    if mode in _FULL_MODES:         # "full" and "full_wifi" both COLLECT the archive; full_wifi gates
        files += FULL_EXTRA         # the actual upload on WiFi (deferred-retry when on cellular)
    if cabin:                       # separate cabin toggle adds the driver camera, independent of mode
        files += CABIN_EXTRA
    return files


def scan_drives(mode: str, cabin: bool) -> dict:
    """Group completed segments by route -> {route_name: {seg_index: [filenames]}}."""
    drives: dict[str, dict] = {}
    if mode == "off" or not os.path.isdir(REALDATA):
        return drives
    wanted = set(_wanted_files(mode, cabin))
    for entry in sorted(os.listdir(REALDATA)):
        m = SEG_RE.match(entry)
        if not m:
            continue
        seg_dir = os.path.join(REALDATA, entry)
        if not os.path.isdir(seg_dir) or not _segment_complete(seg_dir):
            continue
        route, seg = m.group(1), int(m.group(2))
        present = [n for n in os.listdir(seg_dir) if n in wanted]
        if present:
            drives.setdefault(route, {})[seg] = present
    return drives


# GPS message names: external puck (comma 3X) publishes gpsLocationExternal; the comma-4 qcom modem
# publishes gpsLocation. Both structs share latitude/longitude/speed/hasFix — accept either.
_GPS_SERVICES = ("gpsLocationExternal", "gpsLocation")
_TRACK_MIN_DT_S = 1.0    # keep a point at most ~1/sec ...
_TRACK_MIN_MOVE_M = 5.0  # ... unless we've moved >5m (collapses time stopped at a light)
_TRACK_MIN_SPEED_MS = 0.5  # drop fixes while stationary (<0.5 m/s ≈ 1.1 mph) — kills parked GPS jitter


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


_VIDEO_FPS = 20.0          # qcamera encode rate
_SEGMENT_SECONDS = 60.0    # comma segments are 60s; segmentNum * 60 = a segment's video offset
_ENCODE_SERVICES = ("qRoadEncodeIdx", "roadEncodeIdx")


def _video_time_for(mono: float, frames: list) -> float | None:
    """Map a log monotonic time to its VIDEO time (seconds into the concatenated qcamera) via the
    nearest encode frame. ``frames`` is a sorted list of (logMonoTime, video_time). This is the
    deterministic video<->log bridge — no 'drive start' guess. None if no frames."""
    if not frames:
        return None
    import bisect
    monos = [f[0] for f in frames]
    j = bisect.bisect_left(monos, mono)
    # pick whichever neighbouring frame is closest in time
    best = j if j < len(frames) else len(frames) - 1
    if j > 0 and (best >= len(frames) or abs(frames[j - 1][0] - mono) <= abs(frames[best][0] - mono)):
        best = j - 1
    return frames[best][1]


def _track_from_segment(qlog_path: str, seg_index: int, out: list, last: list) -> None:
    """Append downsampled [t, lat, lon] points from ONE qlog segment to ``out`` (in place), where t is
    the VIDEO time (seconds into the concatenated qcamera) of each GPS fix.

    Time is anchored DETERMINISTICALLY to the video frames when available: each encode-index message
    carries a frame's capture logMonoTime + its position in the stream (segmentNum*60 + segmentId/fps),
    so we map every GPS fix to its nearest frame's video time — exact and drift-free. FALLBACK: if a
    segment's qlog has no encode-index messages (qcamera encoder off, or a rotated/partial qlog), we
    derive video time as seg_index*60 + (fix_mono - segment_first_mono) instead of dropping every fix
    (the bug where whole segments contributed nothing and many drives ended up with a 0/1-point track).
    ``last`` holds [lat, lon, t] of the last kept point so downsampling spans the whole route. Reads
    one qlog at a time. Best-effort."""
    try:
        from openpilot.tools.lib.logreader import LogReader  # device-only; lazy import
    except Exception as exc:  # noqa: BLE001
        print(f"[drive] track: LogReader unavailable ({exc})", flush=True)
        return
    try:
        # Pass 1: build the encode-frame timeline (logMonoTime -> video_time) and collect fixes.
        frames: list = []
        fixes: list = []  # (mono, lat, lon, speed_or_None)
        seg_first_mono: float | None = None
        for msg in LogReader(qlog_path):
            w = msg.which()
            mono = msg.logMonoTime / 1e9
            if seg_first_mono is None:
                seg_first_mono = mono
            if w in _ENCODE_SERVICES:
                e = getattr(msg, w)
                vt = e.segmentNum * _SEGMENT_SECONDS + e.segmentId / _VIDEO_FPS
                frames.append((mono, vt))
            elif w in _GPS_SERVICES:
                g = getattr(msg, w)
                if not getattr(g, "hasFix", False):
                    continue
                lat, lon = float(g.latitude), float(g.longitude)
                if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0) or (lat == 0.0 and lon == 0.0):
                    continue
                # speed may be absent on some GNSS sources — keep None (don't coerce to 0.0, which
                # would falsely look "stationary" and drop the whole segment); derive it in pass 2.
                raw_speed = getattr(g, "speed", None)
                speed = float(raw_speed) if raw_speed is not None else None
                fixes.append((mono, lat, lon, speed))
        frames.sort()
        if not frames and seg_first_mono is not None:
            print(f"[drive] track: segment {seg_index} has no encode frames — "
                  f"using wall-clock fallback for {len(fixes)} fix(es)", flush=True)

        # Pass 2: assign each fix its video time, downsample, append. Drop fixes recorded while
        # STATIONARY (a parked-but-on car jitters several metres at ~0 speed -> a "scribble"). Speed
        # is the signal for "actually moving"; when the GNSS speed field is missing we DERIVE ground
        # speed from the distance/time to the previous fix instead of dropping everything. The very
        # first kept point is always allowed so a route still anchors at its origin.
        kept = 0
        prev_mono: float | None = None
        prev_ll: tuple[float, float] | None = None
        for mono, lat, lon, speed in fixes:
            # Derive speed from fix deltas when the field is absent.
            if speed is None:
                if prev_mono is not None and mono > prev_mono and prev_ll is not None:
                    speed = _haversine_m(prev_ll[0], prev_ll[1], lat, lon) / (mono - prev_mono)
                else:
                    speed = 0.0
            prev_mono, prev_ll = mono, (lat, lon)
            if speed < _TRACK_MIN_SPEED_MS and out:
                continue
            if frames:
                t = _video_time_for(mono, frames)
                if t is None:
                    continue
            elif seg_first_mono is not None:
                # No encode timeline: anchor to the segment's wall-clock offset within the drive.
                t = seg_index * _SEGMENT_SECONDS + (mono - seg_first_mono)
            else:
                continue
            if last and (t - last[2]) < _TRACK_MIN_DT_S and \
                    _haversine_m(last[0], last[1], lat, lon) < _TRACK_MIN_MOVE_M:
                continue
            out.append([round(t, 1), round(lat, 5), round(lon, 5)])
            last[:] = [lat, lon, t]
            kept += 1
    except Exception as exc:  # noqa: BLE001 - corrupt/partial qlog must never break the upload
        print(f"[drive] track: segment {seg_index} parse failed ({exc})", flush=True)
        return


def extract_route_track(route: str, segs: dict) -> list | None:
    """Build the route's GPS polyline by concatenating per-segment tracks IN ORDER. Returns a list of
    [t, lat, lon] (t = seconds since drive start; possibly empty if no fix), or None on total failure.
    One segment is parsed at a time."""
    try:
        out: list = []
        last: list = []
        for seg in sorted(segs):
            for name in ("qlog.zst", "qlog"):
                p = os.path.join(REALDATA, f"{route}--{seg}", name)
                if os.path.isfile(p):
                    _track_from_segment(p, int(seg), out, last)
                    break
        # Observability guard: a multi-segment route whose whole track fits inside the first 60s
        # segment is the truncation signature — surface it instead of shipping silently.
        if len(segs) > 1 and out and max(p[0] for p in out) <= _SEGMENT_SECONDS + 1.0:
            print(f"[drive] WARN track for {route} truncated at the first segment "
                  f"({len(out)} pts, {len(segs)} segs) — check encode frames / speed", flush=True)
        return out
    except Exception as exc:  # noqa: BLE001
        print(f"[drive] track: extract failed for {route} ({exc})", flush=True)
        return None


# Earliest year we trust as a real drive time. The comma 4 has NO working RTC: before NTP syncs
# post-boot it reads ~1970, and a copy/restore can bulk-touch old segments. So a segment mtime whose
# year is below this is IMPLAUSIBLE — we send nothing and let the server fall back to created_at
# (upload time), which its metadata backfill already fills when started_at is null. (Hardware-confirmed
# clock caveat, DEVICE-BATCH-001 Item 2.)
_MIN_PLAUSIBLE_YEAR = 2020


def _route_started_at(route: str, segs: dict) -> str | None:
    """Best-effort drive-start timestamp = the EARLIEST segment directory's mtime, as an ISO8601 UTC
    string — or None when we can't trust it.

    The segment dir mtime is written once when comma closes the segment and is NOT re-touched by a later
    upload (Hardware-confirmed on the live device), so the earliest (min seg index) segment's mtime is
    the drive's real record wall-clock. The route name carries no clock (it's a sequential counter +
    random uuid), and the GPS track only has relative seconds-since-start, so mtime is the only absolute
    anchor on-device.

    GUARD (mandatory): if the mtime is IMPLAUSIBLE — year < ``_MIN_PLAUSIBLE_YEAR`` (a pre-NTP ~1970
    read, or a fresh-boot/bulk-touched artifact) — return None so no bogus "Recorded: 1970" reaches the
    server; its created_at fallback fills started_at instead. Any stat/parse error also returns None.
    Pure file I/O (os.stat) — msgq-free, offroad-only (called from collect_uploads)."""
    try:
        if not segs:
            return None
        earliest_seg = min(segs)  # min segment index == drive start
        seg_dir = os.path.join(REALDATA, f"{route}--{earliest_seg}")
        mtime = os.stat(seg_dir).st_mtime
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        if dt.year < _MIN_PLAUSIBLE_YEAR:
            print(f"[drive] {route}: earliest-segment mtime {dt.isoformat()} implausible "
                  f"(< {_MIN_PLAUSIBLE_YEAR}) — omitting started_at, server will derive", flush=True)
            return None
        return dt.isoformat()
    except Exception as exc:  # noqa: BLE001 - a bad stat must never break the upload
        print(f"[drive] {route}: could not derive started_at ({exc})", flush=True)
        return None


def collect_uploads(mode: str, cabin: bool = False) -> list[dict]:
    """Build per-route upload payloads (matching IngestClient.upload_route's shape), skipping files
    already uploaded. ``cabin`` (the separate cabin toggle) adds dcamera; when drive_upload is "off"
    nothing uploads regardless (cabin recording still happens on-device via RecordFront).

    Each file carries its on-disk ``path`` (NOT its bytes) so the uploader can stream it in chunks —
    a tens-of-MB segment is never read fully into RAM (buffering whole files here OOM'd the device).
    """
    if mode == "off":
        return []
    done = set(_read_json(STATE_FILE, []) or [])  # entries: "route/seg/name" + "route/track"
    drives = scan_drives(mode, cabin)
    out = []
    for route, segs in drives.items():
        files = []
        for seg, names in sorted(segs.items()):
            for name in names:
                marker = f"{route}/{seg}/{name}"
                if marker in done:
                    continue
                path = os.path.join(REALDATA, f"{route}--{seg}", name)
                if not os.path.isfile(path):
                    continue
                files.append({
                    "segment_index": seg, "name": name,
                    "kind": KIND.get(name, "qlog"), "path": path, "_marker": marker,
                })
        # GPS track: carried in the routes/start metadata body, not as a file; its marker is persisted
        # by the runner only when the server confirms it stored the track (see uploader.upload_route).
        # The marker is keyed by SEGMENT COUNT so a route that has since GROWN (more segments finished
        # since the last extraction) gets a NEW marker -> re-extracts + re-sends the fuller track,
        # which the server adopts (grow-only). Same seg count = same marker = skip, so steady-state
        # re-scans still don't re-parse. Device half of the "track truncated at the first segment" fix.
        track_marker = f"{route}/track/{len(segs)}"
        track = None
        if track_marker not in done:
            track = extract_route_track(route, segs)
        # started_at = earliest-segment dir mtime (the only absolute drive-time anchor on-device), or
        # None when implausible/unreadable — the server's null-guard then derives it from created_at, so
        # a device-supplied value always WINS but a bad clock never surfaces (DEVICE-BATCH-001 Item 2).
        # ended_at is intentionally NOT sent: the server keeps computing it as started_at + duration_s
        # (ROUTE-TIMESTAMPS Phase 1, already shipped), the least device code for the accurate start.
        # Emit the route when there are files to upload OR a not-yet-sent track to deliver.
        if files or track is not None:
            out.append({
                "name": route,
                "segment_count": len(segs),
                "started_at": _route_started_at(route, segs),
                "files": files,
                "track": track,
                "_track_marker": track_marker if track is not None else None,
            })
    return out


def mark_uploaded(markers: list[str]) -> None:
    done = set(_read_json(STATE_FILE, []) or [])
    done.update(markers)
    _write_json_atomic(STATE_FILE, sorted(done))
