"""Real comma-device backend (openpilot/SunnyPilot).

Implements the :class:`DeviceBackend` contract against on-device APIs (Params, cereal, hardware,
SunnyPilot's Model Manager + updater). openpilot imports are **lazy** so the package still imports
on a dev machine; on a comma device the methods do real work over the network — no SSH.

All driving-affecting actions are **offroad-only** (defense in depth on top of the Stack's gate),
and nothing here touches controlsd / pandad / driver monitoring / torque or panda safety.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone

from mypilot_protocol.telemetry import (
    GPS_STATUSES,
    THERMAL_STATUSES,
    UPDATE_STATES,
    norm_enum,
)

from .base import DeviceBackend

PAIRING_ALERT = "Offroad_MyPilotPairing"
# Prebuilt-safe pairing surface: the on-screen settings panel (MyPilot Link → Pair) reads this file
# to render the QR + PIN. We can't use a Params key for this — on a prebuilt branch params_pyx.so is
# precompiled, so writing an undeclared key (Offroad_MyPilotPairing) raises UnknownKeyName. A plain
# file under the agent's data dir works on every branch with no recompile.
PAIRING_FILE = "/data/mypilot/pairing.json"
# Web pair page; the QR encodes this with the one-time code so a phone scan prefills the PIN.
PAIR_URL_BASE = "https://mypilot.me/devices/pair"


def _try_params():
    try:
        from openpilot.common.params import Params  # type: ignore

        return Params()
    except Exception:  # noqa: BLE001 - not on-device / openpilot not importable
        return None


# SAFETY INVARIANT — NO PERSISTENT msgq DRIVING-BUS READER.
# ---------------------------------------------------------------------------------------------
# This agent must NEVER hold a persistent (let alone conflate=True) subscriber on openpilot's
# realtime msgq bus — above all NOT on a high-rate safety-critical service like carState (100 Hz,
# consumed by controlsd). On 2026-06-29 the old `_Sampler` (persistent conflate readers on
# deviceState + carState + gpsLocation, polled once per heartbeat) caused "TAKE CONTROL IMMEDIATELY
# / Communication Issue Between Processes" (commIssue/softDisable) onroad. Proven two-way on-device:
# kill -STOP the agent -> alert clears in ~8s; kill -CONT -> alert returns in ~10s. The agent was
# SCHED_IDLE at 0.1% CPU, so it was NOT CPU contention — the harm is a sluggish reader sitting on
# the shared ring of a high-rate service, perturbing the safety consumers' receive timing out of
# cereal's FrequencyTracker validity window (0.8x-1.2x of nominal) -> valid=False -> commIssue.
#
# All telemetry here therefore comes from BUS-FREE sources that never touch the msgq SHM bus:
#   - Params reads (a separate file-backed store; reading never registers a msgq reader)
#   - sysfs / HARDWARE API (thermal, power, network)
#   - statvfs (storage)
#   - LastGPSPositionLLK Param for a coarse position pin (locationd writes it ~1/min)
# The detailed route track is reconstructed from RECORDED qlog files post-drive (drive_video.py,
# LogReader) — also bus-free. Live speed/heading/gear (carState-only) are deliberately dropped.
#
# The ONE sanctioned bus reader is an opt-in (MYPILOT_LIVE_GPS=1, OFF by default) live map pin: a
# SINGLE persistent conflate reader on the LOW-RATE GPS service (gpsLocation 1 Hz / gpsLocationExternal
# 10 Hz). That is comma athenad's safe shape (athenad holds deviceState persistently onroad with no
# commIssue). Why it's safe where the old reader wasn't: GPS is low-rate (publisher signal tax is ~1
# syscall/s, not 100), it's its OWN msgq segment (not the carState ring controlsd consumes), and
# selfdrived ignore-lists it (ignore_valid/alive/freq) so GPS timing CANNOT feed the commIssue logic.
# Speed+heading+position all ride in that one GPS message, so speed costs no extra read. `gear` lives
# ONLY on carState (100 Hz, validity load-bearing) and is dropped — no safe source exists for it.
# A single persistent reader (vs fresh-socket-per-read) also avoids any msgq reader-slot churn.
# Stays OFF until proven on-device with the freeze/unfreeze test. The CI guard (agent-no-driving-bus)
# + tests/test_no_driving_bus.py enforce: NEVER carState/SubMaster/drain_sock, and the only sub_sock
# is the audited GPS one (tagged `mypilot-gps-reader`). See SAFETY-HANDOFF / mypilot-agent README.

# Coarse position pin: locationd persists the last good GPS fix to this Param as JSON
# {latitude, longitude, altitude} roughly once a minute (only with a fix). Reading a Param is
# bus-free, so this gives the map a pin with zero msgq interaction.
_LAST_GPS_PARAM = "LastGPSPositionLLK"
# SoC temperature -> dashboard color, derived from openpilot hardwared's onroad thermal bands
# (ok < overheated(92) < critical(98) on the comma SoC). We only need a coarse green/yellow/red.
_THERMAL_YELLOW_C = 92.0
_THERMAL_RED_C = 98.0


def _device_type() -> str | None:
    try:
        from openpilot.system.hardware import HARDWARE  # type: ignore

        return HARDWARE.get_device_type()
    except Exception:  # noqa: BLE001
        return None


class RealDevice(DeviceBackend):
    def __init__(self, hardware_id: str, hostname: str) -> None:
        self.hardware_id = hardware_id
        self.hostname = hostname
        self._params = _try_params()
        # No msgq subscriber on a high-rate/safety service — see the SAFETY INVARIANT note above.
        # The ONLY allowed bus reader is a single persistent one on the low-rate GPS service, opened
        # lazily and ONLY when MYPILOT_LIVE_GPS=1 (off by default). These hold that handle.
        self._thermal_zones = None  # lazily-built {name: zone_number} sysfs map (on-device only)
        self._gps_sock = None
        self._gps_messaging = None
        self._gps_last = None  # (monotonic_ts, last good _live_gps dict) — smooths the conflate race
        self._http = None
        self._started = time.time()
        self._device_type = _device_type()
        self.update_channel = self._param_str("UpdaterTargetBranch") or self._param_str("GitBranch")
        # Per-tick read cache. status() runs on the event loop every heartbeat; without this it would
        # do ~9-11 Params IPC round-trips + a statvfs + 2 JSON parses + a capnp decode EVERY tick,
        # most for values that change rarely or never. Caching slow/static reads keeps the per-tick
        # cost ~constant so the agent never spikes the shared cores (commIssue). Values stay current
        # within their TTL; immutable ones (platform/version/branch) are read once. NB: `onroad` is
        # deliberately NOT cached — command guards depend on a fresh read for safety.
        self._cache: dict[str, tuple[float, object]] = {}

    def _cached(self, key: str, ttl: float, producer):
        """Return producer()'s value, recomputing only when older than ttl seconds. ttl<=0 means
        cache-once-forever (for values that can't change during a session, e.g. the car platform)."""
        now = time.time()
        hit = self._cache.get(key)
        if hit is not None and (ttl <= 0 or (now - hit[0]) < ttl):
            return hit[1]
        val = producer()
        # Cache-once entries (ttl<=0) only stick once they resolve to something real, so a transient
        # None (e.g. platform before the car is seen) is retried next tick rather than frozen.
        if ttl > 0 or val is not None:
            self._cache[key] = (now, val)
        return val

    def _invalidate(self, *keys: str) -> None:
        for k in keys:
            self._cache.pop(k, None)

    # --- wiring -------------------------------------------------------------------------------
    def attach_http(self, client) -> None:
        self._http = client

    def _param_str(self, key: str) -> str | None:
        if self._params is None:
            return None
        try:
            # NB: the on-device (prebuilt) Params.get has signature get(key, block, return_default)
            # with NO encoding kwarg — passing encoding= raises TypeError and would silently break
            # all status/version reporting. It already returns a decoded str for STRING keys.
            val = self._params.get(key)
            if isinstance(val, bytes):
                val = val.decode("utf8", "replace")
            return val or None
        except Exception:  # noqa: BLE001
            return None

    def _read_json_param(self, key: str):
        if self._params is None:
            return None
        try:
            val = self._params.get(key)
        except Exception:  # noqa: BLE001
            return None
        if val is None:
            return None
        if isinstance(val, (bytes, str)):
            try:
                return json.loads(val)
            except Exception:  # noqa: BLE001
                return None
        return val  # fork's Params.get may already decode JSON to dict/list

    def _platform(self) -> str | None:
        """The detected car, from the CarParams capnp struct (carFingerprint, e.g.
        'CHRYSLER_RAM_HD'). CarName/CarModel are NOT params on a prebuilt branch (they raise
        UnknownKeyName), so the old read always came back blank. CarParams is written once the car
        is recognized; CarParamsPersistent survives across the offroad transition so the dashboard
        still shows the platform while parked. Returns None until a car has been seen at least once."""
        if self._params is None:
            return None
        try:
            from cereal import car, messaging  # type: ignore
        except Exception:  # noqa: BLE001 - not on-device
            return None
        for key in ("CarParams", "CarParamsPersistent", "CarParamsCache"):
            try:
                raw = self._params.get(key)
            except Exception:  # noqa: BLE001
                continue
            if not raw:
                continue
            try:
                cp = messaging.log_from_bytes(raw, car.CarParams)
                fp = cp.carFingerprint or None
                # carFingerprint is an upper-snake platform code; make it a touch friendlier.
                return fp.replace("_", " ").title() if fp else None
            except Exception:  # noqa: BLE001 - malformed/partial struct
                continue
        return None

    @staticmethod
    def _bundles(cache) -> list:
        if isinstance(cache, list):
            return [b for b in cache if isinstance(b, dict)]
        if isinstance(cache, dict):
            if isinstance(cache.get("bundles"), list):
                return [b for b in cache["bundles"] if isinstance(b, dict)]
            vals = [v for v in cache.values() if isinstance(v, dict)]
            if vals:
                return vals
        return []

    @staticmethod
    def _bundle_name(b: dict) -> str | None:
        # SunnyPilot's Model Manager catalog uses snake_case: display_name ("LAv2 (January 24,
        # 2024)") + short_name ("LAV2"). Older/other forks may use camelCase displayName — accept
        # both. The ref is a 40-char content hash; only use it as a last-resort label.
        return (
            b.get("display_name")
            or b.get("displayName")
            or b.get("short_name")
            or b.get("ref")
        )

    def _active_model_ref(self) -> str | None:
        active = self._read_json_param("ModelManager_ActiveBundle")
        if isinstance(active, dict):
            # key must stay the stable ref (the Stack round-trips it back as model_key); the label
            # is resolved from it in _available_models / the Stack view.
            return active.get("ref") or self._bundle_name(active)
        return None

    def _available_models(self) -> list[dict]:
        bundles = self._bundles(self._read_json_param("ModelManager_ModelsCache"))
        out = []
        for b in bundles:
            # key = the stable identifier the switch flow matches on (ref preferred, else index).
            key = b.get("ref") or (str(b["index"]) if "index" in b else None)
            if not key:
                continue
            out.append(
                {
                    "key": key,
                    "name": self._bundle_name(b) or key,
                    "generation": str(b["generation"]) if b.get("generation") is not None else None,
                    "runner": b.get("runner"),
                }
            )
        return out

    # --- state --------------------------------------------------------------------------------
    @property
    def onroad(self) -> bool:
        # The IsOnroad Param is the bus-free source of truth (hardwared writes it on every onroad
        # transition). Read FRESH every call — never cached — because the command guards depend on it.
        # The old deviceState.started fallback is GONE: it required a persistent msgq sub, the very
        # thing that caused commIssue. IsOnroad alone is authoritative and sufficient.
        if self._params is not None:
            try:
                return bool(self._params.get_bool("IsOnroad"))
            except Exception:  # noqa: BLE001
                # FAIL-CLOSED (SR-1): on-device, a throwing IsOnroad read means we DON'T KNOW the
                # driving state. Treat unknown as ONROAD so the driving-affecting command guards
                # (`if self.onroad: refuse`) REFUSE rather than allow — never let a read glitch permit
                # a reboot/update/model-switch/settings change on a moving car. Log loudly so the
                # anomaly is visible. (The off-device `_params is None` case below stays False: no real
                # car, and forcing onroad there would break every dev/test path.)
                print("[mypilot] IsOnroad read failed — failing CLOSED to onroad (refusing "
                      "driving-affecting commands)", flush=True)
                return True
        return False

    @staticmethod
    def _thermal_color(max_c) -> str | None:
        # Derive a coarse green/yellow/red from the hottest SoC temperature, using openpilot
        # hardwared's onroad thermal bands (ok < overheated(92C) < critical(98C)). We no longer have
        # deviceState.thermalStatus (that needed a msgq sub); the band is computed from sysfs temps.
        if max_c is None:
            return None
        if max_c >= _THERMAL_RED_C:
            color = "red"
        elif max_c >= _THERMAL_YELLOW_C:
            color = "yellow"
        else:
            color = "green"
        return norm_enum(color, THERMAL_STATUSES)

    def _thermal_zone_map(self) -> dict:
        """Lazily build {zone_type_name: zone_number} from /sys/class/thermal — built once per
        session and cached. Pure file reads; never touches the msgq bus. Empty off-device."""
        if self._thermal_zones is not None:
            return self._thermal_zones
        zones: dict = {}
        base = "/sys/class/thermal"
        try:
            for entry in os.listdir(base):
                if not entry.startswith("thermal_zone"):
                    continue
                try:
                    with open(os.path.join(base, entry, "type")) as fh:
                        zones[fh.read().strip()] = entry
                except OSError:
                    continue
        except OSError:  # not on-device / path missing
            pass
        self._thermal_zones = zones
        return zones

    def _zone_temp_c(self, name: str) -> float | None:
        """Read one thermal zone's temperature in C (sysfs reports milli-C), or None. Bus-free."""
        entry = self._thermal_zone_map().get(name)
        if entry is None:
            return None
        try:
            with open(f"/sys/class/thermal/{entry}/temp") as fh:
                return int(fh.read().strip()) / 1000.0
        except (OSError, ValueError):
            return None

    def _zones_max_c(self, names) -> float | None:
        """Hottest valid reading across the given zone names (drops sentinel <= -100 / unreadable)."""
        vals = [t for t in (self._zone_temp_c(n) for n in names) if t is not None and t > -100]
        return round(max(vals), 1) if vals else None

    def _read_thermal(self) -> dict:
        """Bus-free thermal snapshot built from /sys/class/thermal (the zones hardwared itself reads
        via get_thermal_config). Mirrors the old deviceState-derived shape:
        {status, max_c, cpu_c, gpu_c, memory_c, ambient_c}. All None off-device."""
        cpu = self._zones_max_c([f"cpu{i}-silver-usr" for i in range(4)]
                                + [f"cpu{i}-gold-usr" for i in range(4)])
        gpu = self._zones_max_c(["gpu0-usr", "gpu1-usr"])
        memory = self._zone_temp_c("ddr-usr")
        # mici exposes a board "bottom_soc" zone; fall back to None (== old bottomSocTempC ambient).
        ambient = self._zone_temp_c("bottom_soc")
        cands = [v for v in (cpu, gpu, memory, ambient) if v is not None]
        max_c = round(max(cands), 1) if cands else None
        return {"status": self._thermal_color(max_c), "max_c": max_c,
                "cpu_c": cpu, "gpu_c": gpu, "memory_c": memory, "ambient_c": ambient}

    def _coarse_position(self) -> tuple[float | None, float | None]:
        """A coarse position pin from the LastGPSPositionLLK Param (locationd writes it ~1/min with a
        fix). Bus-free. Returns (lat, lon) or (None, None). The detailed route track is reconstructed
        from recorded qlogs post-drive (drive_video), so this is just the 'where is it now-ish' pin."""
        data = self._read_json_param(_LAST_GPS_PARAM)
        if not isinstance(data, dict):
            return None, None
        try:
            lat = float(data["latitude"])
            lon = float(data["longitude"])
        except (KeyError, TypeError, ValueError):
            return None, None
        if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0 and not (lat == 0.0 and lon == 0.0):
            return round(lat, 6), round(lon, 6)
        return None, None

    def _ensure_gps_sock(self):
        """Lazily open ONE persistent conflate reader on whichever GPS service is actually PUBLISHING,
        and reuse it for the whole session. This is comma athenad's safe shape — a single registered
        reader on a LOW-RATE, ignore-listed, isolated-segment service — NOT a high-rate carState reader
        and NOT a churn of momentary sockets. Returns (messaging, sock) or None.

        IMPORTANT: which GPS service is live is hardware-dependent — the comma-4 qcom modem publishes
        gpsLocationExternal on this device, NOT gpsLocation (which is registered but SILENT). We prefer
        gpsLocationExternal (qcom-4 + 3X external puck both publish it) and only fall back to
        gpsLocation if External isn't registered at all. We do NOT gate binding on a probe frame: at
        the 10Hz publish rate a single non-blocking read frequently races empty between frames, so a
        probe-to-confirm would delay binding by many ticks. Binding the right service name is enough;
        empty reads are handled by the drain + last-good fallback in _live_gps. Both candidates are
        ignore-listed by selfdrived, so neither feeds the commIssue validity logic."""
        if self._gps_sock is not None:
            return self._gps_messaging, self._gps_sock
        try:
            from cereal import messaging  # type: ignore
            from cereal.services import SERVICE_LIST  # type: ignore
        except Exception:  # noqa: BLE001 - not on-device
            return None
        for svc in ("gpsLocationExternal", "gpsLocation"):
            if svc not in SERVICE_LIST:
                continue
            try:
                # Single persistent reader on the low-rate GPS service (audited — see SAFETY note + CI
                # guard). conflate -> msgq keeps only the newest frame, so a per-tick read is O(1).
                self._gps_sock = messaging.sub_sock(svc, conflate=True, timeout=0)  # mypilot-gps-reader
            except Exception:  # noqa: BLE001
                continue
            self._gps_messaging = messaging
            print(f"[mypilot] live GPS reader bound to {svc}", flush=True)
            return messaging, self._gps_sock
        return None
        return None

    # Hold the last good live fix this long (s) so an empty conflate read between heartbeats doesn't
    # blank the dashboard speed/heading. Generous vs the 4s onroad heartbeat; well under "stale".
    _GPS_LAST_GOOD_TTL = 10.0

    def _gps_last_good(self):
        """Return the most recent live GPS reading if it's still fresh, else None. Used when a given
        tick's conflate read comes back empty (race with the publisher) so the map/speed hold steady
        rather than flicker to blank."""
        hit = self._gps_last
        if hit is not None and (time.time() - hit[0]) < self._GPS_LAST_GOOD_TTL:
            return hit[1]
        return None

    def _live_gps(self) -> dict | None:
        """OPT-IN (MYPILOT_LIVE_GPS=1), DISABLED BY DEFAULT. Live position/heading/speed for the map,
        from ONE persistent reader on the GPS service (see _ensure_gps_sock — athenad's safe pattern,
        never carState/100 Hz). Speed is GPS Doppler (same message as position+bearing — no extra
        read). A momentarily-empty read falls back to the last good fix (_gps_last_good) so values
        don't flicker. Stays off until proven on-device with the freeze/unfreeze test. Returns
        {speed_ms, heading_deg, latitude, longitude, accuracy_m, gps_status} or None."""
        if os.environ.get("MYPILOT_LIVE_GPS") != "1":
            return None
        wired = self._ensure_gps_sock()
        if wired is None:
            return self._gps_last_good()
        messaging, sock = wired
        try:
            # Drain to the newest frame: with conflate=True msgq keeps only the latest UNREAD frame,
            # but a single non-blocking read can still race the 10Hz publisher and come back empty on
            # a given heartbeat. Pull all currently-available frames (bounded — there are at most a
            # few) and keep the last; if none are waiting this tick, fall back to the last-good fix so
            # the dashboard speed/heading don't flicker to blank between reads.
            raw = None
            for _ in range(12):  # bounded drain; 10Hz over a ~1s gap is <12 frames, never a hot loop
                nxt = sock.receive(non_blocking=True)
                if nxt is None:
                    break
                raw = nxt
            if raw is None:
                return self._gps_last_good()
            msg = messaging.log_from_bytes(raw)
            g = getattr(msg, msg.which())
            has_fix = bool(getattr(g, "hasFix", False))
            out: dict = {"gps_status": "has_fix" if has_fix else "searching",
                         "speed_ms": None, "heading_deg": None,
                         "latitude": None, "longitude": None, "accuracy_m": None}
            if not has_fix:
                # No fix on a real frame: prefer a recent good fix over reporting "searching" blank.
                return self._gps_last_good() or out
            speed = getattr(g, "speed", None)  # m/s, GPS Doppler (NOT carState.vEgo)
            if speed is not None:
                out["speed_ms"] = round(max(0.0, float(speed)), 2)
            bearing = getattr(g, "bearingDeg", None)
            if bearing is None:
                bearing = getattr(g, "bearing", None)
            # Heading only when actually moving — a near-stationary fix jitters its bearing.
            if bearing is not None and out["speed_ms"] is not None and out["speed_ms"] >= 1.5:
                out["heading_deg"] = round(float(bearing) % 360.0, 1)
            lat = getattr(g, "latitude", None)
            lon = getattr(g, "longitude", None)
            if lat is not None and lon is not None:
                lat, lon = float(lat), float(lon)
                if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0 and not (lat == 0.0 and lon == 0.0):
                    out["latitude"], out["longitude"] = round(lat, 6), round(lon, 6)
                    acc = getattr(g, "horizontalAccuracy", None)
                    if acc is None:
                        acc = getattr(g, "accuracy", None)
                    if acc is not None:
                        out["accuracy_m"] = round(float(acc), 1)
            # Cache this good fix so the next empty-read tick can fall back to it (no flicker).
            self._gps_last = (time.time(), out)
            return out
        except Exception as exc:  # noqa: BLE001 - live GPS is best-effort, never fatal
            print(f"[mypilot] live-gps read error: {exc}", flush=True)
            return self._gps_last_good()

    @staticmethod
    def _storage_bytes() -> dict | None:
        """Absolute /data storage (total/used/free bytes). deviceState only carries a free PERCENT,
        so we statvfs the data partition for real sizes. Used by the dashboard to show e.g.
        "6.1 / 94.5 GB" next to the percentage."""
        try:
            st = os.statvfs("/data")
            total = st.f_blocks * st.f_frsize
            free = st.f_bavail * st.f_frsize
            return {"total_bytes": total, "free_bytes": free, "used_bytes": total - free}
        except Exception:  # noqa: BLE001 - not on-device / path missing
            return None

    def status(self) -> dict:
        """Build the telemetry envelope. See mypilot_protocol.telemetry for the contract: envelope
        {captured_at, onroad, subsystems{...}}, units in keys, enums normalized to a closed set.
        Single source of truth per metric — no duplication."""
        onroad = self.onroad

        # Thermal — bus-free, from /sys/class/thermal (cached briefly; temps move slowly and the
        # sysfs reads are a handful of tiny file opens, but no need to do them every 4s tick).
        thermal = self._cached("thermal", 8.0, self._read_thermal)

        # Position / driving telemetry — ALL bus-free by default.
        #   * Coarse pin: LastGPSPositionLLK Param (locationd writes ~1/min) — no msgq.
        #   * speed/heading/gear: dropped by default (carState is bus-only; reading it caused commIssue).
        # OPT-IN only: MYPILOT_LIVE_GPS=1 enables a momentary, GPS-only, athenad-style live read that
        # upgrades the pin + adds GPS-derived speed/heading. Stays off until freeze-tested on-device.
        gps_status = "no_signal"
        speed_ms = heading_deg = accuracy_m = None
        latitude, longitude = self._coarse_position()
        if latitude is not None:
            gps_status = "has_fix"  # we only store a coarse pin once locationd has had a real fix
        live = self._live_gps()
        if live is not None:
            gps_status = live.get("gps_status") or gps_status
            speed_ms = live.get("speed_ms")
            heading_deg = live.get("heading_deg")
            accuracy_m = live.get("accuracy_m")
            # Prefer the fresh live fix for the pin; fall back to the coarse Param pin if it had none.
            if live.get("latitude") is not None:
                latitude, longitude = live["latitude"], live["longitude"]

        # Storage moves slowly; statvfs every 4s is wasted syscalls — refresh ~every 20s.
        storage = self._cached("storage", 20.0, lambda: self._storage_bytes() or {})
        used_pct = None
        if storage.get("total_bytes"):
            used_pct = round(100.0 * storage["used_bytes"] / storage["total_bytes"], 1)

        # Active model only changes on an explicit switch (we invalidate then); cache ~31s otherwise.
        # TTLs are deliberately co-prime-ish (storage 20 / updater 15 / active 31 / available 37) so
        # the cache misses don't align onto one heavier tick — keeps per-tick IPC flat, no micro-spike.
        active = self._cached("active_model", 31.0, self._active_model_ref)

        return {
            "captured_at": self._now_iso(),
            "onroad": onroad,
            "subsystems": {
                "thermal": thermal,
                "storage": {
                    "used_pct": used_pct,
                    "total_bytes": storage.get("total_bytes"),
                    "used_bytes": storage.get("used_bytes"),
                    "free_bytes": storage.get("free_bytes"),
                },
                "gps": {"status": norm_enum(gps_status, GPS_STATUSES)},
                # Motion telemetry, all BUS-FREE. lat/lon = coarse Param pin (or a momentary GPS fix
                # when MYPILOT_LIVE_GPS=1). speed_ms/heading_deg are null unless that opt-in is on
                # (they need GPS Doppler). `gear` is DROPPED entirely — it lived only on the 100Hz
                # carState bus whose persistent reader caused commIssue, and no consumer requires it.
                # Position streams only behind owner-scoped realtime fan-out (privacy).
                "driving": {"speed_ms": speed_ms, "heading_deg": heading_deg,
                            "latitude": latitude, "longitude": longitude, "accuracy_m": accuracy_m},
                # panda is a stub derived from onroad (no real panda-state source yet).
                "panda": {"status": "connected" if onroad else "available"},
                "power": {"uptime_s": int(time.time() - self._started)},
                # platform (carFingerprint) is immutable for the session -> cache once it resolves.
                "platform": {"name": self._cached("platform", 0, self._platform),
                             "device_type": self._device_type},
                "software": {
                    # Version + branch are boot-constant -> read once. UpdaterState changes rarely.
                    "version": self._cached("version", 0, lambda: self._param_str("Version")),
                    "branch": self._cached("branch", 0, lambda: self._param_str("GitBranch")),
                    "update_channel": self.update_channel,
                    "update_state": norm_enum(
                        self._cached("updater_state", 15.0, lambda: self._param_str("UpdaterState")),
                        UPDATE_STATES) or "idle",
                    "target_version": None,
                },
                "models": {
                    "active_ref": active,
                    "installed_refs": [active] if active else [],
                    "available": self._cached("available_models", 37.0, self._available_models),
                },
            },
        }

    @staticmethod
    def _now_iso() -> str | None:
        # Advisory device sample time. The Stack's receive-time (last_heartbeat_at) is authoritative;
        # the device clock can be skewed pre-NTP, so this is informational only.
        try:
            return datetime.now(timezone.utc).isoformat()
        except Exception:  # noqa: BLE001
            return None

    # --- pairing (on-screen, no SSH) ----------------------------------------------------------
    def show_pairing_code(self, code: str, expires_at: str | None = None) -> None:
        # Primary surface (works on prebuilt): write the code to a file the MyPilot Link → Pair
        # screen reads to render the QR + PIN.
        try:
            os.makedirs(os.path.dirname(PAIRING_FILE), exist_ok=True)
            payload = {
                "code": code,
                "url": f"{PAIR_URL_BASE}?code={code}",
                "expires_at": expires_at,
            }
            tmp = PAIRING_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(payload, fh)
            os.replace(tmp, PAIRING_FILE)  # atomic so the UI never reads a half-written file
        except Exception as exc:  # noqa: BLE001 - display is best-effort, never fatal
            print(f"[mypilot] could not write pairing file: {exc}", flush=True)
        # Secondary surface (only works if the param is declared, e.g. a recompiled build): the
        # home-screen offroad alert. Best-effort; silently a no-op on a stock prebuilt branch.
        try:
            from openpilot.selfdrive.selfdrived.alertmanager import set_offroad_alert  # type: ignore

            set_offroad_alert(PAIRING_ALERT, True, extra_text=code)
        except Exception:  # noqa: BLE001
            pass

    def clear_pairing_code(self) -> None:
        try:
            if os.path.exists(PAIRING_FILE):
                os.remove(PAIRING_FILE)
        except Exception:  # noqa: BLE001
            pass
        try:
            from openpilot.selfdrive.selfdrived.alertmanager import set_offroad_alert  # type: ignore

            set_offroad_alert(PAIRING_ALERT, False)
        except Exception:  # noqa: BLE001
            pass

    # --- settings -----------------------------------------------------------------------------
    def settings_sync_payload(self) -> dict:
        caps: dict = {"protocol_version": 1}
        if self._device_type:
            caps["device_type"] = self._device_type
        # Brand from the detected platform (e.g. "Chrysler Ram Hd" -> "chrysler"); CarName isn't a
        # param on a prebuilt branch. None until a car has been seen.
        platform = self._platform()
        if platform:
            caps["brand"] = platform.split()[0].lower()
        # Report MyPilot-owned config.json toggles the server has a SettingDefinition for, so the
        # web/iOS settings UI shows their real on/off state. log_upload is arm-on-device-only server-
        # side (LOGS-STATUS-001): reporting the value lets the generic gated SettingRow render it —
        # web can only turn it OFF, arming stays on the device. Best-effort read; never fails the sync.
        values: dict = {}
        try:
            from .. import log_upload

            values["log_upload"] = log_upload.log_upload_on()
        except Exception:  # noqa: BLE001 - a config read must never break settings reporting
            pass
        try:
            with open("/data/mypilot/config.json") as _cam_fh:
                _cam_cfg = json.load(_cam_fh) or {}
            values["drive_upload"] = _cam_cfg.get("drive_upload", "off")
            values["cabin_upload"] = bool(_cam_cfg.get("cabin_upload", False))
        except Exception:
            pass
        return {"capabilities": caps, "values": values}

    def apply_setting(self, key: str, value) -> tuple[bool, str]:
        if self.onroad:
            return False, "refused: device is onroad"
        if self._params is None:
            return False, "params unavailable"
        try:
            if key.startswith("mypilot_") or key in ("drive_upload", "cabin_upload"):
                return self._mypilot_write_config(key, value)
            if isinstance(value, bool):
                self._params.put_bool(key, value)
            else:
                # Write the value's NATIVE type (int/float/str). The on-device Params are typed and
                # reject a type mismatch — e.g. an INT param like SpeedLimitMode raises TypeError on
                # put("0") but accepts put(0). The Stack already sends correctly-typed JSON, so pass
                # it straight through; only coerce a genuine string param.
                self._params.put(key, value)
            return True, "applied"
        except Exception as exc:  # noqa: BLE001
            return False, f"params write failed: {exc}"

    # --- commands -----------------------------------------------------------------------------
    async def execute(self, name: str, args: dict) -> tuple[bool, str]:
        if name == "reboot":
            if self.onroad:
                return False, "refused: device is onroad"
            try:
                from openpilot.system.hardware import HARDWARE  # type: ignore

                HARDWARE.reboot()
                return True, "rebooting"
            except Exception as exc:  # noqa: BLE001
                return False, f"reboot unavailable: {exc}"

        if name == "switch_model":
            return self._switch_model(args)

        if name == "software_update":
            return self._software_update(args)

        if name == "restore_settings":
            if self.onroad:
                return False, "refused: device is onroad"
            applied = sum(1 for k, v in (args.get("settings") or {}).items() if self.apply_setting(k, v)[0])
            return True, f"restored {applied} settings"

        return False, f"unknown command: {name}"

    def _switch_model(self, args: dict) -> tuple[bool, str]:
        # Real activation via SunnyPilot's Model Manager: set ModelManager_DownloadIndex; the
        # models_manager process downloads + SHA256-verifies the bundle, then sets the active
        # bundle. Reverting to stock removes ModelManager_ActiveBundle. Offroad-only.
        if self.onroad:
            return False, "refused: device is onroad"
        if self._params is None:
            return False, "params unavailable"
        ref = args.get("model_key")
        if not ref:
            return False, "no model_key"
        if str(ref).lower() in ("default", "stock", "default-stock"):
            try:
                self._params.remove("ModelManager_ActiveBundle")
            except Exception as exc:  # noqa: BLE001
                return False, f"failed: {exc}"
            self._invalidate("active_model", "available_models")  # reflect the change next heartbeat
            return True, "reverting to stock model"
        bundles = self._bundles(self._read_json_param("ModelManager_ModelsCache"))
        match = next(
            (
                b for b in bundles
                if ref in (
                    b.get("ref"), b.get("display_name"), b.get("displayName"),
                    b.get("short_name"), str(b.get("index")),
                )
            ),
            None,
        )
        if match is None or "index" not in match:
            return False, f"model '{ref}' not found in the on-device catalog"
        try:
            self._params.put("ModelManager_DownloadIndex", int(match["index"]))
        except Exception as exc:  # noqa: BLE001
            return False, f"failed to request model: {exc}"
        self._invalidate("active_model", "available_models")  # reflect the change next heartbeat
        return True, f"requested {self._bundle_name(match) or ref} — Model Manager will download + verify"

    def _software_update(self, args: dict) -> tuple[bool, str]:
        # Real update via SunnyPilot's updater: set the target branch and nudge `updated` to
        # fetch it. The device shows "Update available" and applies on the next reboot (offroad).
        if self.onroad:
            return False, "refused: device is onroad"
        if self._params is None:
            return False, "params unavailable"
        branch = args.get("branch") or self._branch_from_url(args.get("install_url"))
        if not branch:
            return False, "no target branch"
        try:
            self._params.put("UpdaterTargetBranch", branch)
        except Exception as exc:  # noqa: BLE001
            return False, f"failed to set target branch: {exc}"
        # Nudge the updater: SIGUSR1 = check, SIGHUP = download/fetch the target branch.
        for sig in ("-SIGUSR1", "-SIGHUP"):
            subprocess.run(["pkill", sig, "-f", "system.updated.updated"], check=False)
        self.update_channel = args.get("channel") or branch
        return True, f"update to {branch} started; device will fetch it and apply on next reboot"

    @staticmethod
    def _branch_from_url(url: str | None) -> str | None:
        if not url:
            return None
        return url.rstrip("/").split("/")[-1] or None

    # --- uploads ------------------------------------------------------------------------------
    def generate_artifacts(self) -> tuple[list, list]:
        # Route/log upload is OFF by default (privacy). Wire to the loggerd realdata root on opt-in.
        return [], []

    async def run_state_cycler(self) -> None:
        return None

    # [MyPilot private] route file-backed settings to /data/mypilot/config.json (prebuilt-safe:
    # these keys aren't declared Params, so put_bool would raise UnknownKeyName).
    _MYPILOT_BOOL_KEYS = ("mypilot_DisableDMNudges", "mypilot_DisableBelowSteerSpeedAlert")
    # Camera-upload keys are PHYSICAL-CONSENT gated: a remote (web) write may turn one OFF or
    # move it between ON positions, but may NEVER arm one from off. Only the on-device toggle
    # can move a camera off -> on. Enforced HERE (the single authoritative writer of the remote
    # path) against the LIVE config.json read in this same call, so a compromised web/stack that
    # replays a SET_SETTING ...=on is refused at the device regardless of any cached state.
    _MYPILOT_CAMERA_KEYS = ("drive_upload", "cabin_upload")
    _MYPILOT_DRIVE_UPLOAD_MODES = ("off", "qcamera", "full")
    @staticmethod
    def _mypilot_is_off(key, val):
        # 'off' state per key: drive_upload -> the string 'off'; cabin_upload -> falsey.
        if key == "drive_upload":
            return val in (None, "off")
        return not bool(val) if not isinstance(val, str) else val.strip().lower() not in ("1", "true", "yes", "on")
    def _mypilot_write_config(self, key, value) -> tuple[bool, str]:
        # Coerce the safety toggles to a real bool: the device readers do bool(value), so a
        # stray string like "false" would be truthy and latch DM-disable ON. Lock it to bool.
        if key in self._MYPILOT_BOOL_KEYS:
            value = bool(value) if not isinstance(value, str) else value.strip().lower() in ("1", "true", "yes", "on")
        if key == "cabin_upload":
            value = bool(value) if not isinstance(value, str) else value.strip().lower() in ("1", "true", "yes", "on")
        path = "/data/mypilot/config.json"
        try:
            try:
                with open(path) as fh:
                    cfg = json.load(fh) or {}
            except Exception:
                cfg = {}
            # Validate + physical-consent gate for the camera keys (against LIVE config.json).
            if key in self._MYPILOT_CAMERA_KEYS:
                if key == "drive_upload" and value not in self._MYPILOT_DRIVE_UPLOAD_MODES:
                    return False, "refused: invalid drive_upload value"
                cur = cfg.get(key, "off" if key == "drive_upload" else False)
                if self._mypilot_is_off(key, cur) and not self._mypilot_is_off(key, value):
                    return False, "refused: enable this camera on the device first"
            cfg[key] = value
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(cfg, fh)
            os.replace(tmp, path)
            return True, "applied"
        except Exception as exc:  # noqa: BLE001
            return False, f"config write failed: {exc}"
