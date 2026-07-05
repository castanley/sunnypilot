# [MyPilot private] config.json-backed toggle widgets (NOT Params — prebuilt-safe).
# The stock BigParamControl/BigMultiParamToggle bind to Params (get_bool/put_bool), which raise
# UnknownKeyName for our undeclared mypilot_*/drive_upload keys on a prebuilt branch. These read and
# write /data/mypilot/config.json directly (the same file the device readers + the MyPilot agent use).
import json
import os

from openpilot.selfdrive.ui.mici.widgets.button import BigToggle, BigMultiToggle

_CONFIG_PATH = "/data/mypilot/config.json"


def _read_config() -> dict:
  try:
    with open(_CONFIG_PATH) as fh:
      return json.load(fh) or {}
  except Exception:
    return {}


def _write_config(key, value) -> bool:
  cfg = _read_config()
  cfg[key] = value
  try:
    os.makedirs(os.path.dirname(_CONFIG_PATH), exist_ok=True)
    tmp = _CONFIG_PATH + ".tmp"
    with open(tmp, "w") as fh:
      json.dump(cfg, fh, indent=2)
    os.replace(tmp, _CONFIG_PATH)
    return True
  except Exception as e:
    # Don't crash the UI on a write failure, but surface it (so a stuck toggle is debuggable) and
    # let the caller revert the on-screen state to match what's actually persisted.
    print(f"[mypilot] config write failed for {key!r}: {e}")
    return False


class MyPilotConfigToggle(BigToggle):
  """On/off toggle persisted to a /data/mypilot/config.json key (bool)."""
  def __init__(self, text, config_key, toggle_callback=None):
    super().__init__(text, "", initial_state=bool(_read_config().get(config_key, False)),
                     toggle_callback=toggle_callback)
    self._config_key = config_key

  def _handle_mouse_release(self, mouse_pos):
    super()._handle_mouse_release(mouse_pos)
    # Revert the visual toggle if the write didn't persist, so the UI never lies about device state.
    if not _write_config(self._config_key, bool(self._checked)):
      self.set_checked(not self._checked)

  def refresh(self):
    self.set_checked(bool(_read_config().get(self._config_key, False)))


class MyPilotConfigMultiToggle(BigMultiToggle):
  """Cycle-through toggle persisted to a config.json key.
  `options` are the human-facing labels shown + cycled on the pill; `values` (parallel list,
  defaults to options) are what actually gets written to the config key. This lets us show
  "off/preview/full" while persisting the "off/qcamera/full" the agent expects."""
  def __init__(self, text, config_key, options, values=None, toggle_callback=None):
    super().__init__(text, options, toggle_callback=toggle_callback)
    self._config_key = config_key
    self._values = list(values) if values is not None else list(options)
    self.refresh()

  def _get_label_font_size(self):
    # Match the stock 3-way (BigMultiParamToggle "driving personality", 42pt). The default rule bumps
    # short labels to 48pt, which makes the label block taller and crowds the option pills/value.
    return 42

  def _stored_value(self):
    try:
      return self._values[self._options.index(self.value)]
    except ValueError:
      return self._values[0]

  def _handle_mouse_release(self, mouse_pos):
    super()._handle_mouse_release(mouse_pos)
    # On write failure, snap the displayed option back to what's persisted on disk.
    if not _write_config(self._config_key, self._stored_value()):
      self.refresh()

  def refresh(self):
    stored = _read_config().get(self._config_key, self._values[0])
    idx = self._values.index(stored) if stored in self._values else 0
    self.set_value(self._options[idx])
