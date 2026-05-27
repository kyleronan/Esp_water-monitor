# Firmware — ESP Water Monitor

ESPHome firmware for the ESP32 water shut-off controller. 

## First-time setup

Before compiling you must create a `secrets.yaml` file in this directory:

```bash
cp firmware/secrets.yaml.example firmware/secrets.yaml
```

Then edit `firmware/secrets.yaml` and fill in all values. ESPHome will refuse to
compile if any `!secret` key is missing.

**Do not commit `secrets.yaml`** — it is listed in `.gitignore`.

### Generating an API encryption key

The `api_encryption_key` must be a base64-encoded 32-byte random value:

```bash
python3 -c "import base64, os; print(base64.b64encode(os.urandom(32)).decode())"
```

Paste the output into `secrets.yaml` as the value for `api_encryption_key`.

## Security notes

| Feature | Config |
|---|---|
| ESPHome API | Encrypted — key in `secrets.yaml` |
| OTA updates | Password-protected — set in `secrets.yaml` |
| Fallback hotspot | Password-protected — set in `secrets.yaml` |
| Built-in web UI | Basic auth — credentials in `secrets.yaml` |
| Bluetooth provisioning | `authorizer: none` — any nearby device can provision via Improv. Disable `esp32_improv` in the YAML if this is a concern. |

## Per-circuit pressure calibration

Both pressure transducers default to a shared factory-typical linear
fit. After bench measurement, override the per-circuit calibration via
the `substitutions:` block at the top of
`esp-water-shut-off-3_10.yaml` — eight values
(`pressure_cal_main_zero_raw / _psi`, `pressure_cal_main_max_raw /
_psi`, plus the irrigation counterparts). See
[`docs/hardware/bring-up-checklist.md`](../docs/hardware/bring-up-checklist.md)
for the two-point measurement procedure.

The defaults work for un-calibrated installs (existing flashes keep
their behaviour unchanged), but per-circuit calibration removes
transducer-to-transducer bias and makes the micro leak test more
sensitive.

## Releasing

Before tagging a firmware release, run the release-check script:

```bash
python scripts/check_firmware_release.py
```

It fails (non-zero exit) when any of these are missing or wrong in
`firmware/esp-water-shut-off-3_10.yaml`:

- `api.encryption.key`
- `ota[*].password`
- `wifi.ap.password`
- `web_server.auth.username` / `web_server.auth.password`
- `dashboard_import.package_import_url` not pinned to an immutable tag
  (`@v3.10.0`, etc.) or commit SHA — `@main` is rejected

When bumping the firmware version, update `dashboard_import.package_import_url`
to the new tag at the same time as the version field. The check script
parses the YAML, so commented-out example lines don't fool it.
