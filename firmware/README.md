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

## Releasing

`dashboard_import.package_import_url` currently points to `@main`. Before any
public release, replace `@main` with an immutable version tag (e.g. `@v3.6.0`)
so adopters always get a known-good version. This is marked as a
`# RELEASE BLOCKER` comment in the YAML.
