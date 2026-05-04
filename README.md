# Water Monitor

Intelligent water monitoring for Home Assistant. ESP32-based hardware with a
companion HA addon that learns your home's water usage patterns, identifies
fixtures, runs scheduled leak tests, and alerts on anomalies.

## Repository structure

This is a Home Assistant addon repository. It also contains the firmware,
documentation, and (eventually) a companion native HA integration:

```
.
├── repository.yaml      # HA addon repository metadata
├── water_monitor/       # The addon itself (HA expects this at the root)
│   ├── config.yaml
│   ├── Dockerfile
│   ├── app/
│   ├── README.md
│   └── CHANGELOG.md
├── firmware/            # ESPHome firmware for the ESP32-S3-WROOM-1
├── integration/         # Native HA integration (planned for v2.4)
└── docs/                # Hardware build notes, pinouts, troubleshooting
```

## Installation

### As a Home Assistant addon

1. In Home Assistant: **Settings → Add-ons → Add-on Store**
2. Open the three-dot menu (top right) → **Repositories**
3. Paste this repository's URL:
   ```
   https://github.com/kyleronan/Esp_water-monitor
   ```
4. Click **Add**, then close the dialog
5. The **Water Monitor** addon will appear in the store — click it and **Install**
6. Open the addon's web UI and follow the setup wizard

Updates are delivered automatically — when a new version is pushed to this
repository, the HA addon store will offer the update.

### Firmware

1. Flash `firmware/esp-water-shut-off-3_4.yaml` to your ESP32-S3-WROOM-1
   using ESPHome
2. Make sure the device is added to Home Assistant before installing the addon
3. The setup wizard will discover the device automatically

See `docs/` for hardware build notes and pinout reference.

## Project status

| Phase | Scope | Status |
|---|---|---|
| **0.1.x** | Core monitoring, leak detection, calibration, display units | Shipped |
| **0.2.x** | Fixture identification (clustering + matching) | In development |
| **0.3.x+** | Anomaly detection, native HA integration | Planned |

See `water_monitor/CHANGELOG.md` for detailed release notes.

## License

MIT — see `LICENSE` file.

## Contributing

This repository is currently in active development; the API and database
schema may change between minor versions. Issues and pull requests are
welcome once the project goes public.
