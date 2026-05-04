# Water Monitor Add-on

Intelligent water monitoring for Home Assistant. Learns your home's normal water usage
patterns, detects anomalies, and runs scheduled micro leak tests. Supports dual-circuit
installations (main + irrigation) with motorised ball valves.

## Features

- **Real-time dashboard** — Live pressure, flow rate, daily/weekly volume, and valve
  control for each circuit. All values display in your chosen units.
- **Configurable display units** — Flow (L/min · gal/min · ft³/min · m³/min) and
  pressure (PSI · bar · kPa) auto-detected from your HA unit system. The setup wizard
  asks you to confirm or change these before finishing; override at any time in Settings.
- **Fixture signature recognition** — Identifies individual fixtures from flow and
  pressure patterns (Phase 2 — active development).
- **Anomaly detection** — Flags events that don't match any known fixture signature
  with configurable alert thresholds.
- **Scheduled micro leak tests** — Closes each valve, monitors pressure decay, and
  notifies on failure. Automatically scheduled at the statistically quietest hour of day.
- **Historical event import** — Reconstructs events from HA recorder history on startup
  and every 30 minutes, filling gaps caused by restarts or the initial install period.
- **Away mode** — Pauses baseline learning while unoccupied; calibration timer is extended
  by the exact away duration so the learning period only counts home time.
- **HA presence linking** — Automatically toggles away mode from `person.*`,
  `device_tracker.*`, `input_boolean.*`, or `alarm_control_panel.*` entities.
- **Mobile push notifications** — All alerts sent to HA sidebar and configured
  `notify.mobile_app_*` services.
- **Full backup and restore** — Three-tier backup system (Quick Restore JSON,
  History Archive SQLite, Full ZIP) with setup wizard restore support.

## Hardware

Designed for the **ESP32-S3-WROOM-1** running ESPHome.

**Device:** `esp-water-shut-off-3` · **Friendly name:** `ESP Water Shut off 3.4`
**Firmware:** `firmware/esp-water-shut-off-3_4.yaml`

### Pin assignment

| Signal | GPIO | Notes |
|---|---|---|
| Pressure Main (ADC) | GPIO01 | Right pin 39 |
| Pressure Irrigation (ADC) | GPIO02 | Right pin 38 |
| Flow Rate Main (pulse) | GPIO39 | Right pin 32 |
| Flow Rate Irrigation (pulse) | GPIO38 | Right pin 31 |
| Endstop open Main | GPIO16 | Left pin 9 |
| Endstop close Main | GPIO15 | Left pin 8 |
| Endstop open Irrigation | GPIO18 | Left pin 11 |
| Endstop close Irrigation | GPIO17 | Left pin 10 |
| LED open valve 1 | GPIO07 | Left pin 7 |
| LED close valve 1 | GPIO04 | Left pin 4 |
| LED open valve 2 | GPIO06 | Left pin 6 |
| LED close valve 2 | GPIO05 | Left pin 5 |
| Relay open Main | GPIO48 | Right pad 25 |
| Relay close Main | GPIO47 | Right pad 24 |
| Relay open Irrigation | GPIO14 | Bottom pin 22 |
| Relay close Irrigation | GPIO21 | Bottom pin 23 |
| USB D− | GPIO19 | Left pin 13 |
| USB D+ | GPIO20 | Left pin 14 |
| SPI CS | GPIO10–11 | Bottom pins 18–19 |
| SPI CLK | GPIO12 | Bottom pin 20 |
| SPI MISO | GPIO13 | Bottom pin 21 |
| J14 pin 1 | GPIO40 | Right pin 33 |
| J14 pin 2 | GPIO41 | Right pin 34 |
| J14 pin 3 | GPIO42 | Right pin 35 |

> GPIO35–37 are connected to internal PSRAM and unavailable for general use.
> GPIO26, GPIO33, and GPIO34 are not exposed on the WROOM-1 module.

### Pressure sensors

| Entity | Rate | Smoothing | HA recorded | Use |
|---|---|---|---|---|
| `pressure_main` | 2 Hz | 1.375 s | Yes (0.1.2+) | Live display + historical import |
| `pressure_main_avg` | 1 Hz | 25 s | Yes | Long-term trend display |
| `pressure_main_fast` | 40 Hz | 50 ms | No (diagnostic) | Live event detection |

## Installation

1. Add this repository to your Home Assistant add-on store.
2. Install the **Water Monitor** add-on.
3. Flash `firmware/esp-water-shut-off-3_4.yaml` to your ESP32-S3-WROOM-1.
4. Open the add-on web UI and follow the six-step setup wizard:

   | Step | Screen | Description |
   |---|---|---|
   | 1 | Find Device | Searches HA for your ESP device by name |
   | 2 | Select | Picks from matching devices |
   | 3 | Entities | Confirms the entity-to-role mapping |
   | 4 | Units | Choose flow and pressure display units (auto-detected from HA) |
   | 5 | Your Home | Bathrooms, floors, occupants, water supply type |
   | 6 | Done | Calibration starts automatically |

To restore from a previous installation, choose **Restore from backup** on the first
screen of the setup wizard and upload your `quick_restore.json` file.

## Data Storage

All data is stored in `/data/water_monitor.db` (SQLite, WAL mode). The database is
preserved across add-on restarts and updates. Schema migrations run automatically at
startup and are idempotent.

| Table | Contents |
|---|---|
| `events` | Raw event records with features and fixture labels |
| `hourly_volume` | Per-hour flow totals for chart display |
| `daily_summary` | Pre-aggregated daily stats (computed nightly) |
| `training_state` | Calibration progress per circuit |
| `volume_snapshots` | HA cumulative sensor readings at period start (for daily/weekly totals) |
| `import_state` | Historical importer last-check timestamps |
| `alert_config` | Per-circuit alert enable/disable flags |
| `home_profile` | Home details, away mode state, display unit preferences |

## Version

See [CHANGELOG.md](CHANGELOG.md) for full release history.

Current: **0.1.2**
