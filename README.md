# Water Monitor

A DIY Home Assistant addon and ESP32-based smart water shut-off valve.
Tracks pressure and flow, learns your home's normal usage, runs scheduled leak
tests, and lets you cut off water to the house from anywhere — without
subscriptions, cloud polling, or trips to the crawl space.

## Background

This started as a hobby project. I wasn't happy with the commercial options
(Moen Flo, StreamLabs Control) — they polled the internet for things that
should work locally, and put basic features behind subscriptions. I also got
tired of crawling under the house every winter to shut off water to the
irrigation, and wanted something I could trigger from an app or a wall switch.

So I built one. It's not as pretty or compact as the off-the-shelf products,
but it has most of the same basic features and runs entirely on my own LAN.

A few design choices worth knowing about up front:

- **Manual override matters.** From day one this had to work with physical
  switches and indicator lights, not just the web UI. If the network is down,
  if I'm not home, if Home Assistant is misbehaving — anyone in the house
  can still open or close the valve.
- **Two circuits.** Main (whole house) plus irrigation. The irrigation valve
  is the one I shut off for winter; having it on a separate circuit makes
  the seasonal routine a one-tap operation.
- **Overbuilt by choice.** Three-piece stainless steel ball valves (serviceable,
  lead-free, more trustworthy than the brass valves on AliExpress), motorised
  actuators with a manual override for power outages, separate pressure
  transducers and flow meters per circuit. You could absolutely build this
  with cheaper parts — most of the firmware doesn't care.
- **Cost-conscious sourcing.** Most components came from AliExpress because the
  same parts from local plumbing/HVAC suppliers were 10× the price. Links to
  what I actually used will go in `docs/` once the hardware build guide is
  written.
- **PCB caveat.** This was my first SMD design. The current revision works,
  but I have a small pile of boards on my shelf that don't. The board is
  optimised for *my* situation, not for manufacturing efficiency or BOM cost.

## On the code

I should be upfront about how this got built. I'm an amateur C coder at best,
and my prior web experience tops out at script-kiddie HTML and Java tinkering.
The firmware was within reach — I had a basically functional firmware and the
valve sitting in the basement "ready" for testing since August 2025 — but the
prospect of writing the Home Assistant addon, with its FastAPI backend, async
WebSockets, Jinja2 templates, SQLite migrations, and clustering algorithms, kept
stalling out against my ADHD. It sat on the shelf for a long time.

So I leaned on AI heavily and did a fair amount of vibe coding to get the
addon to a state where it's actually usable. Originally I figured this whole
project was just for me, but along the way I realised I should at least flesh
it out enough that someone else could build on the idea if they wanted to.
That's why the addon code is more polished than I'd ever have produced on my
own — and also why it might have rough edges only an experienced developer
would have caught.

If you find bugs or design choices that look weird, that's probably why. PRs
welcome.

## TL;DR

I made a thing. It mostly works. If you want to copy it or improve on it,
that's great. I'm probably not going to build another one for myself.

## What it does

- **Real-time monitoring** — pressure and flow on the main and irrigation
  circuits, displayed in the units you choose (L/min · gal/min · ft³/min ·
  m³/min, and PSI · bar · kPa)
- **Learned baseline** — 14-day calibration period during which the addon
  watches your normal usage patterns. When training ends, the Fixtures
  page lists the clusters that were detected; you confirm or remove them,
  then activate the circuit to start anomaly detection (or auto-activates
  after 7 days of no review).
- **Scheduled micro leak tests** — closes each valve in turn at the
  statistically quietest hour of the day, monitors pressure decay, and notifies
  on failure
- **Fixture identification** — clusters water events by their pressure and
  flow signatures to identify individual fixtures (toilets, showers, taps,
  appliances). Live in v0.2.0; naming UI complete; merge-clusters UI and
  ESP-side waveform enrichment added in v0.2.1.
- **Event detail modal** — every event in the History list opens an
  inspector with trigger type, peak flow, propagation delay, anomaly score
  and (with firmware v3.7+) an embedded waveform chart. Events can be
  re-assigned to a fixture or ignored from this view.
- **Degraded-supply guard** (v0.2.2) — detects when the municipal supply
  is pulsing (paddlewheel flow sensors produce garbage under those
  conditions). Affected events are flagged, given an envelope-smoothed
  volume so daily totals stay sensible, and excluded from clustering. The
  Dashboard shows a banner and the History page has a filter.
- **Per-circuit valve type** (v0.2.2) — Setup and Settings let you mark
  each circuit as 2-port (standard) or 3-port (drain-capable for
  winterization). 3-port circuits automatically skip the micro leak test
  (the drain port reads as a constant leak); the schedule is preserved
  if you ever switch back.
- **Runtime flow-meter calibration** (v0.2.2) — pulses-per-litre is a
  per-circuit Home Assistant number entity (NVS-backed, survives OTA), so
  any flow meter works without a firmware edit. A guided **calibration
  helper** (Settings → Flow Meter → Calibrate) measures the true
  pulses-per-litre from a bucket test or a whole-house municipal-meter
  reading — showing each run, their average and range, with an editable,
  safety-gated value before you apply.
- **Anomaly detection** (v0.2.2, Phase 2.3) — flags events that deviate from
  the learned baseline (volume percentiles + per-fixture envelopes) and can
  notify or auto-shut-off, guarded so a thin/young baseline degrades to
  notify-only.
- **Away mode** — pauses learning while unoccupied, automatically toggled
  from your existing HA presence entities
- **Mobile push notifications** for all alerts via HA's `notify.mobile_app_*`
  services, with per-target backoff after repeated failures so a broken
  mobile_app integration doesn't spam the logs.
- **Full backup and restore** — three-tier backup (Quick Restore JSON, History
  Archive SQLite, Full ZIP) with restore-from-backup in the setup wizard
- **Auto dark mode** — the UI follows your OS / browser appearance
  preference automatically. No toggle, no setup.

## Repository structure

This is a Home Assistant addon repository. It also contains the firmware,
documentation, and (eventually) a companion native HA integration.

```
.
├── repository.yaml      # HA addon repository metadata
├── water_monitor/       # The addon itself (HA expects this at the root)
│   ├── config.yaml
│   ├── Dockerfile
│   ├── icon.png         # Addon icon (128 × 128) — shown on HA addon card
│   ├── logo.png         # Addon logo (250 × 100) — shown on HA store detail page
│   └── app/
├── firmware/            # ESPHome firmware for the ESP32-S3-WROOM-1
├── integration/         # Native HA integration (planned for v0.4)
└── docs/                # Hardware build notes, pinouts, parts list
```

## Installation

### Addon

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

### Hardware

You'll need the ESP32-S3-WROOM-1 board (custom PCB), motorised ball valves
with end-stop signalling, pressure transducers, and pulse-output flow meters.

> **If your home has a pressure boosting pump or well pump system**, install
> the addon's flow and pressure sensors *downstream* of the pump (between the
> pump output and the house plumbing). Sensors placed upstream see pump cycling
> artifacts that mask individual fixture pressure signatures, making the
> fixture identification feature in v0.2.x unreliable. Downstream placement is
> also what every reasonable installation diagram recommends regardless of
> monitoring.

PCB design files (KiCad + Gerbers), bill of materials, build guide, and bring-up checklist are in [`docs/hardware/`](docs/hardware/).

### Firmware

1. Copy `firmware/secrets.yaml.example` to `firmware/secrets.yaml` and fill
   in the five values (API encryption key, OTA password, fallback-AP
   password, web UI username + password). See
   [`firmware/README.md`](firmware/README.md) for the key-generation
   snippet and the security model.
2. Flash `firmware/esp-water-shut-off-3_12.yaml` (v3.12.0) to your
   ESP32-S3-WROOM-1 using ESPHome.
3. Make sure the device is added to Home Assistant before installing the
   addon.
4. The setup wizard will discover the device automatically.

Before tagging a firmware release, run
`python scripts/check_firmware_release.py` — it parses the YAML and
fails if any credential is missing or if `dashboard_import` still
points at a mutable branch ref.

## Project status

| Phase | Scope | Status |
|---|---|---|
| **0.1.x** | Core monitoring, leak detection, calibration, display units | Shipped |
| **0.2.0** | Fixture identification — clustering engine live, naming UI complete | Shipped |
| **0.2.1** | ESP-side waveform capture & feature enrichment, event detail modal, merge-clusters UI, Basic/Advanced settings split, auto dark mode, hardware docs (PCB v1.2a) | Shipped |
| **0.2.2** | Runtime per-circuit flow-meter PPL + guided flow-calibration helper (bucket / municipal), Phase 2.3 anomaly detection, degraded-supply guard, per-circuit valve type (2-port / 3-port), per-session CSRF refactor, autocorrelation correctness fix, firmware release-gate script, migration transaction safety, async/blocking SQLite audit, live-capture reliability fixes (importer catch-up checkpoint + stuck no-flow phantom close) | Shipped |
| **0.3.x** | Per-circuit pressure-sensor calibration, esp_idf framework migration with proper task watchdog | Planned |
| **0.4.x** | Native Home Assistant integration (alongside the addon, eventually replacing the WebSocket bridge for users who prefer pip-installable components) | Planned |

See `CHANGELOG.md` for detailed release notes, and
[`docs/TODO.md`](docs/TODO.md) for the running list of manual follow-ups
(real-sample capture for the degraded-supply detector, per-circuit
pressure calibration, etc).

## License

MIT — see `LICENSE` file. Use it, modify it, sell it, build a better one,
whatever you like. Attribution is appreciated but not required.

## Contributing

This repository is in active development; the API and database schema may
change between minor versions. If you build one and find bugs or have ideas,
issues and PRs are welcome — though responses may be slow given this is a
side project.
