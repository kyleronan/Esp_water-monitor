# Hardware Documentation

This folder documents the ESP32 Water Valve Controller hardware build.

Use the board labels from the PCB and KiCad files throughout the hardware docs:

- Valve 1
- Valve 2

Software may allow custom names later, but the hardware build guide uses the physical board labels.

## Main documents

- `build-guide.md` - build overview and recommended assembly flow
- `bom.md` - simplified builder-facing BOM
- `field-hardware.md` - enclosure, sensors, valves, and external hardware
- `pinouts.md` - board connector pinouts
- `valve-wiring.md` - CR501 valve motor and feedback wiring
- `sensor-wiring.md` - flow and pressure sensor wiring
- `panel-led-wiring.md` - external panel LED wiring
- `breakaway-front-panel-controls.md` - optional manual/auto front-panel control board
- `enclosure-wiring.md` - enclosure wiring notes and DIN terminal blocks
- `manufacturing.md` - KiCad/JLCPCB production file notes
- `bring-up-checklist.md` - first power-up and test checklist
- `missing-info-checklist.md` - remaining information to collect

## Images

Sanitized build photos are in:

```text
docs/hardware/images/build/
```

The photo gallery is:

```text
docs/hardware/build-photos.md
```

Only the sanitized copies should be committed. The sanitized copies have EXIF and GPS metadata stripped.
