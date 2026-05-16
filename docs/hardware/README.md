# Hardware Documentation

This folder documents the ESP32 Water Valve Controller hardware build.

Use the board labels from the PCB and KiCad files throughout the hardware docs:

- Valve 1
- Valve 2

Software may allow custom names later, but the hardware build guide uses the physical board labels.

## Main documents

- `bom.md` - simplified builder-facing BOM
- `pinouts.md` - board connector pinouts
- `valve-wiring.md` - CR501 valve motor and feedback wiring
- `sensor-wiring.md` - flow and pressure sensor wiring
- `panel-led-wiring.md` - external panel LED wiring
- `breakaway-front-panel-controls.md` - optional manual/auto front-panel control board
- `enclosure-wiring.md` - enclosure wiring notes and DIN terminal blocks
- `bring-up-checklist.md` - first power-up and test checklist


# Hardware Build Guide

This is a high-level build guide for the ESP32 Water Valve Controller hardware.

## Build order

1. Inspect the bare PCB.
2. Assemble the PCB.
3. Inspect solder joints and orientation-sensitive parts.
4. Separate the breakaway front-panel control section if it will be used.
5. Mount the main PCB in the enclosure.
6. Mount the breakaway control section and panel LEDs on the enclosure front panel.
7. Wire the 12 V DIN rail power supply and DIN terminal blocks.
8. Wire Valve 1 and Valve 2.
9. Wire the flow and pressure sensors.
10. Power up without plumbing pressure and run the bring-up checklist.
11. Test valve open/close behavior.
12. Test sensor readings.
13. Perform plumbing tests and calibration.


## Build photos

The build photo gallery is in:

```text
docs/hardware/build-photos.md
```


