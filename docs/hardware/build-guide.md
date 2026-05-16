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

## Key documents

- `bom.md`
- `pinouts.md`
- `valve-wiring.md`
- `sensor-wiring.md`
- `panel-led-wiring.md`
- `breakaway-front-panel-controls.md`
- `enclosure-wiring.md`
- `bring-up-checklist.md`

## Build photos

The build photo gallery is in:

```text
docs/hardware/build-photos.md
```

The sanitized images are in:

```text
docs/hardware/images/build/
```

Only sanitized image copies should be committed to the public repo.
