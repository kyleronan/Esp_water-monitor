# Bring-Up Checklist

Use this checklist before connecting the system to permanent plumbing.

## Before power

- [ ] Inspect PCB for solder bridges.
- [ ] Check orientation of ESP32 module, USB-C connector, diodes, LEDs, relays, and regulators.
- [ ] Verify terminal blocks are installed correctly.
- [ ] Verify valve wiring matches `pinouts.md`.
- [ ] Verify panel LED wiring matches `breakaway-front-panel-controls.md`.
- [ ] Verify sensor wiring matches `pinouts.md`.

## First power

- [ ] Apply 12 V power.
- [ ] Measure the 12 V rail.
- [ ] Measure the 5 V rail.
- [ ] Measure the 3.3 V rail.
- [ ] Confirm ESP32 boots.
- [ ] Confirm no regulator or relay driver overheats.

## Valve test

- [ ] Test Valve 1 open command.
- [ ] Test Valve 1 close command.
- [ ] Confirm Valve 1 feedback changes at full open and full closed.
- [ ] Test Valve 2 open command.
- [ ] Test Valve 2 close command.
- [ ] Confirm Valve 2 feedback changes at full open and full closed.
- [ ] Test manual mode if the breakaway control board is installed.

## Sensor test

- [ ] Confirm Flow 1 pulses are detected.
- [ ] Confirm Flow 2 pulses are detected.
- [ ] Confirm Pressure 1 reading changes with pressure.
- [ ] Confirm Pressure 2 reading changes with pressure.

## Enclosure test

- [ ] Confirm panel LEDs indicate open and closed status.
- [ ] Confirm cable glands provide strain relief.
- [ ] Confirm wires are routed away from sharp edges.
- [ ] Confirm the front panel can open without pulling on wiring.
