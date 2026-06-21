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

## Pressure-sensor calibration (per circuit)

The firmware ships with a factory-typical two-point linear fit shared
across both circuits. Real transducers drift up to ±3–5 % from that
fit; calibrating per-circuit eliminates the bias and makes the leak
test more sensitive.

Each circuit has four substitutions in
`firmware/esp-water-shut-off-3_10.yaml` (search for "PRESSURE
TRANSDUCER CALIBRATION"). Override them after bench measurement:

```yaml
substitutions:
  pressure_cal_main_zero_raw: "0.05"     # raw ADC at zero PSI
  pressure_cal_main_zero_psi: "0.0"      # calibrated PSI at zero_raw
  pressure_cal_main_max_raw:  "3.18"     # raw ADC at reference pressure
  pressure_cal_main_max_psi:  "100.0"    # gauge reading at max_raw
  # same four for the irrigation circuit (pressure_cal_irr_*)
```

Two-point procedure for one circuit:

- [ ] Disconnect (or ground) the transducer signal pin. Read the
      device's raw ADC voltage from Home Assistant — record as
      `zero_raw`. Record the corresponding PSI as `zero_psi`
      (typically a small negative number for a 0.5–4.5 V transducer
      whose lowest output doesn't quite hit 0 PSI).
- [ ] Connect a known reference: a hand pump with an analogue gauge
      teed into the line is the usual approach. Apply ~80–100 PSI
      (well above any typical household working pressure but inside
      the transducer's range). Read the raw ADC voltage — record as
      `max_raw`. Record the gauge's PSI as `max_psi`.
- [ ] Edit the four substitutions for that circuit. Re-flash. Confirm
      the live pressure reading now matches the gauge across the
      range.
- [ ] Repeat for the other circuit.

Cross-check: with both valves open and water static (no demand), the
two circuits should read within ~1–2 PSI of each other. If they
differ wildly, one transducer may be miswired, swapped, or faulty —
re-check before committing the calibration.

## Enclosure test

- [ ] Confirm panel LEDs indicate open and closed status.
- [ ] Confirm cable glands provide strain relief.
- [ ] Confirm wires are routed away from sharp edges.
- [ ] Confirm the front panel can open without pulling on wiring.
