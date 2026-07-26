# Pinouts

This page summarizes the important board connector pinouts.

## ESP32-S3 GPIO assignments

| Function | Valve 1 (Main) | Valve 2 (Irrigation) |
| --- | --- | --- |
| Relay coil — drive OPEN | `GPIO48` | `GPIO21` |
| Relay coil — drive CLOSE | `GPIO47` | `GPIO14` |
| End stop — fully open | `GPIO16` | `GPIO18` |
| End stop — fully closed | `GPIO15` | `GPIO17` |
| Panel LED — open | `GPIO07` | `GPIO06` |
| Panel LED — closed | `GPIO04` | `GPIO05` |
| Pressure sense (ADC) | `GPIO1` | `GPIO2` |
| Flow pulse | `GPIO39` | `GPIO38` |

> **Corrected 2026-07-26:** the irrigation relay coil pins are `GPIO21` = open,
> `GPIO14` = close. Firmware before 3.13.1 had them swapped.

### Motor drive model (firmware 3.13.1+)

The motor moves only **while** its relay coil GPIO is held high; the motor cuts
its own power at end of travel. The firmware holds the coil until the target
end stop fires (90 s timeout), then releases. Coil pulses do not move the
valve on this board.

### End-stop signal conditioning

The end-stop feedback lines are **active-high**: the valve's feedback contact
feeds the line when closed, and it passes through a 10 K series / 20 K
pulldown divider (same topology as the pressure inputs) before reaching the
GPIO — idle reads ~0 V, pressed reads ~3.3 V at the GPIO. The firmware
configures these inputs with **no internal pull** (3.13.1): enabling the
internal pullup fights the onboard 20 K pulldown into a ~1.3–1.4 V divider,
which sits in the ESP32-S3's undefined logic band and makes reads unreliable.

## Valve connectors

| Connector | Channel | Use |
| --- | --- | --- |
| `J11` | Valve 1 | CR501 valve motor and feedback wires |
| `J13` | Valve 2 | CR501 valve motor and feedback wires |

| PCB label | Valve wire | Function |
| --- | --- | --- |
| `R` | Red | Motor lead |
| `B` | Black | Motor lead |
| `G` | Green | Fully-open feedback |
| `Y` | Yellow | Fully-closed feedback |
| `W` | White | Feedback common |

### Valve Wiring

The build uses CR5 01 / CR501 12 V DC motorized ball valves.


### Motor control

| Command | Black wire / `B` | Red wire / `R` | Result |
| --- | --- | --- | --- |
| Open | +12 V DC | GND / - | Valve opens |
| Close | GND / - | +12 V DC | Valve closes |
| Idle | Unpowered | Unpowered | Motor off |

The manufacturer note says the internal limit switch cuts motor power automatically when the valve reaches fully open or fully closed.

### Feedback contacts

| Valve position | Contact closure |
| --- | --- |
| Fully open | White / `W` to Green / `G` |
| Fully closed | White / `W` to Yellow / `Y` |

The feedback wires are connected to the PCB.

### Optional breakaway control board

If the breakaway front-panel control board is installed, the valve motor `R` and `B` lines pass through it:

```text
Main PCB R/B
  -> breakaway board "From ESP" R/B
  -> manual/auto switch path
  -> breakaway board "To Valve" R/B
  -> valve motor R/B
```

In Auto mode, main PCB software control passes through. In Manual mode, the front-panel open/close switch bypasses ESP32 software control and drives the motor from the 12 V source.


## Panel LED connectors

| Connector | Channel | Use |
| --- | --- | --- |
| `J10` | Valve 1 | External panel LEDs |
| `J12` | Valve 2 | External panel LEDs |

For both `J10` and `J12`:

| LED | Terminals | Meaning |
| --- | --- | --- |
| Green panel LED | `Open +` / `Open -` | Valve open indicator |
| Red panel LED | `Close +` / `Close -` | Valve closed indicator |

## Flow sensor connectors

| Connector | Pin 1 | Pin 2 | Pin 3 |
| --- | --- | --- | --- |
| `Flow1` | Red / + supply / `/3v3` | Yellow / pulse / `GPIO39` | Black / GND / `Earth` |
| `Flow2` | Red / + supply / `/3v3` | Yellow / pulse / `GPIO38` | Black / GND / `Earth` |

Because the flow sensor is powered from 3.3 V, the yellow pulse output can connect directly to the ESP32 GPIO input without a voltage divider or other signal conditioning.

## Pressure sensor connectors

| Connector | Pin 1 | Pin 2 | Pin 3 |
| --- | --- | --- | --- |
| `Pressure1` | Red / + supply / `/5V` | Black / GND / `Earth` | Green / sense, through divider to `GPIO1` |
| `Pressure2` | Red / + supply / `/5V` | Black / GND / `Earth` | Green / sense, through divider to `GPIO2` |

The pressure sensor output is 0.5 to 4.5 V. The PCB resistor divider scales the signal for ESP32 ADC input.

## Pressure sense scaling

| Sensor | Sense pin | Series resistor | ESP32 ADC net | Pulldown resistor |
| --- | --- | --- | --- | --- |
| Pressure 1 | `Pressure1` pin 3 | `R10` 10K | `GPIO1` | `R9` 20K to ground |
| Pressure 2 | `Pressure2` pin 3 | `R8` 10K | `GPIO2` | `R7` 20K to ground |
