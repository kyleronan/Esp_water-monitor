# Pinouts

This page summarizes the important board connector pinouts.

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

## Pressure sensor connectors

| Connector | Pin 1 | Pin 2 | Pin 3 |
| --- | --- | --- | --- |
| `Pressure1` | Red / + supply / `/5V` | Black / GND / `Earth` | Green / sense, through divider to `GPIO1` |
| `Pressure2` | Red / + supply / `/5V` | Black / GND / `Earth` | Green / sense, through divider to `GPIO2` |

## Pressure sense scaling

| Sensor | Sense pin | Series resistor | ESP32 ADC net | Pulldown resistor |
| --- | --- | --- | --- | --- |
| Pressure 1 | `Pressure1` pin 3 | `R10` 10K | `GPIO1` | `R9` 20K to ground |
| Pressure 2 | `Pressure2` pin 3 | `R8` 10K | `GPIO2` | `R7` 20K to ground |
