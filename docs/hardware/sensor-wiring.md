# Sensor Wiring

## Flow sensor wiring

The selected flow sensor is rated for 5 V operation and works down to 3.3 V. In this design, the board powers the flow sensors from 3.3 V.

| Wire color | Function |
| --- | --- |
| Red | + supply |
| Black | Ground |
| Yellow | Pulse output |

| Connector | Pin 1 | Pin 2 | Pin 3 |
| --- | --- | --- | --- |
| `Flow1` | Red / `/3v3` | Yellow / `GPIO39` | Black / `Earth` |
| `Flow2` | Red / `/3v3` | Yellow / `GPIO38` | Black / `Earth` |

Because the flow sensor is powered from 3.3 V, the yellow pulse output can connect directly to the ESP32 GPIO input without a voltage divider or other signal conditioning.

## Pressure sensor wiring

| Wire color | Function |
| --- | --- |
| Red | + supply |
| Black | Ground |
| Green | Analog sense output |

| Connector | Pin 1 | Pin 2 | Pin 3 |
| --- | --- | --- | --- |
| `Pressure1` | Red / `/5V` | Black / `Earth` | Green / sense to `GPIO1` through divider |
| `Pressure2` | Red / `/5V` | Black / `Earth` | Green / sense to `GPIO2` through divider |

The pressure sensor output is 0.5 to 4.5 V. The PCB resistor divider scales the signal for ESP32 ADC input.

## Remaining calibration details

- Flow sensor K-factor or pulses per liter/gallon
- Pressure sensor scale and offset used in firmware or setup
