# Valve Wiring

The build uses CR5 01 / CR501 12 V DC motorized ball valves.

## Connectors

| Connector | Channel | Use |
| --- | --- | --- |
| `J11` | Valve 1 | Motor and feedback wiring |
| `J13` | Valve 2 | Motor and feedback wiring |

## Wire functions

| PCB label | Valve wire color | Function |
| --- | --- | --- |
| `R` | Red | Motor lead |
| `B` | Black | Motor lead |
| `G` | Green | Fully-open feedback |
| `Y` | Yellow | Fully-closed feedback |
| `W` | White | Feedback common |

## Motor control

| Command | Black wire / `B` | Red wire / `R` | Result |
| --- | --- | --- | --- |
| Open | +12 V DC | GND / - | Valve opens |
| Close | GND / - | +12 V DC | Valve closes |
| Idle | Unpowered | Unpowered | Motor off |

The manufacturer note says the internal limit switch cuts motor power automatically when the valve reaches fully open or fully closed.

## Feedback contacts

| Valve position | Contact closure |
| --- | --- |
| Fully open | White / `W` to Green / `G` |
| Fully closed | White / `W` to Yellow / `Y` |

The feedback wires are connected to the PCB.

## Optional breakaway control board

If the breakaway front-panel control board is installed, the valve motor `R` and `B` lines pass through it:

```text
Main PCB R/B
  -> breakaway board "From ESP" R/B
  -> manual/auto switch path
  -> breakaway board "To Valve" R/B
  -> valve motor R/B
```

In Auto mode, main PCB software control passes through. In Manual mode, the front-panel open/close switch bypasses ESP32 software control and drives the motor from the 12 V source.
