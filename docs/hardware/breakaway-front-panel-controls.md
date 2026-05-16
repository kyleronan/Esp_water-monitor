# Breakaway Front-Panel Controls

The bottom section of the PCB is designed as a breakaway section for optional front-panel manual control.

The main PCB stays mounted inside the enclosure. The breakaway switch section can be mounted on the enclosure front panel near the panel LEDs.

## Switch behavior

### Manual / Auto switch

| Position | Behavior |
| --- | --- |
| Auto | Software control. The main PCB and ESP32 control the valve. |
| Manual | Front-panel open/close switch control. ESP32 software control is bypassed. |

### Open / Close switch

| Position | Behavior |
| --- | --- |
| Open | Motor is powered toward the open position. |
| Close | Motor is powered toward the closed position. |
| Center/off | Not applicable. This switch does not have a center/off position. |

## Wiring path

The breakaway switch board is inserted inline with the valve motor wires.

```text
Main PCB R/B motor output
  -> breakaway board "From ESP" R/B
  -> manual/auto switching section
  -> breakaway board "To Valve" R/B
  -> valve motor R/B
```

## Offline behavior

Manual mode can still control the valve if the ESP32 is offline, as long as the main board and enclosure still have power for the switches, motor, and LEDs.

The ESP32 does not read the optional switch state in this hardware bypass path.

## Terminal labels

| Breakaway terminal label | Purpose |
| --- | --- |
| `From ESP` | Receives the main PCB `R` and `B` motor output lines. |
| `To Valve` | Connects to the valve motor `R` and `B` wires. |
