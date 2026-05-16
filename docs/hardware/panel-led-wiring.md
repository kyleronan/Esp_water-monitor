# Panel LED Wiring

The build uses external 12 V DC quick-disconnect panel LEDs.

## Panel LED parts

| Qty | Part | Description |
| ---: | --- | --- |
| 2 | McMaster-Carr `2779K111` | Green 12 V DC panel LED, 5/16 inch cutout |
| 2 | McMaster-Carr `2779K113` | Red 12 V DC panel LED, 5/16 inch cutout |

## Connectors

| Connector | Channel | Use |
| --- | --- | --- |
| `J10` | Valve 1 | Panel LEDs |
| `J12` | Valve 2 | Panel LEDs |

## Pinout

For both `J10` and `J12`:

| LED | Terminals | Meaning |
| --- | --- | --- |
| Green panel LED | `Open +` / `Open -` | Valve open indicator |
| Red panel LED | `Close +` / `Close -` | Valve closed indicator |

## Wiring

- Green panel LED positive lead goes to `Open +`.
- Green panel LED negative lead goes to `Open -`.
- Red panel LED positive lead goes to `Close +`.
- Red panel LED negative lead goes to `Close -`.

The panel LEDs are mounted near the optional breakaway manual/auto control section on the front of the enclosure.
