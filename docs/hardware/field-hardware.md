# Field Hardware

This page documents non-PCB hardware used in the system build.

## Pressure sensor

- Supplier: Amazon
- ASIN: `B0CFJ7BN3L`
- Type: 100 PSI pressure transducer
- Thread: 1/8"-27 NPT
- Supply: 5 to 16 V DC
- Output: 0.5 to 4.5 V analog
- Material: 316 stainless steel
- Listed waterproof rating: IP65

### Pressure sensor wiring

| Wire color | Function |
| --- | --- |
| Red | Supply + |
| Black | Ground |
| Green | Analog sense output |

Pressure sensors are powered from 5 V. The sense output is scaled by a resistor divider before reaching the ESP32 ADC input.

## Flow sensor

- Supplier: AliExpress
- Item: `3256804293371927`
- Selected version: 5 V version
- Used from the PCB 3.3 V supply because the sensor works down to 3.3 V

### Flow sensor wiring

| Wire color | Function |
| --- | --- |
| Red | Supply + |
| Black | Ground |
| Yellow | Pulse output |

The flow sensor pulse output connects directly to the ESP32 GPIO input. No voltage divider or extra signal conditioning is needed because the sensor is powered from 3.3 V.

## Valve

- Ball valve item: AliExpress `3256803975349392`
- Valve motor item: AliExpress `3256804718502721`
- Size: 3/4 inch
- Control type: CR5 01 / CR501
- Voltage: 12 V DC
- Thread type: NPT
- Material: 304 stainless steel
- Pressure rating: 600 WOG

Verify local plumbing requirements and potable-water suitability before installation.

## Enclosure

- Supplier: Amazon
- ASIN: `B0CSJXTLGZ`
- Style: lockable outdoor weatherproof enclosure
- Size selected: 11.4 in x 7.5 in x 5.5 in
- DIN terminal blocks: Dinkle DK2.5N, 12 to 22 AWG, various colors
- Cable glands: TE Connectivity ENTRELEC `1SNG601161R0000`
- 12 V DIN rail power supply: Ideal Power `56YSD60S-1204500`

## Panel accessories

| Qty | Part | Description |
| ---: | --- | --- |
| 2 | McMaster-Carr `2779K111` | Green 12 V DC quick-disconnect panel LED, 5/16 inch panel cutout |
| 2 | McMaster-Carr `2779K113` | Red 12 V DC quick-disconnect panel LED, 5/16 inch panel cutout |

Quick-disconnect terminals are used for the external panel LED wiring.
