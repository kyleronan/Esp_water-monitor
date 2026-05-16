# Bill of Materials

This is the simplified builder-facing BOM for the PCB and related hardware.


## Important BOM notes

- The KiCad source files included in this package use `470R` for the installed resistor value.
- For 4-position terminal groups, the build uses two Phoenix Contact `1935161` 2-position terminal blocks.
- The confirmed quantity for Phoenix Contact `1935161` terminal blocks is 9 per build.
- Majority of PCB components were purchased from Digikey

## PCB BOM

| Qty | References | Value | Footprint | Part | Supplier Part # | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 6 | C1, C2, C3, C4, C5, C6 | `0.01uF` | `Capacitor_SMD:C_0603_1608Metric` | KEMET C0603C103J3HACTU | `399-C0603C103J3HACTUCT-ND` |  |
| 5 | C9, C10, C12, C13, C14 | `0.1uF` | `Capacitor_SMD:C_0603_1608Metric` | TDK C1608X8R1E104K080AA | `445-2500-1-ND` |  |
| 3 | C7, C8, C11 | `10uF` | `Capacitor_SMD:C_0603_1608Metric` | Samsung Electro-Mechanics CL10X106MO8NRNC | `1276-6769-1-ND` |  |
| 2 | Led1, Led2 | `Conn_01x04_Socket` | `Connector_PinSocket_2.54mm:PinSocket_1x04_P2.54mm_Vertical` | Phoenix Contact 1935161 | `277-1667-ND` | Uses two 2-position 1935161 terminal blocks per 4-position connector group |
| 2 | J1, J2 | `Conn_01x05_Pin` | `Connector_PinSocket_2.54mm:PinSocket_1x05_P2.54mm_Vertical` | Phoenix Contact 1935161 | `277-1667-ND` | Uses PCB terminal blocks, confirmed total 1935161 quantity is 9 per build |
| 2 | J7, J9 | `From PCB` | `TerminalBlock_Phoenix:TerminalBlock_Phoenix_MKDS-1,5-2_1x02_P5.00mm_Horizontal` | Phoenix Contact 1935161 | `277-1667-ND` |  |
| 1 | J14 | `GPIO` | `Connector_PinSocket_2.54mm:PinSocket_1x03_P2.54mm_Vertical` |  |  |  |
| 1 | J15 | `SPI` | `Connector_PinSocket_2.54mm:PinSocket_1x04_P2.54mm_Vertical` | Phoenix Contact 1935161 | `277-1667-ND` | Uses two 2-position 1935161 terminal blocks per 4-position connector group |
| 2 | J3, J8 | `Screw_Terminal_01x02` | `TerminalBlock_Phoenix:TerminalBlock_Phoenix_MKDS-1,5-2_1x02_P5.00mm_Horizontal` | Phoenix Contact 1935161 | `277-1667-ND` |  |
| 2 | J10, J12 | `Screw_Terminal_01x04` | `TerminalBlock_Phoenix:TerminalBlock_Phoenix_MKDS-1,5-4_1x04_P5.00mm_Horizontal` | Phoenix Contact 1935161 | `277-1667-ND` | Uses two 2-position 1935161 terminal blocks per 4-position connector group |
| 2 | J11, J13 | `Screw_Terminal_01x05` | `TerminalBlock_Phoenix:TerminalBlock_Phoenix_MKDS-1,5-5_1x05_P5.00mm_Horizontal` | Phoenix Contact 1935161 | `277-1667-ND` | Uses PCB terminal blocks, confirmed total 1935161 quantity is 9 per build |
| 2 | J5, J6 | `To Motor` | `TerminalBlock_Phoenix:TerminalBlock_Phoenix_MKDS-1,5-2_1x02_P5.00mm_Horizontal` | Phoenix Contact 1935161 | `277-1667-ND` |  |
| 1 | J4 | `USB_C_Receptacle_USB2.0_16P` | `Connector_USB:USB_C_Receptacle_GCT_USB4105-xx-A_16P_TopMnt_Horizontal` | GCT USB4215-03-A | `2073-USB4215-03-ACT-ND` | Verify footprint vs purchased connector |
| 2 | D5, D6 | `MBR140SFT3G` | `mbr140sft3g:SOD-123FLv2` | onsemi MBR140SFT3G | `MBR140SFT3GOSCT-ND` |  |
| 1 | U6 | `ESP32-S3-WROOM-1` | `RF_Module:ESP32-S3-WROOM-1` | Espressif ESP32-S3-WROOM-1-N8R2 | `1965-ESP32-S3-WROOM-1-N8R2-ND` |  |
| 1 | U2 | `R-783.3-1.0` | `Converter_DCDC:Converter_DCDC_RECOM_R-78E-0.5_THT` | Recom Power R-783.3-1.0 | `945-1036-ND` |  |
| 1 | U3 | `R-78E5.0-1.0` | `Converter_DCDC:Converter_DCDC_RECOM_R-78E-0.5_THT` | Recom Power R-78E5.0-1.0 | `945-2201-ND` |  |
| 2 | U1, U4 | `ULN2003ADR` | `ULN2003ADR:SOIC127P600X175-16N` | Texas Instruments ULN2003ADR | `296-1368-1-ND` |  |
| 3 | D1, D3, D7 | `LED_Green` | `LED_SMD:LED_0603_1608Metric` | Harvatek B1931NG--20D001114U1930 | `3147-B1931NG--20D001114U1930CT-ND` |  |
| 2 | D2, D4 | `LED_Red` | `LED_SMD:LED_0603_1608Metric` | Harvatek B1931URO-20D000114U1930 | `3147-B1931URO-20D000114U1930CT-ND` |  |
| 1 | F1 | `1.5A` | `Fuse:Fuse_1812_4532Metric_Pad1.30x3.40mm_HandSolder` | Bel Fuse 0ZCG0150AF2C | `5923-0ZCG0150AF2CCT-ND` |  |
| 4 | Flow1, Flow2, Pressure1, Pressure2 | `Conn_01x03_Pin` | `Connector_PinSocket_2.54mm:PinSocket_1x03_P2.54mm_Vertical` |  |  |  |
| 3 | D8, D9, D10 | `D_TVS` | `Diode_SMD:D_SOD-323_HandSoldering` | Diodes Inc. D5V0F1B2WS-7 | `31-D5V0F1B2WS-7CT-ND` | Verify footprint/package |
| 2 | K3, K4 | `RT424F12` | `RT42f12:TE_5-1393243-4` | TE Connectivity Potter & Brumfield RT424F12 | `RT424F12-ND` |  |
| 7 | R4, R6, R8, R10, R14, R16, R20 | `10K` | `Resistor_SMD:R_0805_2012Metric` | Stackpole RMCF0805FT10K0 | `RMCF0805FT10K0CT-ND` |  |
| 6 | R3, R5, R7, R9, R13, R15 | `20K` | `Resistor_SMD:R_0805_2012Metric` | Stackpole RMCF0805FT20K0 | `RMCF0805FT20K0CT-ND` |  |
| 5 | R1, R2, R11, R12, R19 | `470R` | `Resistor_SMD:R_0805_2012Metric` | Stackpole RMCF0805FT470R | `RMCF0805FT470RCT-ND` |  |
| 2 | R17, R18 | `5.1K` | `Resistor_SMD:R_0805_2012Metric` | Stackpole RMCF0805FT5K10 | `RMCF0805FT5K10CT-ND` |  |
| 1 | SW1 | `Control 1` | `Toggle_switch:ANT23SECQE DPDT` |  |  |  |
| 1 | SW3 | `Control 2` | `Toggle_switch:ANT23SECQE DPDT` |  |  |  |
| 1 | SW2 | `Open_Close_1` | `Toggle_switch:ANT23SECQE DPDT` |  |  |  |
| 1 | SW4 | `Open_Close_2` | `Toggle_switch:ANT23SECQE DPDT` |  |  |  |
| 2 | SW5, SW6 | `SW_Push` | `Push_button_Switch:SW PTS636 SP50 SMTR LFS` | C&K PTS636 SP50 SMTR LFS | `CKN12316-1-ND` |  |
| 1 | TP2 | `12V+` | `TestPoint:TestPoint_Pad_1.5x1.5mm` |  |  |  |
| 1 | TP4 | `3v3+` | `TestPoint:TestPoint_Pad_1.5x1.5mm` |  |  |  |
| 1 | TP3 | `5V+` | `TestPoint:TestPoint_Pad_1.5x1.5mm` |  |  |  |
| 1 | TP1 | `GND` | `TestPoint:TestPoint_Pad_1.5x1.5mm` |  |  |  |

## Enclosure and field hardware


### Pressure sensor

- Supplier: Amazon
- ASIN: `B0CFJ7BN3L` https://www.amazon.com/dp/B0CFJ7BN3L
- Type: 100 PSI pressure transducer
- Thread: 1/8"-27 NPT
- Supply: 5 to 16 V DC
- Output: 0.5 to 4.5 V analog
- Material: 316 stainless steel
- Listed waterproof rating: IP65

#### Pressure sensor wiring

| Wire color | Function |
| --- | --- |
| Red | Supply + |
| Black | Ground |
| Green | Analog sense output |

Pressure sensors are powered from 5 V. The sense output is scaled by a resistor divider before reaching the ESP32 ADC input.

### Flow sensor

- Supplier: AliExpress
- Item: `3256804293371927` https://www.aliexpress.us/item/3256804293371927.html
- Selected version: 5 V version
- Used from the PCB 3.3 V supply because the sensor works down to 3.3 V

#### Flow sensor wiring

| Wire color | Function |
| --- | --- |
| Red | Supply + |
| Black | Ground |
| Yellow | Pulse output |

The flow sensor pulse output connects directly to the ESP32 GPIO input. No voltage divider or extra signal conditioning is needed because the sensor is powered from 3.3 V.

### Valve

- Ball valve item: AliExpress `3256803975349392`https://www.aliexpress.us/item/3256803975349392.html
- Valve motor item: AliExpress `3256804718502721`https://www.aliexpress.us/item/3256804718502721.html
- Size: 3/4 inch
- Control type: CR5 01 / CR501
- Voltage: 12 V DC
- Thread type: NPT
- Material: 304 stainless steel
- Pressure rating: 600 WOG

Verify local plumbing requirements and potable-water suitability before installation.

### Enclosure

- Supplier: Amazon
- ASIN: `B0CSJXTLGZ` https://www.amazon.com/dp/B0CSJXTLGZ
- Style: lockable outdoor weatherproof enclosure
- Size selected: 11.4 in x 7.5 in x 5.5 in
- DIN terminal blocks: Dinkle DK2.5N, 12 to 22 AWG, various colors
- Cable glands: TE Connectivity ENTRELEC `1SNG601161R0000`
- 12 V DIN rail power supply: Ideal Power `56YSD60S-1204500`

### Panel accessories

| Qty | Part | Description |
| ---: | --- | --- |
| 2 | McMaster-Carr `2779K111` | Green 12 V DC quick-disconnect panel LED, 5/16 inch panel cutout |
| 2 | McMaster-Carr `2779K113` | Red 12 V DC quick-disconnect panel LED, 5/16 inch panel cutout |

Quick-disconnect terminals are used for the external panel LED wiring.

### DIN rail power supply

The build uses an Ideal Power `56YSD60S-1204500` 12 V DIN rail power supply.

