# Water Monitor

Intelligent water monitoring for Home Assistant. ESP32-based hardware with a
companion HA addon that learns your home's water usage patterns, identifies
fixtures, runs scheduled leak tests, and alerts on anomalies.

## Repository structure

This is a monorepo containing all components of the water monitor system:

```
.
├── addon/         # Home Assistant addon (FastAPI + SQLite + clustering)
├── firmware/      # ESPHome firmware for the ESP32-S3-WROOM-1
├── integration/   # Native HA integration (planned for v2.4)
└── docs/          # Hardware build notes, pinouts, troubleshooting
```

Each subdirectory has its own `README.md` and `CHANGELOG.md`.

## Quick start

1. Flash the firmware in `firmware/` to your ESP32-S3-WROOM-1
2. Install the addon from `addon/` via the Home Assistant addon store
3. Open the addon UI and follow the setup wizard

See `addon/README.md` for full installation and setup instructions, and
`docs/hardware.md` for the hardware build guide.

## Project status

- **v0.1.x** — Core monitoring, leak detection, calibration ✅ shipped
- **v0.2.x** — Fixture identification (Phase 2) 🚧 in development
- **v0.3.x+** — Anomaly detection, native HA integration

See `addon/CHANGELOG.md` for detailed release notes.

## License

MIT — see `LICENSE` file.

## Contributing

Issues and pull requests welcome once the project goes public. For now,
this repository is in development and the API/schema may change.


## Note
Currently this is just a hobby project to create a Home Assistant compatible "smart" water shut off valve for my house.

it is using Esp home with a custom made esp32-s3 board with latching relays to control simple 5 wire motorized valves.

I started this project maily because because I was not happy with Moen Flo or Streamlabs Control being internet polling or requiring a subscription for certain features. While my "system"nis definitely not compact or a s pretty as Moen or Stream labs it has most of the same basic features. Now I will admit the other part of this is lazyness as I didnnot want to have to get into my crawl space to shut off the water to the irrigation for winter. i would rather use an app or switch out sode of the crawl space tonturn the valve on or off.

So I planned from the begining to bot only have the we esp control things but to have a manual mode where I could control the valve with switches and have indicator lights to show the status. This would alao mean hopefully someone other than my self to open and close the valve for rhe house water even if network is down or I was not available.

So I started looking at valves with motors, pressure tansducers and flow sensors. to keep cost lower I mostly got the items from Aliexpress, i will have links to the parts I used. normal commercial available valve from local plumbing or HVAC suppliers were 10 times the cost.

That being said I still over built this thing. for example inwent with a 3piece stainless steel ball valve as its "service" able and I truat stainless steel likely be lead free vs the brass ball valvem from AliExpress were suspect. then the motor I picked one that has a manual way tonopen and close the valve id there was ever a power outage. 
That beong said this project could easily be modfied to use more basic components.

For the PCB this was mainly where the hobby part came in. I have never used SmD components nor design a circuit board with them before. While the current design works, I have a small pile of boards hidding on my shelf of ones that don't. Also again this was desogn and built for my situation so it is definitely not optimized for manufacturingnor costs.

I guess the TLDR is i made a thing it mostly works if you want to copy it or improve it; that cool but I am likely not going to make another one for my self.