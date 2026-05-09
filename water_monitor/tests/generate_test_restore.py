"""
Generate a Quick Restore v3 JSON file with 30 days of synthetic water usage
data for a 2 full bath / 1 half bath / 3100 sqft / 2-person household.

Produces: water_monitor/tests/wm_test_restore_30days.json

Run with:  python water_monitor/tests/generate_test_restore.py
"""
from __future__ import annotations

import json
import math
import random
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 42
random.seed(SEED)

CIRCUIT = "main"
START_DATE = datetime(2026, 4, 9, tzinfo=timezone.utc)
END_DATE   = datetime(2026, 5, 9, tzinfo=timezone.utc)   # exclusive

# ── Fixture cluster definitions ────────────────────────────────────────────

CONFIRMED_FIXTURE_MAP = {
    1: "fix-toilet-main",
    6: "fix-bathroom-tap",
    7: "fix-kitchen-quick",
    8: "fix-kitchen-long",
}

MATCH_LEVEL = {
    1: "confirmed", 2: "learning",
    3: "preliminary", 4: "preliminary", 5: "preliminary",
    6: "confirmed",
    7: "confirmed", 8: "confirmed",
    9: "preliminary", 10: "preliminary",
    11: "learning",
    12: "preliminary",
}

MATCH_CONFIDENCE = {
    "confirmed": 0.92,
    "learning":  0.78,
    "preliminary": 0.60,
}

# Each cluster: (mean_dur_s, std_dur_s, mean_flow, std_flow,
#                pressure_lo, pressure_hi, has_transient, flow_var_lo, flow_var_hi,
#                daily_rate, start_trigger, hour_ranges, weekend_multiplier)
CLUSTERS: dict[int, dict] = {
    1: dict(
        name="toilet (main)", dur=(75, 15), flow=(4.8, 0.5),
        psi=(3.0, 7.0), transient=0, var=(0.05, 0.20),
        daily=9.0, trigger="flow",
        hours=[(6, 23)], weekend=1.0,
    ),
    2: dict(
        name="toilet (guest)", dur=(75, 15), flow=(4.8, 0.5),
        psi=(3.0, 7.0), transient=0, var=(0.05, 0.20),
        daily=2.5, trigger="flow",
        hours=[(6, 23)], weekend=1.0,
    ),
    3: dict(
        name="shower (master)", dur=(480, 120), flow=(6.9, 0.8),
        psi=(5.0, 12.0), transient=1, var=(0.5, 1.2),
        daily=1.0, trigger="pressure",
        hours=[(6, 9), (19, 22)], weekend=1.2,
    ),
    4: dict(
        name="shower (guest)", dur=(450, 120), flow=(6.7, 0.8),
        psi=(5.0, 11.0), transient=1, var=(0.5, 1.2),
        daily=0.9, trigger="pressure",
        hours=[(7, 10)], weekend=2.5,
    ),
    5: dict(
        name="bath", dur=(900, 200), flow=(7.3, 1.0),
        psi=(8.0, 15.0), transient=1, var=(1.0, 2.0),
        daily=0.28, trigger="pressure",
        hours=[(19, 21)], weekend=1.5,
    ),
    6: dict(
        name="bathroom_tap", dur=(20, 8), flow=(3.6, 0.5),
        psi=(2.0, 5.0), transient=0, var=(0.1, 0.3),
        daily=9.0, trigger="flow",
        hours=[(6, 23)], weekend=1.0,
    ),
    7: dict(
        name="kitchen_tap (quick)", dur=(8, 3), flow=(3.8, 0.5),
        psi=(1.5, 4.5), transient=0, var=(0.1, 0.25),
        daily=10.0, trigger="flow",
        hours=[(7, 21)], weekend=0.9,
    ),
    8: dict(
        name="kitchen_tap (long)", dur=(45, 15), flow=(3.7, 0.5),
        psi=(2.0, 6.0), transient=0, var=(0.15, 0.30),
        daily=7.5, trigger="flow",
        hours=[(7, 21)], weekend=0.9,
    ),
    9: dict(
        name="dishwasher", dur=(5400, 600), flow=(0.16, 0.02),
        psi=(0.5, 2.0), transient=0, var=(2.0, 3.5),
        daily=1.5, trigger="flow",
        hours=[(18, 22)], weekend=0.8,
    ),
    10: dict(
        name="washing_machine", dur=(3600, 900), flow=(1.0, 0.12),
        psi=(1.0, 4.0), transient=0, var=(2.5, 4.0),
        daily=0.72, trigger="flow",
        hours=[(8, 18)], weekend=1.3,
    ),
    11: dict(
        name="refrigerator", dur=(5, 2), flow=(2.4, 0.3),
        psi=(0.5, 2.0), transient=0, var=(0.02, 0.08),
        daily=4.0, trigger="flow",
        hours=[(6, 22)], weekend=1.0,
    ),
    12: dict(
        name="hose_bib", dur=(1200, 300), flow=(1.5, 0.3),
        psi=(3.0, 8.0), transient=1, var=(0.3, 0.8),
        daily=0.10, trigger="flow",
        hours=[(8, 18)], weekend=4.0,
    ),
}


def _rand_hour(hour_ranges: list[tuple[int, int]]) -> int:
    """Pick a random hour from the given hour windows."""
    total = sum(hi - lo for lo, hi in hour_ranges)
    pick = random.randint(0, total - 1)
    for lo, hi in hour_ranges:
        span = hi - lo
        if pick < span:
            return lo + pick
        pick -= span
    return hour_ranges[-1][1] - 1


def _event_id(circuit: str, ts: datetime) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{circuit}/{ts.isoformat()}"))


def generate_events() -> list[dict]:
    events = []
    day = START_DATE
    while day < END_DATE:
        is_weekend = day.weekday() >= 5
        for cid, cfg in CLUSTERS.items():
            daily = cfg["daily"]
            if is_weekend:
                daily *= cfg["weekend"]

            # Poisson draw for event count this day
            count = 0
            if daily >= 1:
                count = max(0, int(random.gauss(daily, math.sqrt(daily))))
            else:
                count = 1 if random.random() < daily else 0

            for _ in range(count):
                hour = _rand_hour(cfg["hours"])
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                start_ts = day + timedelta(hours=hour, minutes=minute, seconds=second)
                if start_ts >= END_DATE:
                    continue

                dur = max(2.0, random.gauss(*cfg["dur"]))
                flow = max(0.05, random.gauss(*cfg["flow"]))
                peak = flow * random.uniform(1.05, 1.40)
                vol = round(flow * dur / 60.0, 3)
                psi = round(random.uniform(*cfg["psi"]), 2)
                var = round(random.uniform(*cfg["var"]), 3)
                h = hour + minute / 60.0
                h_sin = round(math.sin(2 * math.pi * h / 24), 6)
                h_cos = round(math.cos(2 * math.pi * h / 24), 6)
                end_ts = start_ts + timedelta(seconds=dur)

                level = MATCH_LEVEL[cid]
                ev = {

                    "id": _event_id(CIRCUIT, start_ts),
                    "circuit": CIRCUIT,
                    "start_ts": start_ts.isoformat(),
                    "end_ts": end_ts.isoformat(),
                    "duration_seconds": round(dur, 2),
                    "avg_flow_lpm": round(flow, 3),
                    "peak_flow_lpm": round(peak, 3),
                    "volume_litres": vol,
                    "pressure_delta_psi": psi,
                    "pre_event_pressure_psi": round(random.uniform(55, 75), 2),
                    "min_pressure_psi": None,
                    "has_pressure_transient": int(cfg["transient"]),
                    "flow_variability": var,
                    "hour_of_day": hour,
                    "day_of_week": day.weekday(),
                    "duration_log": round(math.log(dur + 1), 6),
                    "hour_sin": h_sin,
                    "hour_cos": h_cos,
                    "is_weekend": int(is_weekend),
                    "start_trigger": cfg["trigger"],
                    "cluster_id": cid,
                    "fixture_id": CONFIRMED_FIXTURE_MAP.get(cid),
                    "match_confidence": round(MATCH_CONFIDENCE[level], 4),
                    "match_level": level,
                    "excluded_from_training": 0,
                    "anomaly_score": 0.0,
                    "flagged": 0,
                    "created_at": start_ts.isoformat(),
                }
                events.append(ev)

        day += timedelta(days=1)   # advance to next day — was missing, caused infinite loop

    # Sort by start_ts
    events.sort(key=lambda e: e["start_ts"])

    # Back-fill seconds_since_prev_event and seconds_to_next_event
    for i, ev in enumerate(events):
        if i > 0:
            prev_end = events[i - 1]["end_ts"]
            gap = (
                datetime.fromisoformat(ev["start_ts"])
                - datetime.fromisoformat(prev_end)
            ).total_seconds()
            ev["seconds_since_prev_event"] = round(max(0, gap), 1)
            ev["prev_cluster_id"] = events[i - 1]["cluster_id"]
        if i < len(events) - 1:
            next_start = events[i + 1]["start_ts"]
            gap = (
                datetime.fromisoformat(next_start)
                - datetime.fromisoformat(ev["end_ts"])
            ).total_seconds()
            ev["seconds_to_next_event"] = round(max(0, gap), 1)

    return events


def build_hourly_volume(events: list[dict]) -> list[dict]:
    buckets: dict[tuple, float] = defaultdict(float)
    for ev in events:
        ts = datetime.fromisoformat(ev["start_ts"])
        hour_key = ts.replace(minute=0, second=0, microsecond=0)
        buckets[(CIRCUIT, hour_key.isoformat())] += ev["volume_litres"]
    return [
        {"circuit": c, "hour_ts": h, "volume_litres": round(v, 3)}
        for (c, h), v in sorted(buckets.items())
    ]


def build_daily_summary(events: list[dict]) -> list[dict]:
    by_day: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        day = ev["start_ts"][:10]
        by_day[day].append(ev)

    rows = []
    for day in sorted(by_day):
        day_evs = by_day[day]
        total_vol = sum(e["volume_litres"] for e in day_evs)
        flows = [e["avg_flow_lpm"] for e in day_evs]
        peaks = [e["peak_flow_lpm"] for e in day_evs]

        # Top 5 fixture_ids by count
        fix_counts: dict[str, int] = defaultdict(int)
        for e in day_evs:
            if e.get("fixture_id"):
                fix_counts[e["fixture_id"]] += 1
        top5 = sorted(fix_counts.items(), key=lambda x: -x[1])[:5]
        breakdown = json.dumps([{"fixture_id": fid, "count": cnt} for fid, cnt in top5])

        rows.append({
            "circuit": CIRCUIT,
            "day": day,
            "event_count": len(day_evs),
            "volume_litres": round(total_vol, 3),
            "avg_flow_lpm": round(sum(flows) / len(flows), 3) if flows else 0.0,
            "peak_flow_lpm": round(max(peaks), 3) if peaks else 0.0,
            "alert_count": 0,
            "fixture_breakdown": breakdown,
        })
    return rows


def build_fixtures() -> list[dict]:
    now = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc).isoformat()
    return [
        {
            "id": "fix-toilet-main",
            "circuit": CIRCUIT,
            "name": "Main Toilet",
            "auto_name": "Toilet A",
            "confirmed": 1,
            "fixture_type": "toilet",
            "display_name": "Main Toilet",
            "user_locked": 1,
            "publish_to_ha": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "fix-bathroom-tap",
            "circuit": CIRCUIT,
            "name": "Bathroom Tap",
            "auto_name": "Tap A",
            "confirmed": 1,
            "fixture_type": "bathroom_tap",
            "display_name": "Bathroom Tap",
            "user_locked": 0,
            "publish_to_ha": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "fix-kitchen-quick",
            "circuit": CIRCUIT,
            "name": "Kitchen Quick Rinse",
            "auto_name": "Tap B",
            "confirmed": 1,
            "fixture_type": "kitchen_tap",
            "display_name": "Kitchen (Quick)",
            "user_locked": 0,
            "publish_to_ha": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "id": "fix-kitchen-long",
            "circuit": CIRCUIT,
            "name": "Kitchen Long Rinse",
            "auto_name": "Tap C",
            "confirmed": 1,
            "fixture_type": "kitchen_tap",
            "display_name": "Kitchen (Long)",
            "user_locked": 0,
            "publish_to_ha": 1,
            "created_at": now,
            "updated_at": now,
        },
    ]


def build_device_config() -> list[dict]:
    # Intentionally empty — device config (HA device ID, entity prefix, setup_complete)
    # must come from the real setup wizard and must not be overwritten by test data.
    return []


def build_home_profile() -> list[dict]:
    return [{
        "id": 1,
        "bathrooms_full": 2,
        "bathrooms_half": 1,
        "sqft": 3100,
        "floors": 2,
        "occupants": 2,
        "supply_type": "mains",
        "setup_complete": 1,
        "away_mode": 0,
        "flow_unit": "L/min",
        "pressure_unit": "psi",
        "publish_fixtures_to_ha": 1,
        "mobile_notify_targets": "",
        "ha_presence_entities": "",
        "ha_away_state": "not_home",
        "ha_home_state": "home",
    }]


def build_circuit_entity_map() -> list[dict]:
    # Intentionally empty — entity mapping must come from the real setup wizard.
    # Including fake entity IDs here would overwrite the real mapping and break
    # the addon's connection to HA sensors.
    return []


FEATURE_KEYS = [
    "avg_flow_lpm", "peak_flow_lpm", "duration_seconds", "volume_litres",
    "pressure_delta_psi", "has_pressure_transient", "flow_variability",
    "hour_sin", "hour_cos",
]

CLUSTER_TYPE_MAP = {
    1: "toilet",          2: "toilet",
    3: "shower",          4: "shower",
    5: "bath",
    6: "bathroom_tap",
    7: "kitchen_tap",     8: "kitchen_tap",
    9: "dishwasher",      10: "washing_machine",
    11: "refrigerator_water",
    12: "hose_bib",
}


def build_fixture_clusters(events: list[dict]) -> list[dict]:
    """Compute DBSTREAM-style cluster centroids from the generated events."""
    by_cluster: dict[int, list[dict]] = defaultdict(list)
    for ev in events:
        by_cluster[ev["cluster_id"]].append(ev)

    now = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc).isoformat()
    rows = []
    for cid in sorted(by_cluster):
        evs = by_cluster[cid]
        centroid = {}
        feat_std = {}
        for key in FEATURE_KEYS:
            vals = [float(ev.get(key) or 0) for ev in evs]
            centroid[key] = round(statistics.mean(vals), 6)
            feat_std[key] = round(statistics.stdev(vals), 6) if len(vals) > 1 else 0.0

        level = MATCH_LEVEL[cid]
        last_match = max(ev["start_ts"] for ev in evs)
        rows.append({
            "id":                   cid,
            "circuit":              CIRCUIT,
            "centroid":             json.dumps(centroid),
            "feature_std":          json.dumps(feat_std),
            "transient_template":   None,
            "member_count":         len(evs),
            "suggested_type":       CLUSTER_TYPE_MAP.get(cid),
            "suggested_confidence": round(MATCH_CONFIDENCE[level], 4),
            "confidence_level":     level,
            "fixture_id":           CONFIRMED_FIXTURE_MAP.get(cid),
            "is_compound":          0,
            "component_cluster_ids": None,
            "publish_to_ha":        1,
            "created_at":           now,
            "last_match_at":        last_match,
        })
    return rows


def build_training_state() -> list[dict]:
    """Return training_state rows with calibration already complete for 'main'."""
    now = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc).isoformat()
    calibration_ended = (START_DATE + timedelta(days=14)).isoformat()
    return [
        {
            "circuit":            CIRCUIT,
            "state":              "labelling",
            "calibration_days":   14,
            "started_at":         START_DATE.isoformat(),
            "calibration_ends_at": calibration_ended,
            "minimum_events":     150,
            "events_collected":   1310,
            "labelling_deadline": datetime(2026, 8, 9, tzinfo=timezone.utc).isoformat(),
            "completed_at":       None,
            "updated_at":         now,
        },
        # irrigation has no test events — leave in calibrating state
        {
            "circuit":            "irrigation",
            "state":              "calibrating",
            "calibration_days":   14,
            "started_at":         START_DATE.isoformat(),
            "calibration_ends_at": datetime(2026, 5, 23, tzinfo=timezone.utc).isoformat(),
            "minimum_events":     150,
            "events_collected":   0,
            "labelling_deadline": None,
            "completed_at":       None,
            "updated_at":         now,
        },
    ]


def main() -> None:
    print("Generating events…", flush=True)
    events = generate_events()
    print(f"  {len(events)} events generated", flush=True)

    print("  Building aggregations…", flush=True)
    hourly    = build_hourly_volume(events)
    daily     = build_daily_summary(events)
    clusters  = build_fixture_clusters(events)
    print(f"  {len(hourly)} hourly rows, {len(daily)} daily rows, "
          f"{len(clusters)} fixture_clusters", flush=True)

    payload = {
        "backup_type": "quick_restore",
        "version": 3,
        "exported_at": "2026-05-09T12:00:00+00:00",
        "history_days": 365,
        "tables": {
            "device_config":      build_device_config(),
            "home_profile":       build_home_profile(),
            "circuit_entity_map": build_circuit_entity_map(),
            "training_state":     build_training_state(),
            "fixtures":           build_fixtures(),
            "fixture_signatures": [],
            "fixture_clusters":   clusters,
            "cluster_cooccurrence": [],
            "daily_summary":      daily,
            "events":             events,
            "hourly_volume":      hourly,
        },
    }

    out = Path(__file__).parent / "wm_test_restore_30days.json"
    print(f"  Writing {out.name}…", flush=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    size_mb = out.stat().st_size / 1_048_576
    print(f"  Done — {size_mb:.2f} MB", flush=True)

    print("\nEvents per cluster:")
    by_cluster: dict[int, int] = defaultdict(int)
    for ev in events:
        by_cluster[ev["cluster_id"]] += 1
    for cid in sorted(by_cluster):
        name = CLUSTERS[cid]["name"]
        print(f"  {cid:2d}  {name:<28s}  {by_cluster[cid]:4d}")


if __name__ == "__main__":
    main()
