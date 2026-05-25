"""Offline replay of a propagation-delay capture blob.

Loads a JSON capture emitted by the event detector when
`debug_capture_propagation` is enabled and runs the SAME production scan
function (`water_monitor.app.event_detector.scan_propagation_delay`) against
it.  This lets the propagation algorithm be developed and debugged against
real captured events instead of synthetic guesses.

Usage:
    python -m water_monitor.tools.replay_propagation_capture path/to/capture.json

The input file may be a raw JSON capture or a Home Assistant log line that
contains the 'PROPAGATION_CAPTURE {...}' marker — both are accepted.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from water_monitor.app.event_detector import scan_propagation_delay

_MARKER = "PROPAGATION_CAPTURE "

_RESULT_FIELDS = (
    "delay_ms", "status", "stop_reason", "sample_count", "buffer_span_s",
    "baseline_psi", "min_pressure_psi", "min_smoothed_psi",
    "magnitude_gate_passed", "onset_index", "onset_ts",
    "raw_delay_ms", "final_delay_ms",
)


def load_capture(path: Path) -> dict:
    """Parse a capture blob from a raw JSON file or a log line containing it."""
    text = path.read_text(encoding="utf-8").strip()
    if _MARKER in text:
        text = text[text.rindex(_MARKER) + len(_MARKER):]
    brace = text.find("{")
    if brace > 0:
        text = text[brace:]
    return json.loads(text)


def reconstruct(capture: dict) -> Tuple[List[float], Optional[List[datetime]]]:
    """Return (pressure, timestamps) lists from a capture blob.

    timestamps is None when the capture lacks per-sample offsets.
    """
    samples = capture.get("samples", [])
    pressure = [float(p) for _, p in samples]
    timestamps: Optional[List[datetime]] = None
    t0_raw = capture.get("samples_t0")
    if t0_raw and samples and all(off is not None for off, _ in samples):
        t0 = datetime.fromisoformat(t0_raw)
        timestamps = [t0 + timedelta(milliseconds=float(off)) for off, _ in samples]
    return pressure, timestamps


def replay(capture: dict) -> None:
    """Run scan_propagation_delay against a capture and print the comparison."""
    meta = capture.get("meta", {})
    propagation_onset_psi = float(meta.get("propagation_onset_psi", 0.2))
    pressure, timestamps = reconstruct(capture)
    flow_onset_ts = datetime.fromisoformat(capture["flow_onset_ts"])

    result = scan_propagation_delay(
        pressure, timestamps, flow_onset_ts, propagation_onset_psi)

    print("=== propagation capture replay ===")
    print(f"circuit              : {meta.get('circuit')}")
    print(f"version / git        : {meta.get('version')} / {meta.get('git')}")
    print(f"samples (downsample) : {len(pressure)} (x{meta.get('downsample', 1)})")
    print(f"flow_onset_ts        : {capture.get('flow_onset_ts')}")
    print(f"propagation_onset_psi: {propagation_onset_psi}")
    print(f"start_ts             : {capture.get('start_ts')}")
    print(f"trustworthy_baseline : {meta.get('trustworthy_baseline')}")
    print()
    print("--- scan_propagation_delay (current code) ---")
    for field in _RESULT_FIELDS:
        print(f"  {field:22s}: {getattr(result, field)}")

    stored = capture.get("result")
    if stored:
        print()
        print("--- result stored in capture (when produced) ---")
        for key, value in stored.items():
            print(f"  {key:22s}: {value}")
        if stored.get("delay_ms") != result.delay_ms:
            print()
            print(f"  NOTE: stored delay_ms={stored.get('delay_ms')} differs from "
                  f"replay delay_ms={result.delay_ms} "
                  "(scan algorithm changed since this capture).")


def main(argv: Optional[List[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print(__doc__)
        return 2
    path = Path(argv[0])
    if not path.exists():
        print(f"error: capture file not found: {path}")
        return 1
    try:
        capture = load_capture(path)
    except Exception as e:
        print(f"error: could not parse capture: {e}")
        return 1
    replay(capture)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
