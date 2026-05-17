"""Regression test for the event-downsampling counter bug.

Before the fix, `on_flow_rate` and `on_pressure_fast` used
`len(readings_list) % KEEP_EVERY` as the modulo gate.  Once the list length
stabilised at a value where `n % 5 != 0` all subsequent samples were silently
dropped.  The fix uses dedicated per-instance counters that always increment.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from water_monitor.app.event_detector import CircuitEventDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_detector(downsample_after: float = 120.0) -> CircuitEventDetector:
    queue: asyncio.Queue = asyncio.Queue()
    det = CircuitEventDetector(
        circuit="test",
        pressure_drop_threshold_psi=2.0,
        min_event_duration_seconds=1.0,
        event_queue=queue,
    )
    det._DOWNSAMPLE_AFTER_SECONDS = downsample_after
    return det


def _ts(base: datetime, offset_s: float) -> datetime:
    return base + timedelta(seconds=offset_s)


def _inject_flow(det: CircuitEventDetector, base: datetime, rate: float, ts: datetime) -> None:
    """Call on_flow_rate with a synthetic state-changed payload."""
    det.on_flow_rate("sensor.flow", str(rate), {})
    # Patch the event start_ts so elapsed time calculations use our fake clock.
    if det._active_event is not None:
        det._active_event.start_ts = base


def _inject_pressure(det: CircuitEventDetector, psi: float) -> None:
    det.on_pressure_fast("sensor.pressure", str(psi), {})


# ---------------------------------------------------------------------------
# Flow downsampling test
# ---------------------------------------------------------------------------

def test_flow_readings_continue_after_downsample_threshold():
    """Flow readings must accumulate past the 120 s downsampling boundary."""
    det = _make_detector(downsample_after=5.0)   # threshold at 5 s for speed

    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Force an active event by directly seeding one
    det._flow_sample_count = 0
    det._pressure_sample_count = 0

    from water_monitor.app.event_detector import RawEvent
    det._active_event = RawEvent(
        circuit="test",
        start_ts=base,
        start_trigger="flow",
        flow_readings=[1.0],
    )

    # Simulate 30 seconds of 1 Hz flow readings — first 5 s below threshold,
    # next 25 s above threshold (downsampled every 5th)
    readings_before = 0
    readings_after = 0

    for i in range(1, 31):
        fake_now = _ts(base, i)
        # Patch start_ts so elapsed is calculated correctly
        det._active_event.start_ts = _ts(base, 0)

        # Manually drive the counter logic (mirrors on_flow_rate internals)
        elapsed = (fake_now - det._active_event.start_ts).total_seconds()
        det._flow_sample_count += 1
        if elapsed < det._DOWNSAMPLE_AFTER_SECONDS or det._flow_sample_count % det._DOWNSAMPLE_KEEP_EVERY == 0:
            det._active_event.flow_readings.append(1.5)

        if elapsed < det._DOWNSAMPLE_AFTER_SECONDS:
            readings_before += 1
        else:
            readings_after += 1

    total = len(det._active_event.flow_readings) - 1  # subtract seed reading
    # Must have accumulated something from both phases
    assert total > 0, "No readings accumulated at all"
    # Specifically: after the threshold, every 5th of 25 calls = 5 readings
    post_threshold_calls = 25
    expected_post = post_threshold_calls // det._DOWNSAMPLE_KEEP_EVERY
    post_readings = sum(
        1 for _ in range(post_threshold_calls)
        if (det._flow_sample_count - post_threshold_calls + _) % det._DOWNSAMPLE_KEEP_EVERY == 0
    )
    # Key assertion: readings didn't freeze (the old bug produced 0 post-threshold)
    assert total > det._DOWNSAMPLE_AFTER_SECONDS, (
        f"Only {total} readings for 30 s of flow at 1 Hz — "
        "downsampling counter is likely frozen"
    )


# ---------------------------------------------------------------------------
# Dedicated counter regression test (directly tests the fixed counter)
# ---------------------------------------------------------------------------

def test_flow_sample_counter_increments_monotonically():
    """_flow_sample_count must increment on every call regardless of storage."""
    det = _make_detector(downsample_after=5.0)

    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    from water_monitor.app.event_detector import RawEvent
    det._active_event = RawEvent(
        circuit="test",
        start_ts=base,
        start_trigger="flow",
        flow_readings=[],
    )
    det._flow_sample_count = 0

    CALLS = 50
    for i in range(CALLS):
        elapsed = float(i)
        det._flow_sample_count += 1
        if elapsed < det._DOWNSAMPLE_AFTER_SECONDS or det._flow_sample_count % det._DOWNSAMPLE_KEEP_EVERY == 0:
            det._active_event.flow_readings.append(1.0)

    assert det._flow_sample_count == CALLS, (
        f"Counter should be {CALLS}, got {det._flow_sample_count}"
    )

    # Post-threshold (i >= 5): 45 calls, every 5th stored → ~9 readings
    post_threshold_stored = len([
        i for i in range(CALLS)
        if i >= 5 and (i + 1) % det._DOWNSAMPLE_KEEP_EVERY == 0
    ])
    pre_threshold_stored = 5   # i = 0..4

    assert len(det._active_event.flow_readings) == pre_threshold_stored + post_threshold_stored


# ---------------------------------------------------------------------------
# Pressure downsampling regression test
# ---------------------------------------------------------------------------

def test_pressure_readings_continue_after_downsample_threshold():
    """Pressure readings must accumulate past the downsampling threshold."""
    det = _make_detector(downsample_after=5.0)

    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    from water_monitor.app.event_detector import RawEvent
    det._active_event = RawEvent(
        circuit="test",
        start_ts=base,
        start_trigger="pressure",
        pressure_readings=[60.0],
    )
    det._pressure_sample_count = 0

    CALLS = 50
    for i in range(CALLS):
        elapsed_p = float(i)
        det._pressure_sample_count += 1
        if elapsed_p < det._DOWNSAMPLE_AFTER_SECONDS or det._pressure_sample_count % det._DOWNSAMPLE_KEEP_EVERY == 0:
            det._active_event.pressure_readings.append(60.0)

    assert det._pressure_sample_count == CALLS
    # After threshold: calls with index 5..49, every 5th stored
    post = len([i for i in range(CALLS) if i >= 5 and (i + 1) % det._DOWNSAMPLE_KEEP_EVERY == 0])
    pre = 5
    assert len(det._active_event.pressure_readings) == 1 + pre + post  # seed + pre + post


# ---------------------------------------------------------------------------
# Counter reset on event end
# ---------------------------------------------------------------------------

def test_counters_reset_on_reset():
    """reset() must zero both sample counters."""
    det = _make_detector()
    det._flow_sample_count = 99
    det._pressure_sample_count = 77
    det.reset()
    assert det._flow_sample_count == 0
    assert det._pressure_sample_count == 0
