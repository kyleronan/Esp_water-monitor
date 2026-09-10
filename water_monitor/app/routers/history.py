"""History router — event log and leak test history."""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ..circuit_compat import resolve_circuit
from ..fixtures import FIXTURE_TYPE_LABELS, user_selectable_types
from ..database import (
    _CYCLE_PULSE_WINDOW_SECONDS,
    _NOTE_KIND_SQL,
    _parse_event_ts,
    classify_action,
    clear_auto_labels,
    clear_event_classification,
    count_not_real_events,
    get_daily_summaries,
    get_home_profile,
    get_leak_test_history,
    get_pump_regime_nights,
    get_recent_events,
    local_day_of,
    note_locked_write,
    patch_event as _patch_event,
    propagate_cycle_label,
    recompute_cluster_suggestion_from_user_labels,
    revert_artifact_zeroing_on_relabel,
    revert_irrigation_cross_talk,
    run_db,
    set_event_classification,
    upsert_fixture_signature)
from ..task_registry import spawn
from ._helpers import (
    _orch,
    _tmpl,
    coerce_float,
    ingress_redirect,
    reclassify_in_background,
    run_blocking,
    startup_gate)
_VALID_USER_FIXTURE_TYPES: frozenset = frozenset(user_selectable_types())

log = logging.getLogger(__name__)
router = APIRouter(prefix="/history")

DEFAULT_EVENT_LIMIT = 100
# Ceiling for the caller-supplied ?limit= on the JSON route. Every DB
# call goes through the single serialized executor, so an unbounded
# limit is a process-wide stall, not just a slow response.
MAX_EVENT_LIMIT = 1000


async def _bg_reclassify(circuit: str) -> None:
    """The label-change reclassify. Named here because the debounce below and
    ``test_dangling_tasks`` both reach for it by name."""
    await reclassify_in_background(circuit, skip_when_baseline_locked=True,
                                   what="background")


# Debounce state: coalesce a burst of label saves into ONE reclassify after a
# quiet period. One reclassify per save is ~10s and holds the write lock, so
# rapid labelling stacks them until the next save's own write times out
# ("database is locked"). A monotonic generation per circuit means only the LAST
# save in a burst runs the reclassify. An already-running one is never cancelled
# (idempotent, but cancellation mid-write is messy) — superseded delays no-op.
_RECLASSIFY_DEBOUNCE_S: float = 8.0

# How long a label save keeps trying when the database is busy with a background
# job before it gives up and tells the user. The hourly backlog drain yields the
# file write-lock every ~40 rows (~3-4 s), so one retry almost always suffices;
# three cover a reprocess landing at the same moment. Worst case ~7 s of waiting
# plus the per-attempt busy timeout.
_PATCH_RETRY_BACKOFF_S: tuple = (1.0, 2.0, 4.0)


async def _patch_with_retry(db, event_id: str, circuit: str, payload: dict,
                            sleep=None) -> dict:
    """Run the PATCH's DB step, retrying with backoff while it reports
    ``locked``. Split out so the retry policy is testable without a Request.
    ``sleep`` is injectable for tests; defaults to ``asyncio.sleep``."""
    import asyncio
    _sleep = sleep or asyncio.sleep
    outcome = await run_db(_patch_event_sync, db, event_id, circuit, payload)
    attempt = 0
    while outcome.get("locked") and attempt < len(_PATCH_RETRY_BACKOFF_S):
        delay = _PATCH_RETRY_BACKOFF_S[attempt]
        attempt += 1
        log.info("[%s] label save for %s hit a busy database — retry %d/%d in %ss",
                 circuit, event_id, attempt, len(_PATCH_RETRY_BACKOFF_S), delay)
        await _sleep(delay)
        outcome = await run_db(_patch_event_sync, db, event_id, circuit, payload)
    return outcome
_reclassify_gen: dict = {}


def _schedule_reclassify(circuit: str) -> None:
    """Schedule a debounced background reclassify for ``circuit`` (see above)."""
    import asyncio
    gen = _reclassify_gen.get(circuit, 0) + 1
    _reclassify_gen[circuit] = gen

    async def _delayed() -> None:
        try:
            await asyncio.sleep(_RECLASSIFY_DEBOUNCE_S)
        except asyncio.CancelledError:
            return
        if _reclassify_gen.get(circuit) != gen:
            return                      # a newer save superseded this one
        await _bg_reclassify(circuit)

    # dev57 (2.24): via task_registry.spawn. A bare create_task() left the
    # only reference to this task in asyncio's weak set, so the debounced
    # reclassify could be collected mid-sleep and the just-saved label would
    # never propagate — silently, with no traceback.
    spawn(_delayed(), name=f"history_reclassify[{circuit}]")


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def history_page(request: Request):
    # Proactive readiness gate. This page's DB work is the heaviest in the addon
    # and shares ONE DB worker with the startup reclassify/backfill. Checking
    # readiness BEFORE submitting means a page opened during startup answers
    # instantly instead of queueing behind minutes of boot work.
    gated = startup_gate(request, "history", "History", "/history")
    if gated is not None:
        return gated
    try:
        return await _history_page(request)
    except Exception:
        # Log with full traceback at ERROR; the user-facing page stays
        # generic so we don't leak exception text / stack info into the
        # HTML. Pointing at the addon log is enough actionable detail
        # for the user; the rest belongs in the log only.
        log.error("History page error", exc_info=True)
        return HTMLResponse(
            "<h1>History temporarily unavailable</h1>"
            "<p>Something went wrong rendering this page. "
            "Check the addon log for details, or refresh to retry.</p>",
            status_code=500,
        )


_EMBEDDED_KIND_PHRASES = {
    "toilet": ("toilet flush", "toilet flushes"),
    "tap":    ("tap use", "tap uses"),
}


def _embedded_display_label(embedded: list, vol_factor: float,
                            vol_unit: str) -> str:
    """Homeowner-facing 'Contains' text in DISPLAY units, e.g.
    ``"2 toilet flushes + 1 tap use (~2.4 gal)"``. Unknown kinds fall back to
    the raw kind name so a new detector kind still reads sensibly."""
    counts: dict = {}
    total_l = 0.0
    for item in embedded:
        counts[item.get("kind") or "draw"] = counts.get(item.get("kind") or "draw", 0) + 1
        total_l += float(item.get("excess_litres") or 0.0)
    parts = []
    for kind, n in counts.items():
        one, many = _EMBEDDED_KIND_PHRASES.get(
            kind, (kind.replace("_", " "), kind.replace("_", " ") + "s"))
        parts.append(f"{n} {one if n == 1 else many}")
    if not parts:
        return ""
    vol = total_l * (vol_factor or 1.0)
    vol_txt = f"{vol:.1f}".rstrip("0").rstrip(".")
    return " + ".join(parts) + f" (~{vol_txt} {vol_unit or 'L'})"


# Filter-bar query-param names (dev15). Raw display-unit strings from the GET
# form; _filters_to_storage validates + converts them for the SQL pushdown.
FILTER_BAR_PARAMS = ("dur_min", "dur_max", "dp_min", "dp_max",
                     "vol_min", "vol_max", "flow_min", "flow_max",
                     "fixture", "note")


# `forms.coerce_float` contract as used here: `lo=0.0` drops negatives (a
# negative duration/volume filter is not a thing the bar can mean),
# `default=None` keeps "unusable" distinguishable from a real 0, and NaN/inf are
# rejected — a NaN sails past an unguarded range check.
def _filter_float(value):
    """Lenient non-negative float for the filter bar; garbage/blank → None."""
    return coerce_float(value, lo=0.0, default=None)


def _filters_to_storage(raw: dict, vol_factor: float,
                        pressure_factor: float,
                        flow_factor: float = 1.0) -> dict:
    """Convert the filter bar's display-unit values to get_recent_events
    kwargs in STORAGE units (litres / PSI / L·min⁻¹ / seconds). Pure —
    unit-tested.

    Inputs arrive as the user typed/slid them: duration in MINUTES, volume /
    ΔP / avg flow in the home's display units. The factors are the
    storage→display multipliers from units.build_unit_context, so
    storage = display / factor. Unknown fixture/note values are dropped
    (never trusted into SQL); blanks/garbage are ignored.
    """
    out: dict = {}
    dur_min = _filter_float(raw.get("dur_min"))
    dur_max = _filter_float(raw.get("dur_max"))
    if dur_min is not None:
        out["dur_min_s"] = dur_min * 60.0
    if dur_max is not None:
        out["dur_max_s"] = dur_max * 60.0
    dp_min = _filter_float(raw.get("dp_min"))
    dp_max = _filter_float(raw.get("dp_max"))
    if dp_min is not None and pressure_factor:
        out["dp_min"] = dp_min / pressure_factor
    if dp_max is not None and pressure_factor:
        out["dp_max"] = dp_max / pressure_factor
    vol_min = _filter_float(raw.get("vol_min"))
    vol_max = _filter_float(raw.get("vol_max"))
    if vol_min is not None and vol_factor:
        out["vol_min_l"] = vol_min / vol_factor
    if vol_max is not None and vol_factor:
        out["vol_max_l"] = vol_max / vol_factor
    flow_min = _filter_float(raw.get("flow_min"))
    flow_max = _filter_float(raw.get("flow_max"))
    if flow_min is not None and flow_factor:
        out["flow_min_lpm"] = flow_min / flow_factor
    if flow_max is not None and flow_factor:
        out["flow_max_lpm"] = flow_max / flow_factor
    fixture = (raw.get("fixture") or "").strip()
    if fixture == "unlabelled" or fixture in FIXTURE_TYPE_LABELS:
        out["fixture_type"] = fixture
    note = (raw.get("note") or "").strip()
    if note in _NOTE_KIND_SQL:
        out["note_kind"] = note
    return out


def waveform_time_axis(n_bins: int, duration_s: float, src_n, src_hz):
    """Per-channel bin-centre times for a stored waveform envelope.

    ESP-sourced channel (``src_hz`` present, fixed-rate capture): the TRUE
    span is ``src_n / src_hz`` seconds, and ``t_k = (k+½)·src_n/(hz·N)``.
    Duration-scaling is wrong here — the capture window ≠ the event window.
    The capture's wall-clock start is NOT recoverable (WaveformRecord.start_ms
    is firmware millis(), received_at is monotonic; neither persisted), so the
    axis is anchored at event start with the residual offset disclosed:
    basis = ``uniform_exact_unanchored`` (exact spacing/span, approximate
    anchor).

    Software channel / rows without metadata (pre-20260801 backlog, cleared
    by the 60-day retention): the event-driven series has NO recoverable
    axis, so the fallback stretches bins uniformly across the event window:
    ``t_k = duration·(k+½)/N``, basis = ``uniform_approx``.
    """
    if n_bins <= 0:
        return [], "empty"
    if src_hz and src_n:
        span = float(src_n) / float(src_hz)
        times = [round((k + 0.5) * span / n_bins, 3) for k in range(n_bins)]
        return times, "uniform_exact_unanchored"
    dur = float(duration_s or 0)
    times = [round(dur * (k + 0.5) / n_bins, 3) for k in range(n_bins)]
    return times, "uniform_approx"


def transient_dip_note(t: dict) -> "str | None":
    """Advisory for a firmware-Failed test whose SUSTAINED drop
    (median of the monitor window's tail) sits under half the threshold: the
    terminal verdict most likely latched on a momentary dip the line
    recovered from. DISPLAY ONLY — the verdict, badge and tallies are
    untouched, and the advice is always to RE-RUN the test, never to
    dismiss it. Requires t["ui"] to be set (classify_leak_test)."""
    try:
        # dev41 (B4/C1): an indeterminate addon-side measurement must not
        # fire the note — one noisy baseline capture, an under-sampled
        # window or an open other valve can't be allowed to flip it.
        # Legacy rows (status NULL) keep the dev38 behavior.
        if t.get("addon_measure_status") == "indeterminate":
            return None
        sus = t.get("sustained_drop_psi")
        thr = t.get("threshold_psi")
        if (t.get("ui", {}).get("category") == "fail" and sus is not None
                and thr and float(sus) < float(thr) / 2.0):
            return (f"Sustained drop {float(sus):.2f} psi (below the "
                    f"{float(thr):.1f} psi threshold). A transient dip "
                    "likely triggered this result — recommend re-running "
                    "the leak test.")
    except (TypeError, ValueError):
        pass
    return None


def classify_leak_test(t: dict) -> dict:
    """Leak-test row → the verdict the page shows for it.

    ONE source of truth for the badge on each row AND the "Leak tests (last 20)"
    summary tally, so the counts cannot disagree with the badges. A non-'Passed'
    result is NOT automatically a red failure — ignored ones, aborts and manual
    stops are not.

    Follows leak_test_scheduler.py — a leak failure is 'Failed' or any 'leak'
    mention, with Passed excluded. Pre-flight skips ('Not run …', incl. the
    3-port "micro leak test not applicable" wording) short-circuit BEFORE that
    check in the scheduler, so the skip/abort prefixes must be matched FIRST and
    only then the 'leak' catch-all — otherwise the benign 3-port message reads
    as a red leak.

    ``category`` is what the summary strip counts on:
      ``pass``    — a clean test
      ``fail``    — an unexplained failure (the only red one, the only one tallied
                    as failed)
      ``ignored`` — an admin marked the failure known/benign; counted apart
      ``other``   — no verdict either way (aborted, timed out, stopped, not run)
      ``none``    — still running / no result yet
    """
    r = (t.get("result") or "")
    if not r:
        return {"category": "none", "label": "", "pill": "pill-neutral", "row": ""}
    if r.startswith("Passed"):
        return {"category": "pass", "label": "Passed",
                "pill": "pill-green", "row": "hist-row-pass"}
    if r.startswith("Failed") and t.get("draw_verdict") == "demand":
        # 3.13.2 — litres came back through the meter when the valve reopened,
        # so a fixture was running and the test measured the draw, not the
        # plumbing. Presented as an abort: retry, don't go hunting for a drip.
        return {"category": "other", "label": "Aborted — water in use",
                "pill": "pill-amber", "row": "hist-row-degraded"}
    if r.startswith("Failed") and t.get("user_dismissed"):
        # dev30 — user acknowledged this failure as benign (interrupted by an
        # update, known coincident draw): amber, not red.
        return {"category": "ignored", "label": "Failed — ignored",
                "pill": "pill-amber", "row": "hist-row-degraded"}
    if r.startswith("Failed"):
        return {"category": "fail", "label": "Failed",
                "pill": "pill-red", "row": "hist-row-fail"}
    if r.startswith("Aborted"):
        return {"category": "other", "label": "Aborted",
                "pill": "pill-amber", "row": "hist-row-degraded"}
    if r.startswith("Timed out"):
        return {"category": "other", "label": "Timed out",
                "pill": "pill-amber", "row": "hist-row-degraded"}
    if r.startswith("Not run"):
        return {"category": "other", "label": "Not run",
                "pill": "pill-neutral", "row": "hist-row-skip"}
    if r.startswith("Stopped"):
        return {"category": "other", "label": "Stopped",
                "pill": "pill-neutral", "row": "hist-row-skip"}
    if r.lower() == "cancelled":
        return {"category": "other", "label": "Cancelled",
                "pill": "pill-neutral", "row": "hist-row-skip"}
    if "leak" in r.lower():
        # Any other wording mentioning a leak (and not a pre-flight skip) → leak.
        return {"category": "fail", "label": "Failed",
                "pill": "pill-red", "row": "hist-row-fail"}
    # Unrecognized non-empty result → AMBER (caution), never benign grey.
    return {"category": "other", "label": "Unknown",
            "pill": "pill-amber", "row": "hist-row-degraded"}


def annotate_pump_overnight(tests, nights_by_date, ha_tz=None) -> None:
    """Attach the nightly pressure watch's verdict to leak-test rows whose own
    window couldn't judge the pump.

    The in-window cross-circuit check (classify_cross_circuit) needs ~3 recharge
    cycles INSIDE the test window. A normal-length test on a slow pump cannot
    hold that — this home's last confirmed period is ~8 min, so the bar is ~24
    minutes — and tests are deliberately SHORT (5–15 min): a longer isolation
    window invites false failures from occupant draws (icemaker, humidifier,
    someone up at night) and from thermal contraction (a winter test started
    after a heater cycle reads the tank+piping cooling as a leak). "Run a longer
    test" is never the answer. Instead the pump-regime detector's passive 3-hour
    nightly window — the same small hours the scheduled test runs in — closes no
    valves and is immune to decay-shaped thermal effects (it counts repeated
    recharge RISES, not slow decline). For the reassurance direction that
    evidence is strictly stronger than a short test window.

    Display-time on purpose: the same-night analysis lands ~30 min after the
    3-hour window ends — hours AFTER the 1–2 AM test wrote its verdict — so a
    stored-at-test-time fallback would permanently miss the most relevant night.

    Verdicts that DID rule in-window ('untested_side', 'quiet') are left alone:
    localization to the isolated line is exactly what the overnight watch cannot
    provide. Eligible rows get ``t['pump_overnight']`` = 'quiet' | 'active' and
    ``t['pump_overnight_night']`` = the analysis night used (the test's local
    date, else the day before — the watch whose window most recently ended).
    """
    from datetime import datetime, timedelta, timezone
    tz = ha_tz or timezone.utc
    for t in tests:
        if t.get("pump_verdict") in ("untested_side", "quiet"):
            continue
        raw = t.get("run_at")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_date = dt.astimezone(tz).date()
        for cand in (local_date, local_date - timedelta(days=1)):
            key = cand.isoformat()
            if key in nights_by_date:
                t["pump_overnight"] = ("active" if nights_by_date[key]
                                       else "quiet")
                t["pump_overnight_night"] = key
                break


def _collect_circuit_history_sync(
    db,
    circuits,
    date_from: str,
    date_to: str,
    chart_range: str,
    chart_from,
    filter_param: str,
    filter_circuit: str,
    today,
    show_hidden: bool = False,
    filter_bar_raw: dict | None = None,
) -> list[dict]:
    """Synchronous bundle of the history page's per-circuit DB work.

    Owns the per-circuit loop so the calling async handler can offload
    it via run_blocking() in one executor hop. Everything inside is
    plain sqlite3 + dict assembly — no awaits.
    """
    import json
    from ..feature_extractor import (SIGNATURE_POINTS, _wf_resample,
                                     classify_flow_shape, classify_magnitude_tier)
    from ..units import load_unit_context
    _units = load_unit_context(db)
    # Overnight pump-watch fallback for the leak-test Pump check column (see
    # annotate_pump_overnight). Home-level: fetched once for every circuit.
    try:
        from ..event_rules import get_home_timezone
        _ha_tz = get_home_timezone()
    except Exception:
        _ha_tz = None
    _nights_by_date = {n["night_date"]: bool(n["any_detected"])
                       for n in get_pump_regime_nights(db, limit=200)}
    # The "hide not-real-use events" preference, read once for all circuits. One
    # toggle hides EVERY volume-zeroing verdict — phantom, cross-talk, low-flow
    # dribble — so the single "Not real use" label maps to a single switch.
    # Display-only: their volume is already zeroed at detection, so hiding never
    # changes a total. OR of the two legacy columns (kept in lockstep by the
    # settings save) so an older profile that set only one still hides all.
    _profile = get_home_profile(db)
    hide_not_real = bool(_profile and (_profile["hide_pressure_artifact_events"]
                                       or _profile["hide_cross_talk_events"]))
    # How long a test window would need to be for the IN-WINDOW pump check to
    # rule (3 × the home's last confirmed recharge period, clamped to the
    # detector's plausible band). Shown in the "No pump verdict" tooltip so the
    # bar stops being a mystery — NOT as advice to run longer tests (tests are
    # deliberately short; see annotate_pump_overnight).
    _pump_judge_min = None
    try:
        _pp = _profile["pump_detect_period_s"] if _profile is not None else None
        if _pp:
            from ..pump_regime_math import PUMP_PERIOD_MIN_S, PUMP_PERIOD_MAX_S
            _pp = min(max(float(_pp), PUMP_PERIOD_MIN_S), PUMP_PERIOD_MAX_S)
            _pump_judge_min = int(-(-3.0 * _pp // 60.0))   # ceil to minutes
    except (IndexError, KeyError, TypeError, ValueError):
        pass
    # dev15 filter bar: convert the raw display-unit params ONCE (same units
    # for every circuit) into storage-unit pushdown kwargs.
    bar_filters = _filters_to_storage(
        filter_bar_raw or {}, _units["vol_factor"], _units["pressure_factor"],
        _units["flow_factor"])
    out: list[dict] = []
    for circuit_cfg in circuits:
        if filter_circuit and circuit_cfg.circuit != filter_circuit:
            continue
        # Dashboard "Degraded supply" / "Unusual events" links arrive as
        # ?filter=degraded / ?filter=anomaly / ?filter=anomaly_unreviewed.
        # The filter is pushed into the SQL WHERE (not a Python post-filter)
        # so the recency limit counts MATCHING events — otherwise a flagged
        # event older than the newest `limit` rows silently vanishes from the
        # very view meant to surface it. The anomaly views show every flagged
        # event, INCLUDING volume-zeroed ones — an anomaly on a zeroed event
        # is exactly the case that must not stay hidden. anomaly_unreviewed
        # (the dashboard card's Review link) further restricts to events
        # awaiting triage, matching the card's count.
        _anomaly_view = filter_param in ("anomaly", "anomaly_unreviewed")
        # Settings "Hide not-real-use events" toggle — hides every volume-zeroing
        # verdict (phantom / cross-talk / dribble) so the one "Not real use" label
        # maps to one switch. Must be pushed into the SQL WHERE, not applied as a
        # Python post-filter, so the recency limit counts VISIBLE rows: during the
        # 2026-07 pump-cycling storm ~82 of the newest 100 rows were hidden
        # artifacts and a post-filter starves the page down to ~18 events. The
        # anomaly filters bypass hiding for the must-not-vanish reason, and so
        # does the filter bar's Note = "Not real use" — the user just asked for
        # exactly those rows. ?show_hidden=1 bypasses for one render
        # (presentation-only, viewer-safe).
        # Water Use's "Label on History" link arrives as ?filter=review&circuit=…
        # The review card is rebuilt here rather than passing ids through the URL
        # so the list and the card cannot disagree, and because ids in a query
        # string go stale the moment the card is regenerated.
        _review_ids = None
        if filter_param == "review":
            from ..review_queue import build_card
            _review_ids = [it.event_id for it in
                           build_card(db, circuit_cfg.circuit).items]
        _hiding_active = (hide_not_real and not show_hidden and not _anomaly_view
                          and _review_ids is None
                          and bar_filters.get("note_kind") != "not_real")
        events = get_recent_events(
            db, circuit_cfg.circuit,
            limit=DEFAULT_EVENT_LIMIT,
            date_from=date_from or None,
            date_to=date_to or None,
            flagged_only=_anomaly_view,
            degraded_only=(filter_param == "degraded"),
            unreviewed_only=(filter_param == "anomaly_unreviewed"),
            exclude_not_real=_hiding_active,
            only_ids=_review_ids,
            **bar_filters,
        )
        # "N hidden — show them" badge: count the hidden rows within the time
        # span the visible list covers (oldest displayed row onward), so the
        # badge keeps meaning "hidden among what you're looking at".
        hidden_not_real = 0
        if _hiding_active:
            hidden_not_real = count_not_real_events(
                db, circuit_cfg.circuit,
                date_from=date_from or None,
                date_to=date_to or None,
                # Floor the count at the oldest visible row ONLY when the
                # list was actually truncated. The condition is truncation,
                # not "is there a date range": if every matching row is on
                # screen, a hidden row OLDER than the oldest visible one is
                # still inside what the user asked for, and flooring
                # undercounts it (a real 12 h window of 27 events whose oldest
                # is hidden badges 5 instead of 6).
                since_ts=(events[-1]["start_ts"]
                          if events and len(events) >= DEFAULT_EVENT_LIMIT
                          else None),
            )
        # Display-time signature upgrade: historical events store a 32-pt
        # signature, but many carry a hi-res event_waveforms envelope with real
        # pulse detail. When the envelope is finer than the stored signature,
        # rebuild a SIGNATURE_POINTS display signature from the per-bin peak
        # envelope (same normalization + onset anchor as _flow_signature).
        # Presentation-only — the stored flow_signature_json is a cluster
        # feature and is never rewritten; the flow_shape word below derives
        # from the same upgraded array the sparkline draws, so they agree.
        # Overlap-duplicate provenance: link each zeroed wrapper to the kept
        # event(s) that actually count its water (overlap_audit, dev28), so the
        # modal banner can point at where the "ignored" volume went instead of
        # reading like a loss. Display-only.
        _dup_ids = [e["id"] for e in events
                    if e.get("match_rejection_reason") == "overlap_duplicate"]
        # dev56 — the UI keys the duplicate surfaces on the REASON, not the
        # phantom bit (wrappers no longer carry it). Full vs partial matters:
        # a partial wrapper still holds real water and must not read as zeroed.
        for e in events:
            _mrr_dup = e.get("match_rejection_reason") == "overlap_duplicate"
            _veff = float(e.get("volume_litres_effective") or 0.0)
            e["is_zeroed_duplicate"] = _mrr_dup and _veff < 0.1
            e["is_partial_duplicate"] = _mrr_dup and _veff >= 0.1
        _covering: dict = {}
        if _dup_ids:
            _ph = ",".join("?" * len(_dup_ids))
            # dev38: stale audit rows (events superseded by reprocess or
            # removed by retention) are SKIPPED — their kept ids point at
            # rows that no longer exist, which previously rendered as blank
            # "covering event" chips (43 dangling wrappers on the audited DB).
            for _row in db.execute(
                    f"SELECT wrapper_event_id, kept_event_ids FROM overlap_audit "
                    f"WHERE wrapper_event_id IN ({_ph}) "
                    f"AND stale_reason IS NULL ORDER BY id", _dup_ids
                    ).fetchall():
                try:
                    _kept = json.loads(_row["kept_event_ids"] or "[]")
                except (ValueError, TypeError):
                    _kept = []
                if _kept:   # later audit rows overwrite earlier (last verdict wins)
                    _covering[_row["wrapper_event_id"]] = _kept
            _all_kept = sorted({i for ids in _covering.values() for i in ids})
            _kept_meta: dict = {}
            if _all_kept:
                _ph = ",".join("?" * len(_all_kept))
                for _row in db.execute(
                        f"SELECT id, start_ts, volume_litres_effective "
                        f"FROM events WHERE id IN ({_ph})", _all_kept).fetchall():
                    _kept_meta[_row["id"]] = _row
            for e in events:
                _kept = _covering.get(e.get("id"))
                if not _kept:
                    continue
                e["covering_json"] = json.dumps([
                    {"id": k,
                     "ts": (_kept_meta[k]["start_ts"] if k in _kept_meta else None),
                     "vol_l": (round(float(_kept_meta[k]["volume_litres_effective"] or 0.0), 2)
                               if k in _kept_meta else None)}
                    for k in _kept])
        _ids = [e["id"] for e in events if e.get("id")]
        _envelopes: dict = {}
        if _ids:
            _ph = ",".join("?" * len(_ids))
            for _row in db.execute(
                    f"SELECT event_id, flow_max_json FROM event_waveforms "
                    f"WHERE event_id IN ({_ph})", _ids).fetchall():
                _envelopes[_row["event_id"]] = _row["flow_max_json"]
        for e in events:
            try:
                _sig = json.loads(e.get("flow_signature_json") or "[]")
            except (ValueError, TypeError):
                _sig = []
            if len(_sig) >= SIGNATURE_POINTS:
                continue
            _env_json = _envelopes.get(e.get("id"))
            if not _env_json:
                continue
            try:
                _env = [float(v) for v in json.loads(_env_json)]
            except (ValueError, TypeError):
                continue
            if len(_env) <= len(_sig):
                continue        # envelope no finer than the stored signature
            _peak = max(_env)
            if _peak <= 0:
                continue
            if _env[0] > 0:
                _env = [0.0] + _env     # onset anchor, as in _flow_signature
            e["flow_signature_json"] = json.dumps([
                round(min(max(v / _peak, 0.0), 1.0), 4)
                for v in _wf_resample(_env, SIGNATURE_POINTS)
            ])
        # Display-time FLOW-shape label so the History "Shape" word matches the flow
        # sparkline it sits next to (the stored resistance_curve_shape describes the
        # ΔP/Q pressure load, which can read "pulsed" over a flat-flow rectangle).
        # Derived from the same flow_signature_json the sparkline draws.
        for e in events:
            try:
                _sig = json.loads(e.get("flow_signature_json") or "[]")
            except (ValueError, TypeError):
                _sig = []
            e["flow_shape"] = classify_flow_shape(
                _sig,
                steady_state_fraction=e.get("steady_state_fraction"),
                flow_rise_rate=e.get("flow_rise_rate_lpm_s"),
                flow_fall_rate=e.get("flow_fall_rate_lpm_s"),
                mid_event_flow_drop=e.get("mid_event_flow_drop_lpm"),
                peak=e.get("peak_flow_lpm"),
            )
            # Size tier for the sparkline's vertical scale. Use effective volume
            # so dribble/phantom/cross-talk-zeroed events fall to 'trickle' and
            # the tier agrees with the gal number shown in the row. The tier
            # blends peak flow and volume via MAX, so on a ZEROED event the
            # peak-flow dimension must be suppressed too — a phantom's peak
            # reflects pressure-window noise, not real flow, and without this
            # 246 zeroed events rendered medium/large next to "0 gal".
            _tier_vol = (e.get("volume_litres_effective")
                         if e.get("volume_litres_effective") is not None
                         else e.get("volume_litres"))
            _tier_peak = e.get("peak_flow_lpm")
            if (e.get("volume_litres_effective") is not None
                    and float(e.get("volume_litres_effective") or 0.0) < 0.1
                    and (e.get("is_pressure_restoration_phantom")
                         or e.get("is_cross_talk")
                         or e.get("is_low_flow_dribble")
                         or e.get("match_rejection_reason") == "overlap_duplicate")):
                _tier_peak = None
            e["magnitude_tier"] = classify_magnitude_tier(
                peak_flow_lpm=_tier_peak,
                volume_litres=_tier_vol,
            )
            # Embedded fixtures hidden inside this event (a toilet flushed mid-shower).
            # Display-only; the parent's volume/label are unchanged. The label is
            # built here, not by summarize_embedded, so it uses the home's display
            # units — the raw form ("toilet ×2 (~9 L)") shows litres to gallon homes.
            try:
                _emb = json.loads(e.get("embedded_fixtures_json") or "[]")
            except (ValueError, TypeError):
                _emb = []
            e["embedded_label"] = _embedded_display_label(
                _emb, _units["vol_factor"], _units["vol_unit"]) if _emb else ""
            # Homeowner-facing anomaly reason for flagged events. Splits the one
            # internal "flagged" bit into the three cases that mean different
            # things to the user: a genuinely big real draw, a flag raised on an
            # ESTIMATED number (pulsing supply — the number itself may be wrong),
            # and a large draw the phantom guard kept-but-questioned
            # (anomaly_type 'suppression_averted'). Presentation-only.
            if e.get("flagged"):
                _at = e.get("anomaly_type") or ""
                _vem = e.get("volume_estimation_method") or "raw"
                if "suppression_averted" in _at:
                    e["anomaly_reason"] = "review_draw"
                elif _vem == "pulsing_supply_envelope" or e.get("degraded_supply"):
                    e["anomaly_reason"] = "estimated"
                else:
                    e["anomaly_reason"] = "high_usage"
            else:
                e["anomaly_reason"] = ""
        leak_tests = get_leak_test_history(db, circuit_cfg.circuit, limit=20)
        # Verdict resolved server-side so the row badge and the summary tally
        # read the SAME classification (see classify_leak_test).
        for _t in leak_tests:
            _t["ui"] = classify_leak_test(_t)
            # Transient-dip advisory — see _transient_dip_advice. DISPLAY ONLY:
            # the verdict, badge and tallies are untouched.
            _t["transient_note"] = transient_dip_note(_t)
        annotate_pump_overnight(leak_tests, _nights_by_date, _ha_tz)
        summaries  = get_daily_summaries(
            db, circuit_cfg.circuit,
            date_from=chart_from,
        )

        # For YoY: also fetch prior-year summaries (shifted 365 days).
        prior_summaries = []
        if chart_range == "yoy" and chart_from:
            from datetime import date as _date, timedelta as _td
            prior_from = (_date.fromisoformat(chart_from)
                          - _td(days=365)).isoformat()
            prior_to   = (today - _td(days=365)).isoformat()
            prior_summaries = get_daily_summaries(
                db, circuit_cfg.circuit,
                date_from=prior_from,
                date_to=prior_to,
            )

        # Hourly-volume fallback: aggregate to daily for chart when no
        # summaries exist yet (first-day install).
        hv_daily: dict = {}
        if not summaries:
            # Bucketed in Python, not `date(hour_ts)`: hour_ts is UTC, and the
            # fallback has to land on the same LOCAL day boundary the real
            # summaries use, or the first-day chart disagrees with every chart
            # that follows it.
            hv_rows = db.execute("""
                SELECT hour_ts, volume_litres
                FROM hourly_volume
                WHERE circuit = ?
                  AND (? IS NULL OR hour_ts >= ?)
                ORDER BY hour_ts ASC
            """, (circuit_cfg.circuit, chart_from, chart_from)).fetchall()
            for _r in hv_rows:
                _d = local_day_of(_r["hour_ts"])
                if _d:
                    hv_daily[_d] = hv_daily.get(_d, 0.0) + (_r["volume_litres"] or 0.0)

        # dev15 filter-bar slider bounds — per-circuit maxima in DISPLAY units
        # (minutes / display-ΔP / display-volume), ceil'd to whole numbers so
        # the sliders get stable, friendly tops. The page aggregates across
        # circuits (one shared bar filters every circuit's list).
        import math as _math
        _b = db.execute(
            "SELECT MAX(duration_seconds) AS d, "
            "       MAX(COALESCE(pressure_delta_psi, 0)) AS p, "
            "       MAX(COALESCE(volume_litres_effective, volume_litres, 0)) AS v, "
            "       MAX(COALESCE(true_avg_flow_lpm, avg_flow_lpm, 0)) AS f "
            "FROM events WHERE circuit = ?", (circuit_cfg.circuit,)).fetchone()
        filter_bounds = {
            "dur_max": max(1, _math.ceil(float(_b["d"] or 0.0) / 60.0)),
            "dp_max":  max(1, _math.ceil(
                float(_b["p"] or 0.0) * _units["pressure_factor"])),
            "vol_max": max(1, _math.ceil(
                float(_b["v"] or 0.0) * _units["vol_factor"])),
            "flow_max": max(1, _math.ceil(
                float(_b["f"] or 0.0) * _units["flow_factor"])),
        }

        out.append({
            "circuit":         circuit_cfg.circuit,
            "display_name":    circuit_cfg.label,
            "events":          events,
            "leak_tests":      leak_tests,
            "pump_judge_min":  _pump_judge_min,
            "event_count":     len(events),
            "hidden_not_real": hidden_not_real,
            "summaries":       summaries,
            "prior_summaries": prior_summaries,
            "hv_daily":        hv_daily,
            "filter_bounds":   filter_bounds,
        })
    return out


async def _history_page(request: Request):
    orch = _orch(request)
    cfg  = orch._cfg

    date_from   = request.query_params.get("from", "").strip()
    date_to     = request.query_params.get("to",   "").strip()
    chart_range = request.query_params.get("range", "30d")
    # Optional filters surfaced by the Dashboard "Degraded supply" banner
    # link. `filter=degraded` restricts the per-circuit events list to
    # degraded-supply rows; `circuit=...` further scopes to one circuit.
    filter_param  = request.query_params.get("filter",  "").strip().lower()
    filter_circuit = resolve_circuit(request.query_params.get("circuit", "").strip())
    # "N hidden — show them" escape hatch for the Settings hide toggle.
    show_hidden = request.query_params.get("show_hidden", "") == "1"
    # dev15 filter bar — raw display-unit values; validated/converted in the
    # sync collector (_filters_to_storage). Kept raw here for the form echo.
    filter_bar_raw = {k: request.query_params.get(k, "").strip()
                      for k in FILTER_BAR_PARAMS}
    filters_active = any(filter_bar_raw.values())
    # Canonical query-string fragment ("&dur_min=2&fixture=toilet…") appended to
    # self-links that must preserve the active filters (show-hidden link etc.).
    from urllib.parse import urlencode
    filter_qs = urlencode({k: v for k, v in filter_bar_raw.items() if v})
    filter_qs = ("&" + filter_qs) if filter_qs else ""
    # 30d | 6m | 1y | all | monthly | yearly | yoy
    using_range = bool(date_from or date_to)

    from datetime import datetime as _dt, timedelta as td, timezone as _tz
    # The home's today, not the container's. daily_summary rows are keyed on the
    # local day, so a UTC-derived window edge would clip or pad the chart by a
    # day for the six hours either side of local midnight.
    from ..event_rules import get_home_timezone
    today = _dt.now(get_home_timezone() or _tz.utc).date()
    chart_from_map = {
        "30d":    (today - td(days=30)).isoformat(),
        "6m":     (today - td(days=183)).isoformat(),
        "1y":     (today - td(days=365)).isoformat(),
        "all":    None,
        "monthly": today.replace(day=1).isoformat(),
        "yearly":  today.replace(month=1, day=1).isoformat(),
        "yoy":    (today - td(days=730)).isoformat(),
    }
    chart_from = chart_from_map.get(chart_range, chart_from_map["30d"])

    # All per-circuit DB work (recent events with full columns, leak test
    # history, daily summaries, optional YoY summaries, hourly volume fallback)
    # runs in a single thread-pool hop rather than blocking the event loop per
    # query — on a 2-circuit deployment with a year of events, the difference
    # between a ~150 ms dashboard-fight and a clean async hand-off.
    circuit_history = await run_blocking(
        _collect_circuit_history_sync,
        orch.db,
        list(cfg.circuits),
        date_from, date_to, chart_range, chart_from,
        filter_param, filter_circuit,
        today,
        show_hidden,
        filter_bar_raw,
    )

    fixture_type_options = [
        {"value": k, "label": FIXTURE_TYPE_LABELS.get(k, k.replace("_", " ").title())}
        for k in user_selectable_types()
    ]

    # One shared filter bar drives every circuit's list → slider tops are the
    # max across circuits (each ch carries its own per-circuit bounds).
    filter_bounds = {
        "dur_max": max((ch["filter_bounds"]["dur_max"]
                        for ch in circuit_history), default=1),
        "dp_max":  max((ch["filter_bounds"]["dp_max"]
                        for ch in circuit_history), default=1),
        "vol_max": max((ch["filter_bounds"]["vol_max"]
                        for ch in circuit_history), default=1),
        "flow_max": max((ch["filter_bounds"]["flow_max"]
                         for ch in circuit_history), default=1),
    }

    return _tmpl(request).TemplateResponse("history.html", {
        "request":              request,
        "circuit_history":      circuit_history,
        "page":                 "history",
        "date_from":            date_from,
        "date_to":              date_to,
        "using_range":          using_range,
        "default_limit":        DEFAULT_EVENT_LIMIT,
        "chart_range":          chart_range,
        "fixture_type_options": fixture_type_options,
        "fixture_type_labels":  FIXTURE_TYPE_LABELS,
        "filter_param":         filter_param,
        "filter_circuit":       filter_circuit,
        "show_hidden":          show_hidden,
        "filter_bar":           filter_bar_raw,
        "filters_active":       filters_active,
        "filter_qs":            filter_qs,
        "filter_bounds":        filter_bounds,
    })


@router.get("/api/event/{event_id}/waveform")
async def event_waveform(event_id: str, request: Request):
    """Return the high-resolution min/max waveform for one event.

    Source: the `event_waveforms` table, populated by feature_extractor's
    `_persist_waveform` after each new event is processed. Historical events
    captured before this migration won't have a waveform row — clients get
    a 404 and should fall back to the existing 32-point signature display.
    """
    import json as _json
    orch = _orch(request)
    # dev46 (46a): waveform rows are the largest BLOBs in the schema and this
    # fires on every modal open — never query it inline from the loop thread.
    row = await run_db(
        lambda: orch.db.execute(
            """SELECT w.flow_min_json, w.flow_max_json,
                      w.pressure_min_json, w.pressure_max_json,
                      w.duration_seconds,
                      w.flow_src_n, w.press_src_n, w.flow_src_hz, w.press_src_hz,
                      e.start_ts, e.degraded_supply
               FROM event_waveforms w
               JOIN events e ON w.event_id = e.id
               WHERE w.event_id = ?""",
            (event_id,),
        ).fetchone())
    if not row:
        return JSONResponse({"error": "waveform not found"}, status_code=404)
    try:
        flow_max = _json.loads(row["flow_max_json"])
        press_max = _json.loads(row["pressure_max_json"])
        dur = float(row["duration_seconds"] or 0)
        times_flow, flow_basis = waveform_time_axis(
            len(flow_max), dur, row["flow_src_n"], row["flow_src_hz"])
        times_press, press_basis = waveform_time_axis(
            len(press_max), dur, row["press_src_n"], row["press_src_hz"])
        return JSONResponse({
            "event_id":         event_id,
            "start_ts":         row["start_ts"],
            "duration_seconds": row["duration_seconds"],
            "degraded_supply":  bool(row["degraded_supply"]),
            "flow_min":         _json.loads(row["flow_min_json"]),
            "flow_max":         flow_max,
            "pressure_min":     _json.loads(row["pressure_min_json"]),
            "pressure_max":     press_max,
            # dev38 — per-channel time axes. The two channels are binned from
            # DIFFERENT source streams (an ESP flow capture can pair with the
            # ~5 s HA pressure series in one row), so index i of one array is
            # NOT the same instant as index i of the other; the audit found
            # 18.2% of events visibly misaligned on the shared index axis.
            "times_flow":       times_flow,
            "times_pressure":   times_press,
            "flow_time_basis":  flow_basis,
            "pressure_time_basis": press_basis,
        })
    except (ValueError, TypeError) as e:
        log.warning("Waveform JSON decode failed for %s: %s", event_id, e)
        return JSONResponse({"error": "waveform corrupted"}, status_code=500)


@router.get("/api/events/{circuit}")
async def events_api(
    circuit: str,
    request: Request,
    limit: int = DEFAULT_EVENT_LIMIT,
    date_from: str = "",
    date_to: str = "",
):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    # This route has zero callers inside the add-on, but may have external ones
    # (an HA automation, Node-RED flow, curl habit), so it is hardened rather
    # than removed.
    #
    # `limit` arrives straight from the query string with no ceiling, and every
    # DB call runs on the single serialized executor — so one ?limit=999999
    # stalls page renders process-wide for every open tab.
    # NOT `int(limit or DEFAULT_EVENT_LIMIT)`: 0 is falsy, so `or` turns an
    # explicit ?limit=0 back into 100 — the same falsy-default slip that puts a
    # stored 0 back to 2 on the shutoff cap. FastAPI already supplies the default
    # when the parameter is absent, so the only job here is the range.
    limit = max(1, min(int(limit), MAX_EVENT_LIMIT))
    # A malformed bound used to be an inert string comparison. Now that it
    # is parsed into a UTC window it has to be checked, and checked HERE —
    # the DB layer drops what it cannot parse, which silently widens the
    # result set rather than telling the caller their filter is wrong.
    for name, raw in (("date_from", date_from), ("date_to", date_to)):
        if not raw:
            continue
        try:
            datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(
                {"error": "bad_date",
                 "message": f"{name} must be a real date as YYYY-MM-DD"},
                status_code=400)
    events = await run_db(                                    # dev46 (46a)
        get_recent_events, orch.db, circuit,
        limit=limit,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return JSONResponse(events)


@router.patch("/api/events/{circuit}/{event_id}")
async def patch_event_api(circuit: str, event_id: str, request: Request):
    """Update user-editable fields on a single event.

    Accepted payload keys (all optional):
      user_fixture_type (str | null) — assign or clear a fixture type label.
      excluded_from_training (bool)  — ignore / restore the event.
    """
    payload = await request.json()
    db = _orch(request).db

    # Validate fixture type before touching the DB
    if "user_fixture_type" in payload:
        ftype = payload["user_fixture_type"] or None
        if ftype is not None and ftype not in _VALID_USER_FIXTURE_TYPES:
            return JSONResponse(
                {"error": f"Invalid fixture type: {ftype!r}"},
                status_code=400,
            )

    # ONE hop for the whole write path. This handler is the PATCH that races the
    # chunked startup reclassify (see the write-time re-check in
    # set_event_matched_fixture_type): it must never touch the shared connection
    # from the loop thread while the DB worker holds it.
    #
    # A label must never lose to background maintenance — labelling is the one
    # activity that unfreezes the learning loop, and the hourly backlog drain
    # holds the file write-lock in short bursts. Retry a few times with backoff,
    # awaiting between attempts so the DB worker and the event loop stay free;
    # the 503 is the last resort, not the first response. The sync step is
    # idempotent (a plain UPDATE of user fields).
    outcome = await _patch_with_retry(db, event_id, circuit, payload)
    if "error" in outcome:
        return JSONResponse(outcome["error"], status_code=outcome["status"])
    if outcome.get("reclassify"):
        # asyncio.create_task needs the running loop — stays out of run_db.
        _schedule_reclassify(circuit)
    return JSONResponse(outcome["ok"])


def _patch_event_sync(db, event_id: str, circuit: str, payload: dict) -> dict:
    """Every DB touch of the event PATCH, in one DB-thread callable.

    Returns either ``{"error": <json>, "status": <code>}`` for an early exit
    or ``{"ok": <json>, "reclassify": <bool>}``; the caller owns the
    HTTP response and the background-task scheduling (which needs the loop).

    Self-contained transaction-wise: every write and its commit happen inside
    this one call.
    """
    propagation_meta: dict = {}
    signature_meta: dict = {}
    classification_meta = None

    # Manual classification (independent checkboxes). Authoritative over
    # auto-detection; a Phantom mark zeroes volume from totals. A manual save
    # (incl. all-false = "normal") always locks; reset:true returns the event to
    # automatic detection. Processed alongside — not instead of — a fixture-type
    # change, so the single Save button persists both in one request. The client
    # only sends `classification` when the user actually changed a checkbox, so
    # labelling alone never locks it.
    if "classification" in payload:
        action, data = classify_action(payload["classification"])
        if action == "error":
            return {"error": {"error": data["msg"]}, "status": 400}
        if action == "reset":
            ok = clear_event_classification(db, event_id, circuit)
        else:
            ok = set_event_classification(
                db, event_id, circuit,
                phantom=data["phantom"],
                supply_pressure=data["supply_pressure"],
                dribble=data["dribble"],
                cross_talk=data["cross_talk"],
            )
        if not ok:
            return {"error": {"error": "Event not found"}, "status": 404}
        classification_meta = {"action": action, "classification": data}

    kwargs: dict = {}
    if "user_fixture_type" in payload:
        kwargs["user_fixture_type"] = payload["user_fixture_type"] or None
    if "excluded_from_training" in payload:
        # Ignore/Restore — route to the explicit user_ignored intent (Sprint H);
        # patch_event re-derives effective excluded_from_training.
        kwargs["user_ignored"] = bool(payload["excluded_from_training"])
    if "training_excluded_by_user" in payload:
        # dev46 (46f): "keep my label, but don't learn from this event."
        # Annotate-don't-modify — never touches the label, verdict or volume,
        # and is NOT cleared by a later relabel (review is what sets it).
        kwargs["training_excluded_by_user"] = bool(
            payload["training_excluded_by_user"])
    if "user_reviewed" in payload:
        # Anomaly triage: mark a flagged event as looked-at (dashboard count).
        kwargs["user_reviewed"] = bool(payload["user_reviewed"])
    if "review_verdict" in payload:
        # Two-option triage: 'normal' (confirmed legitimate use) or 'unknown'
        # (looked, didn't recognise it — held out of baseline refits). Either
        # verdict implies user_reviewed=1; null clears the verdict.
        verdict = payload["review_verdict"] or None
        if verdict not in (None, "normal", "unknown"):
            return {"error": {"error": "invalid review_verdict"}, "status": 400}
        kwargs["review_verdict"] = verdict

    # B7 — guard against accidentally EXCLUDING a sparse 'training' anchor: that
    # silently unseeds the class from the k-NN, and in History the event looks
    # ordinary. Warn (single confirm) only on this harmful action — relabel and
    # excluding a 'cycle'/'user' event are unaffected.
    if payload.get("excluded_from_training") and not payload.get("confirm_exclude"):
        srow = db.execute(
            "SELECT fixture_label_source, user_fixture_type FROM events "
            "WHERE id = ? AND circuit = ?", (event_id, circuit)).fetchone()
        if srow and srow["fixture_label_source"] == "training":
            ftype = srow["user_fixture_type"] or "this fixture"
            others = db.execute(
                "SELECT COUNT(*) FROM events WHERE circuit = ? "
                "  AND fixture_label_source = 'training' AND user_fixture_type = ? "
                "  AND id <> ? AND COALESCE(excluded_from_training, 0) = 0",
                (circuit, srow["user_fixture_type"], event_id)).fetchone()[0]
            scope = "the only" if others == 0 else "one of several"
            return {"error": {
                "needs_confirm": "exclude_training",
                "message": (f"This event was captured during setup and is {scope} "
                            f"training example for {ftype}. Excluding it may leave "
                            f"that fixture unclassified. Exclude anyway?"),
            }, "status": 200}

    if kwargs:
        # A locked DB is a transient state (a background job writing, or a
        # wedged writer — one held the lock for 27 min on 2026-08-03), not a
        # server fault: answer 503 with words the user can act on rather than a
        # raw 500 traceback, and feed the stuck-writer detector so a persistent
        # holder names itself in the log.
        import sqlite3 as _sqlite3

        try:
            found = _patch_event(db, event_id, circuit, **kwargs)
        except _sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            note_locked_write("history.patch_event")
            return {"error": {
                "error": "The database is busy with a background job — "
                         "your change was NOT saved. Try again in a minute; "
                         "if this keeps happening, restart the add-on."},
                "status": 503, "locked": True}
        if not found:
            return {"error": {"error": "Event not found"}, "status": 404}

    # A real fixture label = "confirmed real water" — it must also undo an
    # automatic irrigation cross-talk zeroing, restoring the event's volume
    # (the UI frames these flags as "relabel if wrong", so relabeling IS the
    # recovery path). revert_irrigation_cross_talk self-guards: it only touches
    # rows the auto pass flagged (never a user-classified cross-talk) and
    # audits the revert.
    # ORDERING: this must run AFTER the classification block above — a fresh
    # fixture label is the newer, more specific user intent and must win over a
    # checkbox verdict saved in the same request. revert_artifact_zeroing_
    # on_relabel covers every zeroing verdict (dribble/below-meter-floor, the
    # phantom family, cross-talk); revert_irrigation_cross_talk stays for its
    # audit-row bookkeeping on the auto-flagged irrigation rows it owns.
    if kwargs.get("user_fixture_type"):
        try:
            if revert_irrigation_cross_talk(db, event_id, circuit):
                log.info("[%s] relabel to %r reverted auto cross-talk on %s",
                         circuit, kwargs["user_fixture_type"], event_id)
            elif revert_artifact_zeroing_on_relabel(db, event_id, circuit):
                log.info("[%s] relabel to %r restored zeroed volume on %s",
                         circuit, kwargs["user_fixture_type"], event_id)
        except Exception:
            log.exception("artifact revert on relabel failed (label saved)")

    # Sprint B — if the patch touched user_fixture_type, propagate the
    # signal into the cluster's suggested_type (soft hint; never silently
    # links a cluster to a fixture). When the event has no cluster_id we
    # have nowhere to land the signal — log it so the gap is visible.
    reclassify = False
    if "user_fixture_type" in payload:
        try:
            row = db.execute(
                "SELECT cluster_id, excluded_from_training "
                "FROM events WHERE id = ? AND circuit = ?",
                (event_id, circuit),
            ).fetchone()
            cid = row["cluster_id"] if row else None
            if cid is not None:
                result = recompute_cluster_suggestion_from_user_labels(
                    db, circuit, int(cid)
                )
                if result is not None:
                    propagation_meta["cluster_id"] = int(cid)
                    propagation_meta["suggested_type"] = result["suggested_type"]
                    propagation_meta["labelled_member_count"] = \
                        result["labelled_member_count"]
                    propagation_meta["total_label_count"] = \
                        result["total_label_count"]
            elif row and not row["excluded_from_training"]:
                # Event eligible for clustering but has no cluster_id yet (the
                # preliminary-match-without-commit gap). The label IS recorded and
                # trains the k-NN classifier — only the legacy cluster
                # bulk-propagation waits for the next backfill. DEBUG, not INFO:
                # a known low-impact gap that would otherwise fire on every label
                # of an un-clustered event.
                log.debug(
                    "[%s] event %s labelled with cluster_id NULL — label recorded and "
                    "trains the classifier; cluster propagation follows once it is "
                    "clustered (next backfill)",
                    circuit, event_id,
                )

            # Sprint C — refresh the fixture_type signature for whichever
            # type(s) this patch touched. We refresh the NEW type and,
            # if the user changed an existing label, also the OLD type
            # (otherwise the old signature keeps a stale event member).
            old_type = payload.get("_previous_user_fixture_type")
            new_type = payload.get("user_fixture_type") or None
            for sig_type in {t for t in (new_type, old_type) if t}:
                sig = upsert_fixture_signature(db, circuit, sig_type)
                if sig is not None:
                    signature_meta[sig_type] = {
                        "member_count": sig["member_count"],
                    }

            # 2a — auto-label this appliance event's cycle-mates (source='cycle')
            # so one label seeds several. No-op for non-appliance types. Commit so
            # the cycle labels are durable + visible to the History reload and the
            # background reclassify below.
            cycle_propagated = propagate_cycle_label(db, circuit, event_id, new_type)
            if cycle_propagated:
                db.commit()
                propagation_meta["cycle_propagated"] = cycle_propagated

            # The label changed the fingerprint library too — drop the live
            # matcher's cache so the next event sees the new label at once
            # (reclassify below loads its own fresh library anyway).
            from ..fingerprint_matcher import invalidate_library_cache
            invalidate_library_cache(circuit)

            # Re-run the label-trained k-NN over unlabelled events so the new label
            # spreads + stale matched_fixture_type clears. Offloaded to the background
            # AND debounced (the user's own label + cycle-mates are already applied
            # synchronously above) so a burst of labels doesn't stack reclassifies and
            # starve the next save's write lock. The caller schedules it — the
            # debounce task needs the event loop, which this thread doesn't have.
            reclassify = True
        except Exception as e:
            # Propagation is best-effort. A failure here must not block
            # the label save the user already saw succeed.
            log.warning(
                "[%s] propagate user label for event %s failed: %s",
                circuit, event_id, e,
            )

    return {
        "ok": {
            "ok": True,
            "propagation": propagation_meta,
            "signatures": signature_meta,
            "classification": classification_meta,
        },
        "reclassify": reclassify,
    }


@router.post("/api/events/{circuit}/undo_auto_cycle")
async def undo_auto_cycle_api(circuit: str, request: Request):
    """Bulk-undo the auto 'cycle' labels around an anchor event — clears every
    `source='cycle'` label within ±45 min of it (the cycle), then refreshes the
    k-NN in the background. Never touches 'user'/'training'/legacy labels."""
    from datetime import timedelta
    db = _orch(request).db
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    event_id = (payload or {}).get("event_id")
    if not event_id:
        return JSONResponse({"error": "event_id required"}, status_code=400)

    def _undo_sync():
        # dev46 (46a): the anchor lookup, the id sweep and the clear are one
        # DB-thread callable — never inline on the loop.
        anchor = db.execute(
            "SELECT start_ts, cycle_group_id FROM events "
            "WHERE id = ? AND circuit = ?",
            (event_id, circuit),
        ).fetchone()
        if anchor is None:
            return {"error": {"error": "Event not found"}, "status": 404}
        gid = anchor["cycle_group_id"]
        if gid:
            # dev.24 — precise: clear by the persisted cycle-group key (the
            # rollup grouping), so exactly this cycle's auto labels are undone.
            ids = [r["id"] for r in db.execute(
                "SELECT id FROM events WHERE circuit = ? AND cycle_group_id = ? "
                "  AND fixture_label_source = 'cycle'",
                (circuit, gid),
            ).fetchall()]
        else:
            # Legacy fallback — pre-dev.24 cycle labels have no group id, so
            # undo by the original ±45-min proximity window around the anchor.
            a_ts = _parse_event_ts(anchor["start_ts"])
            if a_ts is None:
                return {"error": {"error": "Event has no usable timestamp"},
                        "status": 400}
            lo = (a_ts - timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
            hi = (a_ts + timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
            ids = [r["id"] for r in db.execute(
                "SELECT id FROM events WHERE circuit = ? "
                "  AND fixture_label_source = 'cycle' "
                "  AND start_ts BETWEEN ? AND ?",
                (circuit, lo, hi),
            ).fetchall()]
        return {"cleared": clear_auto_labels(db, circuit, event_ids=ids)
                if ids else 0}

    outcome = await run_db(_undo_sync)
    if "error" in outcome:
        return JSONResponse(outcome["error"], status_code=outcome["status"])
    cleared = outcome["cleared"]
    if cleared:
        _schedule_reclassify(circuit)
    return JSONResponse({"ok": True, "cleared": cleared})


@router.post("/api/leak-tests/{test_id}/dismiss")
async def dismiss_leak_test(test_id: int, request: Request):
    """dev30 — toggle a failed leak test's user-dismissed flag. Display-only
    acknowledgement ("I know why this failed — update interrupted it, someone
    ran water"): the row renders amber instead of red; the record itself is
    never altered and future tests are unaffected."""
    db = _orch(request).db

    def _toggle_sync():
        # dev46 (46a/N2a): read-modify-write + commit in ONE callable, so the
        # toggle's transaction can't be split across a queue boundary.
        row = db.execute(
            "SELECT user_dismissed FROM leak_test_history WHERE id = ?",
            (test_id,)).fetchone()
        if row is None:
            return None
        new_val = 0 if row["user_dismissed"] else 1
        db.execute(
            "UPDATE leak_test_history SET user_dismissed = ? WHERE id = ?",
            (new_val, test_id))
        db.commit()
        return new_val

    new_val = await run_db(_toggle_sync)
    if new_val is None:
        return JSONResponse({"status": "error", "error": "not_found"},
                            status_code=404)
    log.info("leak test %d %s by user", test_id,
             "dismissed" if new_val else "un-dismissed")
    return ingress_redirect(request, "/history")


# dev.50 — plain-language text for each reprocess.  _probe_refusal reason. Every one of
# these means the SAME thing operationally: nothing was deleted and the event is
# untouched, because HA's history could not prove it can rebuild the water.
_REPROCESS_REFUSAL_TEXT = {
    "nothing_to_do":
        "There are no automatic events in this window to rebuild.",
    "fetch_failed":
        "Couldn't read Home Assistant's history just now — nothing was changed. "
        "Try again shortly.",
    "no_history":
        "Home Assistant no longer keeps flow history this far back, so this event "
        "can't be rebuilt. Nothing was changed — the event is still here.",
    "gappy_history":
        "Home Assistant's history has gaps across this event (a restart or a sensor "
        "dropout), so a rebuild would read the gap as no-flow and lose water. "
        "Nothing was changed.",
    "volume_unaccounted":
        "The history that's left only accounts for part of this event's water, so a "
        "rebuild would under-count it. Nothing was changed.",
    # dev52 — the KEPT rows, not HA history, refuse the rebuild.
    "blocked_by_kept_events":
        "This span already holds an event you labelled or marked yourself, and the "
        "rebuilt draw would land on top of it. Your labelled events are always kept, "
        "so there is nothing here to rebuild. Nothing was changed. To rebuild this "
        "span, clear that label first. (Labels the app's own cycle detectors "
        "applied do not block a rebuild — only yours do.)",
    "kept_events_underfit":
        "This span holds an event you labelled yourself, but the rebuilt draw here is "
        "larger than what that label covers. Deleting the recorded event would lose "
        "the difference, so nothing was changed. To rebuild this span, widen or clear "
        "that label first.",
}


@router.post("/api/events/{circuit}/{event_id}/reprocess")
async def reprocess_event_api(circuit: str, event_id: str, request: Request):
    """Rebuild ONE event from HA history.

    Deletes this event (and any overlapping purely-machine-derived events),
    reversing their volume, then re-imports the event's own span — optionally
    padded by ``buffer_minutes`` — so a garbled/unclosed event (e.g. an irrigation
    run that failed to close and absorbed a whole day) becomes the real runs. The
    window comes from the event's authoritative ``start_ts``/``end_ts``, so the
    user never has to guess which day a long event began. User-labelled / classified
    / ignored events are preserved (``delete_events_in_range`` skips them); the
    re-import deliberately does not move the catch-up checkpoint.
    """
    from datetime import timedelta, timezone
    from ..reprocess import reprocess_window
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    db = orch.db
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        buffer_min = int((payload or {}).get("buffer_minutes", 0) or 0)
    except (TypeError, ValueError):
        buffer_min = 0
    buffer_min = max(0, min(buffer_min, 240))            # clamp 0..4 h

    row = await run_db(                                       # dev46 (46a)
        lambda: db.execute(
            "SELECT start_ts, end_ts, user_fixture_type, user_classified, "
            "       user_ignored FROM events WHERE id = ? AND circuit = ?",
            (event_id, circuit),
        ).fetchone())
    if row is None:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    # delete_events_in_range PRESERVES anything the user has touched, so a
    # reprocess of such an event deletes nothing and rebuilds nothing. Returning
    # deleted=0/imported=0 renders as "✓ Deleted 0, re-importing 0 run(s)" —
    # indistinguishable from success — so say so explicitly. The modal also hides
    # this control for such events, but an API caller must not get the hollow
    # success either.
    if (row["user_fixture_type"] or row["user_classified"]
            or row["user_ignored"]):
        return JSONResponse(
            {"error": "This event has your own label or classification on it, so it "
                      "is never rebuilt — that is what keeps your labels safe. Clear "
                      "the fixture type (set it to “— no assignment —”) and save, "
                      "then reprocess."},
            status_code=409)
    start = _parse_event_ts(row["start_ts"])
    if start is None:
        return JSONResponse({"error": "Event has no usable timestamp"},
                            status_code=400)
    end = _parse_event_ts(row["end_ts"]) or start
    # Normalise to aware UTC so the widen math (compute_widened_window) compares
    # like with like — stored strings are +00:00 but guard a stray naive value.
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    buf = timedelta(minutes=buffer_min)
    from_dt, to_dt = start - buf, end + buf

    try:
        result = await reprocess_window(orch, circuit, from_dt, to_dt)
    except Exception as e:
        log.error("[%s] reprocess_event %s failed: %s", circuit, event_id, e,
                  exc_info=True)
        return JSONResponse({"error": "Reprocess failed — see addon log."},
                            status_code=500)
    if result.get("busy"):
        return JSONResponse(
            {"error": "Another volume operation is running — try again shortly."},
            status_code=409)
    # dev.50 — the probe refused BEFORE anything was deleted. Each reason is a real
    # answer, not a failure: the event is still there, intact, and rebuilding it from
    # the history that exists would have lost water.
    refused = result.get("refused")
    if refused:
        return JSONResponse({"error": _REPROCESS_REFUSAL_TEXT.get(
            refused, "This event can't be rebuilt from Home Assistant history right "
                     "now. Nothing was changed.")}, status_code=409)
    return JSONResponse({"ok": True, **result})
