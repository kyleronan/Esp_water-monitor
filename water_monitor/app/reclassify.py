"""The reclassify pass: verdict stamps, what releases them, and the
prepare / chunked row loop / finalize driver (sync and async entry points).

``database`` must not depend on this module at import time: it imports lazily
inside functions and re-exports the moved names through
``database.__getattr__`` (PEP 562). An eager ``from .reclassify import ...``
there closes the cycle and raises ``ImportError: ... partially initialized
module`` when this module is imported first.

Siblings are reached through the MODULE object (``_db.run_db``), never
``from .database import run_db``: a from-import snapshots the object, so the
tests' ``monkeypatch.setattr(database, ...)`` would bind an attribute nobody
reads.

``log`` is database.py's own logger, not ``getLogger(__name__)``: a renamed
channel silently empties every
``caplog.at_level(logger="water_monitor.app.database")`` watching this pass
while leaving those tests green.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from . import database as _db
from .config import pump_gates_active as _pga

# Same channel as before the split — see the module docstring.
log = _db.log


# Bump when the STAMP'S OWN definition changes (a new input folded in, a
# component computed differently). Every stamp then differs from every stored
# one and the next pass re-derives everything.
_VERDICT_STAMP_ALGO = 3

# Force a full unstamped pass if the last one is older than this. The stamp can
# only be as complete as the input list baked into it; this is what stops an
# omission from meaning "stale forever" (see the migration's docstring).
_VERDICT_STAMP_MAX_AGE_DAYS = 7



def compute_verdict_stamp(conn: sqlite3.Connection, circuit: str) -> str:
    """Fingerprint of everything that can change an unlabelled event's verdict.

    Same stamp ⇒ the classifier would store the same answer, so the event is
    skipped. Inputs: the classifier code version (a deploy can change any
    rule, veto or tier, and it is the safety net for any input not listed
    here — staleness cannot outlive a release) and the rule bands of EVERY
    regime (an event is judged by its era's bands, so a re-fit in any regime
    can move verdicts; one row per regime).

    Deliberately NOT here: the label pool — one new label invalidated all
    ~5,400 events (labelling 3 re-derived 5,417 verdicts in 85 s and moved
    zero), so a label instead PUSHES an invalidation to its own cluster via
    ``invalidate_cluster_verdict_stamps``, cost proportional to the change;
    and per-event features, which a global stamp cannot express — the
    invalidation trigger releases those. ``_VERDICT_STAMP_MAX_AGE_DAYS`` bounds
    both omissions: a cluster is an imperfect similarity proxy (DBSTREAM label
    purity 0.387), so a label CAN reach an event in another cluster, and the
    weekly unfiltered pass catches it.
    """
    import hashlib

    h = hashlib.sha256()
    h.update(f"algo={_VERDICT_STAMP_ALGO};circuit={circuit};".encode())

    h.update(f"code={_db._code_fingerprint()};".encode())

    try:
        # Hash the fitted bands THEMSELVES (params), not just updated_at: a
        # re-fit that lands identical values should NOT invalidate, and a band
        # edited without touching the timestamp MUST. The JSON is the truth.
        for r in conn.execute(
                "SELECT regime_id, params, locked_at FROM rule_calibration "
                "WHERE circuit = ? ORDER BY regime_id", (circuit,)):
            h.update(f"|band:{r[0]}:{r[1]}:{r[2]}".encode())
    except sqlite3.OperationalError:        # pre-migration schema
        h.update(b"|band:unavailable")

    # home_profile inputs the pass reads, enumerated column by column (the
    # invalidation trigger's inputs-only rule), never `SELECT *`: away_mode
    # flips with presence and updated_at moves on any write, which would
    # re-stamp the whole table several times a day. The softener columns feed
    # the session detector, fingerprint_labeling_enabled gates the fingerprint
    # tier, build_year / epa_flush_cap_enabled feed the toilet flush cap veto,
    # and daily_summary_tz the time-of-day rule predicates.
    try:
        row = conn.execute(
            "SELECT has_water_softener, softener_circuit, "
            "       fingerprint_labeling_enabled, build_year, "
            "       epa_flush_cap_enabled, daily_summary_tz "
            "FROM home_profile WHERE id = 1").fetchone()
        h.update(f"|home:{tuple(row) if row else ()}".encode())
    except sqlite3.OperationalError:
        h.update(b"|home:unavailable")

    # circuit_type picks the rule set; winterized suspends the circuit.
    try:
        row = conn.execute(
            "SELECT circuit_type, winterized FROM circuit_profile "
            "WHERE circuit = ?", (circuit,)).fetchone()
        h.update(f"|circuit:{tuple(row) if row else ()}".encode())
    except sqlite3.OperationalError:
        h.update(b"|circuit:unavailable")

    # Regime SPANS, because an event is judged by the bands of the era its
    # start_ts falls in — closing or moving a regime re-points events at a
    # different band set even when no band value changed. Metadata columns
    # (detected_at, note, ...) are deliberately excluded.
    try:
        for r in conn.execute(
                "SELECT id, started_at, ended_at FROM supply_regime "
                "ORDER BY id"):
            h.update(f"|regime:{r[0]}:{r[1]}:{r[2]}".encode())
    except sqlite3.OperationalError:
        h.update(b"|regime:unavailable")

    return h.hexdigest()[:32]


def invalidate_cluster_verdict_stamps(conn: sqlite3.Connection, circuit: str,
                                      event_id: str) -> int:
    """A new label PUSHES a re-check to its cluster — the events a new exemplar
    can plausibly move — rather than releasing the whole circuit, keeping the
    work proportional to the change.

    The labelled event itself needs no releasing (the pass only visits
    ``user_fixture_type IS NULL``). Returns the number of peers released; zero
    is normal — no cluster means no known neighbours — and the weekly
    unfiltered pass is the backstop for anything this misses.
    """
    row = conn.execute(
        "SELECT cluster_id FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit)).fetchone()
    if row is None or row[0] is None:
        return 0
    # Narrow further by TIER. Rule bands are fit-once and hard-locked and an
    # event's features are immutable, so a peer already claimed by a rule (or
    # by a cycle/session detector, equally rule-driven and above the label
    # tiers) returns the identical verdict no matter how many labels exist.
    # Everything else — an abstention, a fingerprint hit, a k-NN hit — was
    # decided by evidence a new exemplar can move, so it is released.
    cur = conn.execute(
        "UPDATE events SET verdict_stamp = NULL "
        "WHERE circuit = ? AND cluster_id = ? AND user_fixture_type IS NULL "
        "  AND verdict_stamp IS NOT NULL "
        # LIKE 'rule%' rather than a list: the tier emits rule_toilet,
        # rule_shower, rule_dishwasher and gains more as rules are added, and
        # an enumeration would silently stop covering the new ones.
        "  AND (matched_via IS NULL OR (matched_via NOT LIKE 'rule%' "
        "       AND matched_via NOT IN ('softener_session','washer_cycle',"
        "                               'dishwasher_cycle')))",
        (circuit, row[0]))
    return cur.rowcount or 0


def release_settle_window(conn: sqlite3.Connection, circuit: str,
                          hours: int) -> int:
    """Re-open the last ``hours`` of events for one pass — boot's catch-up.

    maturity_recheck re-evaluates recent events hourly because cycle context
    arrives after an event closes; hours the add-on was off got no re-check,
    and the stamp cannot tell (those rows were stamped when first classified
    and look settled). Releasing the window unconditionally costs a handful of
    rows (the hourly pass logs 6-26). Events the add-on missed entirely arrive
    unstamped as new rows via the historical importer.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    cur = conn.execute(
        "UPDATE events SET verdict_stamp = NULL "
        "WHERE circuit = ? AND start_ts >= ? AND user_fixture_type IS NULL "
        "  AND verdict_stamp IS NOT NULL",
        (circuit, cutoff))
    return cur.rowcount or 0


def _verdict_stamp_pass_is_due(conn: sqlite3.Connection, circuit: str) -> bool:
    """True when the stamp filter must be BYPASSED and everything re-derived.

    The stamp is only as complete as the input list baked into it. If an input
    is ever missed, affected verdicts stay stale silently. This bounds that
    failure to ``_VERDICT_STAMP_MAX_AGE_DAYS`` instead of forever, at a cost of
    one background full pass a week.

    Unreadable state returns True: fail towards the expensive-but-correct path.
    """
    # The stamp is only trustworthy while the invalidation trigger exists to
    # release rows whose own inputs changed. Without it a stamped row keeps a
    # verdict derived from features it no longer has and nothing says so, so
    # the skip refuses to engage rather than run unguarded.
    try:
        if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",
                ("trg_events_verdict_stamp_invalidate",)).fetchone() is None:
            log.warning("[%s] verdict-stamp trigger missing — re-deriving every "
                        "unlabelled event (the skip stays off until it exists)",
                        circuit)
            return True
    except sqlite3.OperationalError:
        return True

    try:
        row = conn.execute(
            "SELECT last_full_reclassify_at FROM training_state WHERE circuit = ?",
            (circuit,)).fetchone()
    except sqlite3.OperationalError:        # pre-migration schema
        return True
    if row is None or not row[0]:
        return True
    last = _db._parse_event_ts(str(row[0]))
    if last is None:
        return True
    now = datetime.now(timezone.utc)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (now - last).total_seconds() > _VERDICT_STAMP_MAX_AGE_DAYS * 86400


def _mark_full_reclassify(conn: sqlite3.Connection, circuit: str) -> None:
    """Record that an unfiltered pass just completed (max-age backstop)."""
    try:
        conn.execute(
            "UPDATE training_state SET last_full_reclassify_at = ? "
            "WHERE circuit = ?",
            (datetime.now(timezone.utc).isoformat(), circuit))
    except sqlite3.OperationalError:        # pre-migration schema
        pass


def invalidate_verdict_stamps(conn: sqlite3.Connection, event_ids) -> int:
    """Mark these events as needing re-classification.

    Call from ANY path that rewrites a feature the classifier reads (cycle-pulse
    recount, volume recompute, waveform repair, exclusion-verdict changes): the
    global stamp cannot see per-row edits, so an unreleased row keeps a verdict
    derived from features it no longer has. Clearing is always safe — worst
    case one needless re-derive; NOT clearing is the unsafe direction.
    """
    ids = [e for e in (event_ids or []) if e]
    if not ids:
        return 0
    n = 0
    for i in range(0, len(ids), 400):       # keep well under SQLITE_MAX_VARIABLE
        chunk = ids[i:i + 400]
        cur = conn.execute(
            "UPDATE events SET verdict_stamp = NULL WHERE id IN "
            "(" + ",".join("?" * len(chunk)) + ")", chunk)
        n += cur.rowcount or 0
    return n


def _new_reclassify_counters() -> Dict[str, Any]:
    """Fresh accumulator bundle for a reclassify pass.

    The chunked pass's loop-carried state lives in this one dict threaded
    through every batch; it is all that crosses a row boundary. ``changed``
    counts rows whose verdict actually DIFFERED from the stored one — the
    number that decides whether a ~145 s boot pass earned its cost; the others
    report throughput, which hid a second boot re-deriving 5,417 identical
    answers nine minutes after the first.
    """
    return {"scanned": 0, "matched": 0, "rule_matched": 0,
            "softener_matched": 0, "cleared": 0, "abstained": 0,
            "changed": 0, "veto_counts": {},
            # Per-tier buckets: anything that was not softener or plain k-NN
            # used to be logged as "via rules", hiding TinyModel, fingerprint
            # and k-NN-invariant hits in the one line that says which tier did
            # the work.
            "tinymodel_matched": 0, "fingerprint_matched": 0, "knn_matched": 0}


def _match_bucket(via: Optional[str]) -> str:
    """Which reclassify counter a matched verdict belongs to, by tier."""
    if via == "softener_session":
        return "softener_matched"
    if via == "tinymodel":
        return "tinymodel_matched"
    if via == "fingerprint":
        return "fingerprint_matched"
    if via in ("knn", "knn_invariant"):
        return "knn_matched"
    return "rule_matched"          # rule_*, washer_cycle, dishwasher_cycle, composite


# The backlog slice: NULL-stamp rows first and newest-first (someone is
# waiting on those); the stale-stamp group OLDEST-first so the historical tail
# drains front-to-back instead of being starved by fresh invalidations. A
# constant so the ordering rule is unit-testable without the whole context.
_BACKLOG_ORDER_BY = ("(verdict_stamp IS NOT NULL), "
                     "CASE WHEN verdict_stamp IS NULL THEN start_ts END DESC, "
                     "start_ts ASC")


def _forced_reopen_allowed(since_ts: Optional[str]) -> bool:
    """The periodic full re-open may fire only from an UNWINDOWED pass (boot,
    the hourly backlog slice) — never from the hourly settle-window call, which
    would re-open a whole circuit from inside a 6-hour window."""
    return since_ts is None


# How often the SYNC reclassify path commits so the SQLite file write-lock is
# released. The default cadence (300 rows ≈ 26 s at ~87 ms/row) exceeds a user
# save's 5 s busy timeout, so a label landing mid-slice was refused. ~40 rows
# ≈ 3-4 s keeps every hold under that timeout.
_SYNC_YIELD_EVERY_ROWS: int = 40


# How many events one pass may re-derive. The backlog left by a global change
# (a new build, a rules re-fit) drains a slice at a time via the hourly pass
# rather than in one burst at boot, when the operator wants to look at the
# add-on. ~400 costs a few seconds and drains 5,400 events overnight.
#
# Deliberately released rows (NULL stamp — new events, the settle window, a
# label's cluster push) are prioritised inside this budget.
_VERDICT_BACKLOG_PER_PASS: int = 400

# The chunk budget, in SECONDS: how long one run_db call may hold the single DB
# worker before a queued page render notices. ~1 s is the threshold at which a
# wait stops being distinguishable from a normal request.
_CHUNK_TARGET_SECONDS: float = 1.0
# First chunk of a pass, before there is a measurement to aim with. Small on
# purpose: guessing low costs one extra round-trip, guessing high costs the
# operator a visible stall on the boot they are watching.
_CHUNK_START_ROWS: int = 25
# Below this, chunk overhead (a commit and a round-trip each) dominates on a
# slow event.
_CHUNK_MIN_ROWS: int = 5


def _reclassify_prepare(conn: sqlite3.Connection, circuit: str, ha_tz=None,
                        since_ts: Optional[str] = None,
                        backlog_limit: Optional[int] = None):
    """Everything a reclassify pass computes ONCE, plus its candidate rows.

    The expensive-but-bounded half: signature training, the per-regime calib
    cache, the whole-circuit cycle detectors (washer / softener / dishwasher),
    usage baselines, the fingerprint library and the toilet cap. All of it is
    READ-ONLY for the row loop, which is what makes batching safe.

    Returns ``(ctx, rows)``.
    """
    # 1. Retrain the per-type centroids for the Signatures UI display. The
    #    k-NN reads events directly, so this only keeps that page in sync.
    signatures_trained = 0
    type_rows = conn.execute(
        "SELECT DISTINCT user_fixture_type FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND user_fixture_type <> ''",
        (circuit,),
    ).fetchall()
    for tr in type_rows:
        if _db.upsert_fixture_signature(conn, circuit, tr[0]) is not None:
            signatures_trained += 1

    # 2. Backfill over unlabelled events. The query carries BOTH the legacy and
    #    the active-flow features so the matcher uses whichever it can. An event
    #    now excluded_from_training carries no fixture identity, so its
    #    matched_fixture_type is cleared.
    from .event_rules import (detect_dishwasher_cycles,
                              detect_softener_sessions, detect_washer_cycles,
                              get_home_timezone, parse_hhmm_to_minutes)
    from .rule_calibration import load_rule_calibration

    # Frozen per-home rule bands, fitted per SUPPLY REGIME (empty dict →
    # shipped defaults). The per-event rule tier resolves each event's calib by
    # its start_ts, so a pre-pump event is judged by pre-pump bands; the
    # window-scanning cycle detectors take ONE calib per pass, the CURRENT
    # regime's, so a full reprocess spanning a regime boundary scans historical
    # cycles with current bands (known limitation).
    from .supply_regime import get_current_regime_id, get_regimes
    _regimes = get_regimes(conn)
    _calib_cache: Dict[int, Dict[str, Any]] = {
        0: load_rule_calibration(conn, circuit)}
    for _rg in _regimes:
        _calib_cache[int(_rg["id"])] = load_rule_calibration(
            conn, circuit, regime_id=int(_rg["id"]))
    calib = _calib_cache.get(get_current_regime_id(conn), _calib_cache[0])
    qfeats = tuple(dict.fromkeys(
        _db._SIGNATURE_MATCH_FEATURES + _db._SIGNATURE_KNN_ACTIVE_FEATURES
        # The regime-invariant rung's shape features (the rest of its set is
        # already covered by the tuples above).
        + _db._SIGNATURE_KNN_INVARIANT_FEATURES
        + ("has_pressure_transient",)     # the flush predicate's extra input
        # The edge tier reads these off the query dict (expanded inside
        # match_event_to_signature_knn); harmless extras for the rules tier.
        + ("onset_signature_json", "offset_signature_json")))
    circuit_type = _db.get_circuit_type(conn, circuit)
    # Windowed (periodic maturity re-check): bound the per-event k-NN row loop
    # to events >= since_ts, but give the detectors a lookback (>= the
    # softener's max session span) so a cycle straddling the window start is
    # still seen WHOLE — otherwise its in-window members are wrongly retracted.
    # since_ts=None → full circuit (startup / manual reprocess).
    detector_since = since_ts
    if since_ts is not None:
        _s = _db._parse_event_ts(since_ts)
        if _s is not None:
            detector_since = (_s - timedelta(hours=4)).isoformat()
    # One O(n) pass for the whole circuit — the per-row loop then does dict
    # lookups, never per-event window queries.
    washer_ids = (detect_washer_cycles(conn, circuit, since_ts=detector_since,
                                       calib=calib)
                  if circuit_type != "zone" else {})
    # Water-softener sessions (hard-gated: enabled AND this circuit).
    softener_ids: Dict[str, Any] = {}
    prof = _db.get_home_profile(conn)
    if (prof is not None and prof["has_water_softener"]
            and (prof["softener_circuit"] or "main") == circuit):
        band = parse_hhmm_to_minutes(prof["softener_regen_start"])
        if band is not None:
            tz = ha_tz if ha_tz is not None else get_home_timezone()
            softener_ids = detect_softener_sessions(conn, circuit, band,
                                                    since_ts=detector_since, tz=tz,
                                                    calib=calib)
    # Dishwasher cycles: a chain of gentle small fills the per-event cycle-pulse
    # rule misses. Exclude ids the washer/softener detectors already claimed so
    # a brine chain / laundry top-off can't be re-read as a dishwasher.
    dishwasher_ids = (
        detect_dishwasher_cycles(conn, circuit, since_ts=detector_since, calib=calib,
                                 exclude_ids=set(washer_ids) | set(softener_ids))
        if circuit_type != "zone" else {})
    # Re-score each scanned event against the FROZEN baseline (storage only;
    # reclassify NEVER notifies or shuts off). Baseline + sensitivity loaded
    # once for the whole pass; the extra SELECT columns the scorer needs are
    # deduped into the query so a column already in qfeats isn't selected twice.
    from .anomaly_baseline import load_usage_baselines
    _baselines = load_usage_baselines(conn, circuit)
    _sens = _db.get_sensitivity_config(conn, circuit)
    _SCORE_COLS = ("volume_litres_effective", "volume_litres", "duration_seconds",
                   "peak_flow_lpm", "is_pressure_restoration_phantom", "is_cross_talk",
                   "is_low_flow_dribble", "user_ignored",
                   "phantom_suppression_averted")

    # Fingerprint tier library — built ONCE per run, fresh (no cache; a
    # reclassify usually follows a label change). Gated by the home_profile
    # toggle; any failure just disables the tier for this run.
    _fp_library = None
    try:
        _fp_row = conn.execute(
            "SELECT fingerprint_labeling_enabled FROM home_profile WHERE id = 1"
        ).fetchone()
        _fp_enabled = bool(_fp_row["fingerprint_labeling_enabled"]) if _fp_row else True
    except sqlite3.OperationalError:
        _fp_enabled = True   # column mid-migration → schema default is ON
    if _fp_enabled:
        try:
            from .fingerprint_matcher import FingerprintLibrary
            _fp_library = FingerprintLibrary.load(conn, circuit)
        except Exception as e:  # noqa: BLE001 — tier is optional, never fatal
            log.warning("[%s] fingerprint library unavailable: %s", circuit, e)

    # Toilet physics veto — cap computed once for the whole pass.
    _toilet_cap = _db.get_toilet_flush_cap_litres(conn)

    where = "WHERE circuit = ? AND user_fixture_type IS NULL"
    qparams: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        qparams.append(since_ts)

    # Skip events whose verdict provably cannot have changed. A row is a
    # candidate when its stamp is NULL (new, or explicitly released by
    # invalidate_verdict_stamps) or differs from the current one (code, bands
    # or labels moved). Everything else already holds the answer this pass
    # would compute, so skipping is loss-free by construction and every
    # uncertain case falls on the recompute side.
    stamp = compute_verdict_stamp(conn, circuit)
    # The weekly backstop INVALIDATES rather than bypassing the filter: every
    # pass is a slice, and with the filter off the rows a sweep re-derives stay
    # candidates, so it re-does the same slice forever; marking completion
    # after one slice would cover 400 of ~5,500 rows and restart the clock —
    # the silent omission it exists to prevent. Cleared stamps make the sweep
    # ordinary work, and the clock restarts HERE because those rows cannot be
    # skipped again until re-stamped. Unwindowed callers only (boot, the
    # hourly backlog slice): from the settle-window pass this would re-open
    # ~5,000 rows at once, ~12 h to re-drain at 400/hr with the write lock
    # held throughout.
    forced = _forced_reopen_allowed(since_ts) and _verdict_stamp_pass_is_due(conn, circuit)
    if forced:
        n = conn.execute(
            "UPDATE events SET verdict_stamp = NULL "
            "WHERE circuit = ? AND user_fixture_type IS NULL "
            "  AND verdict_stamp IS NOT NULL", (circuit,)).rowcount or 0
        conn.commit()
        _mark_full_reclassify(conn, circuit)
        log.info("[%s] reclassify: periodic full re-derive due — re-opened %d "
                 "event(s); they drain at the normal slice rate", circuit, n)
    where += (" AND (verdict_stamp IS NULL OR verdict_stamp <> ?)")
    qparams.append(stamp)

    # An event younger than the settle horizon is NOT DECIDED YET and must not
    # be stamped: cycle context arrives AFTER an event closes (a dishwasher's
    # third fill is what claims the first), and a stamped young event would be
    # skipped by the hourly maturity re-check for the rest of the window.
    # Leaving them unstamped is also the offline catch-up — an event that ages
    # past the horizon while the add-on is down is picked up by the next pass.
    try:
        from .maturity_recheck import _SETTLE_HORIZON_HOURS as _settle_h
    except Exception:                       # noqa: BLE001 — never block a pass
        _settle_h = 6
    stamp_before_ts = (datetime.now(timezone.utc)
                       - timedelta(hours=_settle_h)).isoformat()
    select_cols = list(dict.fromkeys(
        ("id", "start_ts", "matched_fixture_type", "matched_via",
         "cycle_group_id", "excluded_from_training",
         "match_rejection_reason",       # abstention marker mark/retract
         "match_confidence",             # so an unchanged model verdict is not
                                         # rewritten every pass
         "active_flow_segment_count")
        + qfeats + _SCORE_COLS))
    # With a budget, take the highest-priority slice (``_BACKLOG_ORDER_BY``:
    # released NULL-stamp rows first, newest-first, because recent events are
    # what the operator opens) and leave the rest for the next pass. The
    # subquery picks the slice; the outer ORDER BY start_ts keeps the row
    # loop's ascending order, so batching changes WHICH rows a pass sees, never
    # how it processes them.
    backlog_remaining = 0
    if backlog_limit is not None and backlog_limit > 0:
        total = conn.execute("SELECT COUNT(*) FROM events " + where,
                             qparams).fetchone()[0]
        backlog_remaining = max(0, total - backlog_limit)
        # Two queues, two directions. NULL-stamp rows (new, settling, freed by
        # a label) go first and NEWEST-first. The STALE-stamp group — events a
        # new build or a promotion re-opened — drains OLDEST-first: newest-first
        # there lets a steady trickle of fresh invalidations push the historical
        # tail to the back of every slice (~1,041 July events went unexamined
        # for weeks, never reaching the front of the budget).
        rows = conn.execute(
            "SELECT " + ", ".join(select_cols) + " FROM events "
            "WHERE id IN (SELECT id FROM events " + where + " "
            "             ORDER BY " + _BACKLOG_ORDER_BY + " "
            "             LIMIT ?) "
            "ORDER BY start_ts",
            qparams + [backlog_limit],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT " + ", ".join(select_cols) + " "
            "FROM events "
            + where + " "
            "ORDER BY start_ts",
            qparams,
        ).fetchall()
    return ({
        "signatures_trained": signatures_trained,
        "softener_ids":       softener_ids,
        "washer_ids":         washer_ids,
        "dishwasher_ids":     dishwasher_ids,
        "qfeats":             qfeats,
        "circuit_type":       circuit_type,
        "calib":              calib,
        "calib_cache":        _calib_cache,
        "regimes":            _regimes,
        "fp_library":         _fp_library,
        "toilet_cap":         _toilet_cap,
        "baselines":          _baselines,
        "sens":               _sens,
        "score_cols":         _SCORE_COLS,
        # The stamp every row this pass touches gets written with, and whether
        # a due full pass re-opened the circuit.
        "verdict_stamp":      stamp,
        "forced_full":        forced,
        "stamp_before_ts":    stamp_before_ts,
        "backlog_remaining":  backlog_remaining,
    }, rows)


def _reclassify_chunk_sync(conn: sqlite3.Connection, circuit: str, rows: list,
                           ctx: Dict[str, Any],
                           counters: Dict[str, Any],
                           yield_lock: bool = False) -> None:
    """One batch of the reclassify row loop — runs on the single DB thread.

    Every statement for the chunk plus its commit happens inside this one
    callable, so no foreign statement lands in an open transaction: chunk
    boundary = transaction boundary = where a queued page render interleaves.
    ``counters`` is mutated in place. The write-time ``user_fixture_type IS
    NULL`` guard inside ``set_event_matched_fixture_type`` is what makes an
    interleaved user relabel safe here — do not weaken it.
    """
    from .event_rules import (CYCLE_ONLY_FIXTURE_TYPES, rule_classify_event,
                             toilet_burst_veto_reason,
                              toilet_veto_reason)
    from .supply_regime import resolve_regime_for_ts
    from .anomaly_baseline import score_event_anomaly

    softener_ids   = ctx["softener_ids"]
    washer_ids     = ctx["washer_ids"]
    dishwasher_ids = ctx["dishwasher_ids"]
    qfeats         = ctx["qfeats"]
    circuit_type   = ctx["circuit_type"]
    calib          = ctx["calib"]
    _calib_cache   = ctx["calib_cache"]
    _regimes       = ctx["regimes"]
    _fp_library    = ctx["fp_library"]
    _toilet_cap    = ctx["toilet_cap"]
    _baselines     = ctx["baselines"]
    _sens          = ctx["sens"]
    _SCORE_COLS    = ctx["score_cols"]
    _stamp         = ctx["verdict_stamp"]
    _stamp_before  = ctx["stamp_before_ts"]

    scanned          = counters["scanned"]
    matched          = counters["matched"]
    rule_matched     = counters["rule_matched"]
    softener_matched = counters["softener_matched"]
    tinymodel_matched   = counters.get("tinymodel_matched", 0)
    fingerprint_matched = counters.get("fingerprint_matched", 0)
    knn_matched         = counters.get("knn_matched", 0)
    cleared          = counters["cleared"]
    abstained        = counters["abstained"]
    changed          = counters["changed"]
    veto_counts      = counters["veto_counts"]
    # The model tier's confidence for the current row. Assigned in the TinyModel
    # branch; the writer only consumes it when the verdict's provenance is
    # 'tinymodel', so a value can never leak onto a row another tier claimed.
    new_conf = None
    # Burst context for the toilet rule's appliance veto. Computed ONCE for the
    # whole chunk: compute_for_events does a single windowed read over the id
    # set, so hoisting it out of the loop is one query instead of one per event.
    # MATURE because in a batch pass every sibling is already in the table.
    _burst_ctx = {}
    try:
        from . import burst_features as _bf
        _burst_ctx = _bf.compute_for_events(
            conn, circuit, [r["id"] for r in rows], config=_bf.CONFIG_MATURE)
    except Exception as _bf_exc:                       # noqa: BLE001
        # No burst context means the veto abstains — never a reason to fail a
        # reclassify.
        log.debug("burst context unavailable in reclassify: %s", _bf_exc)
    for r in rows:
        scanned += 1
        new_group = None
        if r["id"] in softener_ids:
            # Softener is checked BEFORE the excluded gate — a deliberate
            # exception to "excluded → no identity": regen consumption is real,
            # and matched_* is written separately from the volume verdict, so a
            # dribble-flagged regen pulse keeps its verdict AND reads
            # water_softener.
            _role, new_group = softener_ids[r["id"]]
            new_type, new_via = "water_softener", "softener_session"
        elif r["excluded_from_training"]:
            new_type, new_via = None, None   # artifacts carry no fixture identity
        elif r["id"] in washer_ids:
            new_type, new_via = "washing_machine", "washer_cycle"
            new_group = washer_ids[r["id"]][1]
        elif r["id"] in dishwasher_ids:
            new_type, new_via = "dishwasher", "dishwasher_cycle"
            new_group = dishwasher_ids[r["id"]][1]
        else:
            feats = {f: r[f] for f in qfeats}
            try:
                _pump = _pga(conn, circuit)
            except Exception:
                _pump = False
            _ev_calib = (_calib_cache.get(
                resolve_regime_for_ts(_regimes, r["start_ts"]), calib)
                if _regimes else calib)
            _ev_burst = _burst_ctx.get(r["id"])
            rule_hit = rule_classify_event(feats, circuit_type, calib=_ev_calib,
                                           pump_mode=_pump, burst=_ev_burst)
            if rule_hit is not None:
                new_type, new_via = rule_hit
            else:
                # DEBUG per event, one INFO summary at the end. The rule returns
                # None either way, so without this a veto that fires and a veto
                # that never fires look identical from outside.
                _bw = toilet_burst_veto_reason(feats, _ev_calib, _pump, _ev_burst)
                if _bw:
                    log.debug("[%s] event %s: toilet match vetoed by burst "
                              "context — %s (vol=%s L)", circuit, r["id"], _bw,
                              r["volume_litres"])
                    _key = "in an appliance burst"
                    veto_counts[_key] = veto_counts.get(_key, 0) + 1
                # TinyModel tier, in the same ladder position the live path
                # uses (anchors -> model -> fingerprint/k-NN). The batch pass
                # uses MATURE burst features: every event's siblings are already
                # in the table, which the live pass cannot have.
                _tm_hit = None
                try:
                    from . import tinymodel as _tm
                    _tm_hit = _tm.classify(conn, circuit, r["id"], feats,
                                           burst_config=_tm.bf.CONFIG_MATURE)
                except Exception as _tm_exc:
                    log.debug("tinymodel tier unavailable in reclassify: %s",
                              _tm_exc)
                # Fingerprint tier — whole-waveform NN against USER-labeled
                # events, tight-threshold only. Sits between the structural
                # rules and the scalar k-NN: stronger evidence than a scalar
                # vote, weaker than the cycle/session context above.
                if _tm_hit is not None:
                    # The model claimed it; the tiers below are its fallback,
                    # not its reviewers.
                    new_type = _db._canonical_fixture_type(_tm_hit[0])
                    new_via = "tinymodel" if new_type is not None else None
                    # Carry the model's confidence to the row, as the live path
                    # does. Only this tier has one.
                    new_conf = float(_tm_hit[1]) if new_type is not None else None
                    fp_hit = None
                else:
                    fp_hit = None
                    if _fp_library is not None:
                        from .fingerprint_matcher import match_event_fingerprint
                        try:
                            fp_hit = match_event_fingerprint(
                                conn, circuit, r["id"], library=_fp_library)
                        except Exception as e:  # noqa: BLE001 — never break reclassify
                            log.debug("[%s] fingerprint match failed for %s: %s",
                                      circuit, r["id"], e)
                if _tm_hit is not None:
                    pass                      # decided above
                elif fp_hit is not None:
                    new_type = _db._canonical_fixture_type(fp_hit["fixture_type"])
                    new_via = "fingerprint" if new_type is not None else None
                else:
                    hit = _db.match_event_to_signature_knn(conn, circuit, feats)
                    new_type = _db._canonical_fixture_type(hit["fixture_type"]) if hit else None
                    # Multi-fill appliances need cycle context (washer_cycle / dishwasher
                    # rule, both checked above) — a lone k-NN signature must not stamp them.
                    # (The fingerprint tier MAY name them: whole-waveform evidence at the
                    # tight threshold, enforced inside FingerprintLibrary.match.)
                    if new_type in CYCLE_ONLY_FIXTURE_TYPES:
                        new_type = None
                    # Distinguish the regime-invariant rung so its contribution
                    # is measurable (and separable) in the data.
                    new_via = (
                        None if new_type is None
                        else "knn_invariant"
                        if hit.get("match_source") == "invariant_features"
                        else "knn")
            # Toilet physics veto: whatever tier proposed 'toilet' (rule /
            # fingerprint / k-NN), the event must be physically able to BE a
            # flush. Vetoed → abstain (never re-guess another type).
            if new_type == "toilet":
                vfeats = dict(feats)
                vfeats["active_flow_segment_count"] = r["active_flow_segment_count"]
                why = toilet_veto_reason(vfeats, _toilet_cap)
                if why:
                    # DEBUG per event, one INFO summary at the end: ~60 of
                    # these fire per reclassify and they are the veto WORKING.
                    # The recurring 2.2–2.8 L band is the labelled dishwasher's
                    # upper fill-pulse tail (9 user labels in-band, 0 of them
                    # toilet, every event has a neighbour within 30 min), so the
                    # floor is what keeps appliance pulses from being named
                    # flushes.
                    log.debug("[%s] event %s: toilet match (%s) vetoed by "
                              "flush physics — %s (vol=%s L)", circuit,
                              r["id"], new_via, why, r["volume_litres"])
                    veto_counts[why.split(" (")[0]] = (
                        veto_counts.get(why.split(" (")[0], 0) + 1)
                    new_type, new_via = None, None
        prev = r["matched_fixture_type"]
        _prev_conf = r["match_confidence"] if "match_confidence" in r.keys() else None
        _conf = new_conf if new_via == "tinymodel" else None
        if (new_type, new_via, new_group, _conf) != (
                prev, r["matched_via"], r["cycle_group_id"], _prev_conf):
            _db.set_event_matched_fixture_type(conn, circuit, r["id"], new_type,
                                           via=new_via, cycle_group_id=new_group,
                                           confidence=_conf)
            # This branch IS the pass's real output — the only way to see how
            # much of the boot pass was necessary.
            changed += 1
            if new_type is None and prev is not None:
                cleared += 1
        # Stamp EVERY examined row, agreed-with verdicts included, or it returns
        # as a candidate on every boot forever — but only once it is past the
        # settle horizon (a younger row keeps NULL so the hourly maturity
        # re-check keeps re-evaluating it), and only while still unlabelled:
        # the same guard as the verdict write, so a PATCH landing mid-pass is
        # not stamped from premises that no longer hold.
        if r["start_ts"] and r["start_ts"] < _stamp_before:
            conn.execute(
                "UPDATE events SET verdict_stamp = ? "
                "WHERE id = ? AND user_fixture_type IS NULL",
                (_stamp, r["id"]))
        # Mark / retract classification-tier abstention so a silent outage is
        # measurable and a recovery is visible. Only ever fills an EMPTY reason
        # (artifact + cluster reasons are more specific), and is retracted the
        # moment any tier names the event.
        from .feature_extractor import NO_TIER_MATCHED_REASON as _NTM
        _mrr = r["match_rejection_reason"] if "match_rejection_reason" in r.keys() \
            else None
        if new_type is None and not _mrr and not r["excluded_from_training"]:
            conn.execute(
                "UPDATE events SET match_rejection_reason = ? "
                "WHERE id = ? AND match_rejection_reason IS NULL", (_NTM, r["id"]))
        elif new_type is not None and _mrr == _NTM:
            conn.execute(
                "UPDATE events SET match_rejection_reason = NULL WHERE id = ?",
                (r["id"],))
        if new_type is not None:
            matched += 1
            _b = _match_bucket(new_via)
            if _b == "softener_matched":
                softener_matched += 1
            elif _b == "tinymodel_matched":
                tinymodel_matched += 1
            elif _b == "fingerprint_matched":
                fingerprint_matched += 1
            elif _b == "knn_matched":
                knn_matched += 1
            else:
                rule_matched += 1
        else:
            abstained += 1
        # Re-score against the frozen baseline + persist (storage only — no
        # notify / shut-off from a backfill). flagged=1 marks a genuine
        # (non-artifact) anomaly.
        sfeats = {c: r[c] for c in _SCORE_COLS}
        sfeats["matched_fixture_type"] = new_type
        av = score_event_anomaly(sfeats, _baselines, _sens)
        conn.execute(
            "UPDATE events SET anomaly_score = ?, anomaly_type = ?, flagged = ? "
            "WHERE id = ?",
            (av.get("score"), av.get("anomaly_type"),
             1 if av.get("is_anomalous") else 0, r["id"]),
        )
        # Through run_db the chunk IS the transaction and the single commit
        # below is correct — yielding would sleep 30 ms holding the single DB
        # worker. The SYNC entry point instead runs inside run_isolated_write
        # (maturity_recheck, reprocess, training_manager) on a PRIVATE
        # connection whose contract is per-row commits releasing the file
        # write-lock; without the yield one long pass holds the SQLite write
        # lock throughout and every concurrent user save gets "database is
        # locked".
        if yield_lock:
            _db.yield_write_lock(conn, scanned, every=_SYNC_YIELD_EVERY_ROWS)
    conn.commit()
    counters.update(scanned=scanned, matched=matched,
                    rule_matched=rule_matched,
                    softener_matched=softener_matched,
                    tinymodel_matched=tinymodel_matched,
                    fingerprint_matched=fingerprint_matched,
                    knn_matched=knn_matched,
                    cleared=cleared, abstained=abstained, changed=changed)


def _reclassify_finalize(conn: sqlite3.Connection, circuit: str,
                         since_ts: Optional[str], ctx: Dict[str, Any],
                         counters: Dict[str, Any]) -> Dict[str, Any]:
    """Composite annotation + result assembly, after every batch has run."""
    signatures_trained = ctx["signatures_trained"]
    scanned          = counters["scanned"]
    matched          = counters["matched"]
    rule_matched     = counters["rule_matched"]
    softener_matched = counters["softener_matched"]
    cleared          = counters["cleared"]
    abstained        = counters["abstained"]
    changed          = counters["changed"]
    veto_counts      = counters["veto_counts"]
    # ── Composite annotation ─────────────────────────────────────────────────
    # Annotate sustained events with fixtures hidden inside them (a toilet
    # flushed mid-shower), then upgrade events that abstained but clearly
    # contain a second draw from "(none)" to "other"/composite. Wrapped so a
    # waveform/JSON hiccup cannot undo the classification committed above.
    embedded_annotated = embedded_other = 0
    try:
        emb = _db.recompute_embedded_fixtures(conn, circuit, since_ts=since_ts)
        embedded_annotated = emb["annotated"]
    except Exception:                       # pragma: no cover - defensive
        log.exception("[%s] embedded-fixture annotation failed (classification "
                      "already committed)", circuit)
    # Re-promote abstained events hiding a second draw (ANY embedded kind) to
    # "other"/composite from the STORED annotation: the loop above just cleared
    # them to NULL and the embedded scan is incremental. Its own try — a
    # waveform hiccup in the scan must not also skip this, or prior 'other'
    # events flicker to NULL until the next reclassify. Decided from the PARSED
    # JSON, not a LIKE, so it can't drift from the detector's serialization.
    try:
        embedded_other = _db.promote_embedded_composites(conn, circuit,
                                                     since_ts=since_ts)
        conn.commit()
    except Exception:                       # pragma: no cover - defensive
        log.exception("[%s] composite re-promotion failed (classification "
                      "already committed)", circuit)
    result = {
        "signatures_trained": signatures_trained,
        "events_scanned": scanned,
        "events_matched": matched,
        "events_rule_matched": rule_matched,
        "events_softener_matched": softener_matched,
        "events_tinymodel_matched": counters.get("tinymodel_matched", 0),
        "events_fingerprint_matched": counters.get("fingerprint_matched", 0),
        "events_knn_matched": counters.get("knn_matched", 0),
        "events_cleared": cleared,
        "events_abstained": abstained,
        # Rows whose verdict actually differed from the stored one. The pass's
        # only real output; everything above is throughput.
        "events_changed": changed,
        # What this pass deliberately did NOT do. A capped pass that reports
        # only what it processed reads as "everything is done".
        "events_backlog_remaining": ctx.get("backlog_remaining") or 0,
        "events_embedded_annotated": embedded_annotated,
        "events_composite_other": embedded_other,
    }
    log.info(
        "[%s] reclassify: trained %d signature(s); scanned %d unlabelled "
        "event(s) → %d matched (%d via rules, %d model, %d fingerprint, %d k-NN, "
        "%d softener), %d abstained (%d stale cleared) — %d verdict(s) CHANGED, "
        "%d re-promoted composite",
        circuit, signatures_trained, scanned, matched, rule_matched,
        counters.get("tinymodel_matched", 0), counters.get("fingerprint_matched", 0),
        counters.get("knn_matched", 0), softener_matched, abstained,
        cleared, changed, embedded_other,
    )
    # Read `changed` TOGETHER with the composite count: the row loop CLEARS
    # composite events (no single tier names them) and
    # promote_embedded_composites puts 'other'/'composite' straight back, so
    # `changed` alone OVERSTATES churn by roughly the composite count (measured
    # boot: circuit_1 48 changed / 52 composites, circuit_2 1 / 1, net ~0). A
    # skip condition built on `changed` alone would call that tail-chasing
    # real work.
    if scanned and changed <= embedded_other:
        log.info("[%s] reclassify: no NET verdict change — this pass "
                 "re-derived %d stored answer(s) and every write it made was "
                 "the composite round-trip", circuit, scanned)

    # The max-age clock is restarted at the START of the periodic full
    # re-derive, in prepare, where it clears the stamps. Nothing to record
    # here — by the time this runs the rows are already un-stamped and will
    # drain like any others. A stamped pass skips rows by design and must never
    # be mistaken for full coverage, or the backstop would never fire.
    _left = ctx.get("backlog_remaining") or 0
    if _left:
        log.info("[%s] reclassify: %d event(s) re-derived, %d still queued — "
                 "draining in the background so a deploy costs nothing at boot",
                 circuit, scanned, _left)
    if not scanned and not _left:
        log.info("[%s] reclassify: nothing to do — every unlabelled event "
                 "already carries the current verdict stamp (code, rule bands "
                 "and labels all unchanged since the last pass)", circuit)
    if veto_counts:
        log.info("[%s] toilet vetoes: %s (per-event detail at DEBUG)",
                 circuit, "; ".join(f"{n}× {why}" for why, n
                                    in sorted(veto_counts.items(),
                                              key=lambda kv: -kv[1])))
    return result


async def reclassify_all_events_from_signatures_async(
        conn: sqlite3.Connection, circuit: str, ha_tz=None,
        since_ts: Optional[str] = None, batch: int = 200,
        backlog_limit: Optional[int] = None) -> Dict[str, Any]:
    """``reclassify_all_events_from_signatures`` in chunks.

    Same pass, same result, but the row loop goes to ``run_db`` one batch at a
    time: with ONE DB worker a monolithic multi-minute submission makes every
    queued page render wait for the whole pass. Mirrors
    ``ClusterEngine.backfill_unmatched_async`` — chunk = transaction = one
    run_db call; prepare and finalize are their own submissions.
    """
    ctx, rows = await _db.run_db(_reclassify_prepare, conn, circuit, ha_tz,
                             since_ts, backlog_limit)
    counters = _new_reclassify_counters()
    # ADAPTIVE batch: the contract is in time (no run_db call holds the worker
    # past ~1 s) and a fixed row count is not a time budget — at ~87 ms per
    # production event a fixed batch=200 held the single worker ~17 s, and a
    # page render queued behind it waited that long. Per-event cost varies
    # with hardware, waveform size and tiers reached, so no constant suits both
    # a dev laptop and a HA host; each chunk is re-aimed from measured
    # throughput.
    import time as _time

    i, size = 0, min(batch, _CHUNK_START_ROWS)
    while i < len(rows):
        t0 = _time.monotonic()
        await _db.run_db(_reclassify_chunk_sync, conn, circuit,
                     rows[i:i + size], ctx, counters)
        took = _time.monotonic() - t0
        i += size
        if took > 0.01:
            per_row = took / max(1, size)
            size = int(_CHUNK_TARGET_SECONDS / per_row)
        size = max(_CHUNK_MIN_ROWS, min(batch, size))
    return await _db.run_db(_reclassify_finalize, conn, circuit, since_ts, ctx,
                        counters)



def reclassify_all_events_from_signatures(
    conn: sqlite3.Connection,
    circuit: str,
    ha_tz=None,
    since_ts: Optional[str] = None,
    backlog_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Retrain signatures, then backfill ``matched_fixture_type`` over every
    unlabelled event on ``circuit`` — STRUCTURAL RULES FIRST (softener session,
    then the washer-cycle sweep, then the per-event toilet/dishwasher/shower/
    zone rules), k-NN as the residual. Each write stamps ``matched_via`` and
    ``cycle_group_id`` (the History rollup key).

    ``ha_tz`` is needed only for the softener regen-band match (local clock vs
    UTC-stored timestamps) — pass it from EVERY caller so the softener label is
    stable across reclassifies; softener detection is hard-gated by
    ``home_profile.has_water_softener`` + ``softener_circuit``.

    NEVER touches user-labelled rows (WHERE user_fixture_type IS NULL). Writes
    the canonical matched type, or NULL on abstention — clearing a stale prior
    match and its provenance is what makes the pass idempotent. The tier ladder
    never writes 'other' (a display-only fallback); only the composite
    re-promotion in ``_reclassify_finalize`` does. Returns the counts assembled
    there.
    """
    ctx, rows = _reclassify_prepare(conn, circuit, ha_tz, since_ts,
                                    backlog_limit)
    counters = _new_reclassify_counters()
    # yield_lock=True: this entry point runs on a PRIVATE connection inside
    # run_isolated_write, where releasing the file write-lock periodically is
    # what lets a waiting user save through. See the note at the call site.
    _reclassify_chunk_sync(conn, circuit, rows, ctx, counters, yield_lock=True)
    return _reclassify_finalize(conn, circuit, since_ts, ctx, counters)
