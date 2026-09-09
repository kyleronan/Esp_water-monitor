"""The reclassify pass: verdict stamps, staleness, and the chunked driver.

Split out of ``database.py`` by unit 7.1. This is a MOVE — no behaviour
changed, no thresholds touched. What lives here is one coherent subsystem:
the verdict stamp (what makes an event's stored verdict re-usable), the
things that release a stamp, and the three-phase pass — prepare, chunked row
loop, finalize — plus its sync and async entry points.

Direction of the dependency, which is the whole reason this file is shaped
the way it is
--------------------------------------------------------------------------
``reclassify`` depends on ``database``; ``database`` does NOT depend on
``reclassify`` at import time. The one remaining in-file caller
(``database.patch_event`` -> ``invalidate_cluster_verdict_stamps``) imports
lazily, and every external caller reaches the moved names through
``database.__getattr__`` (PEP 562), which forwards here. An eager
``from .reclassify import ...`` at the bottom of database.py would close the
loop and raise ``ImportError: cannot import name ... from partially
initialized module`` whenever this module is imported first.

The siblings are reached through the MODULE object (``_db.run_db``), not
through ``from .database import run_db``. That is deliberate and load-bearing:
those names were module globals of ``database`` before the split, so tests and
callers can substitute them with ``monkeypatch.setattr(database, ...)``. A
from-import would snapshot the original object and the substitution would bind
an attribute nobody reads — the pass would then run for real while the test
that thinks it is watching measures nothing.

``log`` is the SAME logger object database.py uses, not a fresh
``getLogger(__name__)``. Renaming the channel would silently empty every
``caplog.at_level(logger="water_monitor.app.database")`` that watches this
pass, while leaving those tests green.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from . import database as _db

# Same channel as before the split — see the module docstring.
log = _db.log


# dev46 (46k) — bump when the STAMP'S OWN definition changes (a new input
# folded in, a component computed differently). Every stamp then differs from
# every stored one and the next pass re-derives everything, which is the
# correct response to "the rule for deciding staleness just changed".
# v2 (2026-08-18): the label pool left the stamp — a label now pushes an
# invalidation to its own cluster instead. Bumping forces one full re-derive so
# no row keeps a stamp computed under the old, stricter rule.
_VERDICT_STAMP_ALGO = 3

# Force a full unstamped pass if the last one is older than this. The stamp can
# only be as complete as the input list baked into it; this is what stops an
# omission from meaning "stale forever" (see the migration's docstring).
_VERDICT_STAMP_MAX_AGE_DAYS = 7



def compute_verdict_stamp(conn: sqlite3.Connection, circuit: str) -> str:
    """Fingerprint of everything that can change an unlabelled event's verdict.

    Same stamp ⇒ the classifier would reach the same answer it already stored,
    so the event needs no work. The components, and why each is here:

    * **classifier code version** — a deploy can change any rule, veto or
      tier. This is also the safety net for any input NOT enumerated below:
      whatever is missed, shipping a build invalidates everything, so
      staleness can never outlive a release.
    * **rule bands, all regimes** — bands are fitted per supply regime and an
      event is judged by ITS era's bands, so a re-fit in any regime can move
      verdicts. Cheap: one row per regime.
    **The label pool is deliberately NOT here, and that is the whole design.**
    It was, and it made the stamp useless on the circuit that mattered: one
    new label invalidated all ~5,400 events, so the boot pass ran in full
    every time the operator labelled anything — which is constantly. Measured
    on the production database: labelling 3 events re-derived 5,417 verdicts
    in 85 s and moved **zero** of them. Three more examples of a class that
    already has dozens does not shift a k-NN neighbourhood.

    A label's realistic reach is events SIMILAR to it, which is a bounded set,
    not the whole table — so a label PUSHES an invalidation to its own cluster
    (``invalidate_cluster_verdict_stamps``) instead of the stamp polling every
    row. That keeps the cost proportional to what could actually change, and
    flat as the table grows: at 20,000 events the old rule would have swept
    ~9 minutes per boot to conclude nothing had changed.

    Also deliberately not here: an individual event's own features. Those are
    per-row, and a global stamp cannot express them — the invalidation trigger
    handles those.

    What makes both omissions safe is ``_VERDICT_STAMP_MAX_AGE_DAYS``: a
    cluster is an imperfect proxy for similarity (46e measured DBSTREAM label
    purity at 0.387), so a label CAN reach an event in another cluster. The
    weekly unfiltered pass means anything the targeted invalidation misses
    lands within days rather than never.
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

    # Settings the pass reads that are NOT rule bands. Enumerated column by
    # column rather than hashing the rows, and that is not fussiness:
    # home_profile.away_mode flips with presence and updated_at moves on any
    # write, so `SELECT *` would re-stamp the whole table several times a day
    # and quietly undo the entire optimisation. Same rule as the invalidation
    # trigger's watch list — inputs only.
    #
    #   has_water_softener / softener_circuit -> the softener session detector
    #   fingerprint_labeling_enabled          -> whether the fingerprint tier runs
    #   build_year / epa_flush_cap_enabled    -> the toilet flush cap (veto input)
    #   daily_summary_tz                      -> time-of-day rule predicates
    try:
        row = conn.execute(
            "SELECT has_water_softener, softener_circuit, "
            "       fingerprint_labeling_enabled, build_year, "
            "       epa_flush_cap_enabled, daily_summary_tz "
            "FROM home_profile WHERE id = 1").fetchone()
        h.update(f"|home:{tuple(row) if row else ()}".encode())
    except sqlite3.OperationalError:
        h.update(b"|home:unavailable")

    # circuit_type picks the rule set; winterized (46h) suspends the circuit.
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
    """A new label PUSHES a re-check to the events it could plausibly affect.

    Called when the operator labels an event. Its cluster is the set of
    events the classifier considers similar, and therefore the set whose
    verdicts a new exemplar can realistically move. Releasing those — rather
    than the whole circuit — is what keeps the work proportional to the change
    and flat as history grows.

    The event itself does not need releasing: it now carries a user label, so
    the pass skips it by definition (``user_fixture_type IS NULL``).

    Returns the number of peers released. Zero is normal and fine — an event
    with no cluster yet simply has no known neighbours, and the weekly
    unfiltered pass is the backstop for anything this misses.
    """
    row = conn.execute(
        "SELECT cluster_id FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit)).fetchone()
    if row is None or row[0] is None:
        return 0
    # Narrow further by TIER, and the reason is the locked-baseline design.
    # The ladder tries the rule tier FIRST and the label-driven tiers only
    # when it abstains. Rule bands are fit-once and hard-locked, and an
    # event's features are immutable, so a peer already claimed by a rule
    # (or by a cycle/session detector, which sit above the label tiers and
    # are equally rule-driven) will return the identical verdict no matter
    # how many labels exist. Re-deriving those is provably wasted work.
    #
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
    """Re-open recent events for one pass — boot's CATCH-UP job.

    The hourly maturity re-check (maturity_recheck.py) re-evaluates events
    from the last few hours, because an event's cycle context arrives after
    it closes. If the add-on was off — a power cut, a redeploy, a crash —
    those hours had no re-check, and the stamp alone would not know: those
    events were stamped when they were first classified, so they look settled.

    Boot therefore releases the same window unconditionally. It is a handful
    of events (the hourly pass logs 6-26), so it costs nothing, and it means a
    restart can never leave an event stuck with a verdict taken before its
    cycle context landed.

    Events the add-on missed entirely arrive as NEW rows via the historical
    importer and are unstamped already, so they need nothing here.
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

    The stamp is only as complete as the input list baked into it. If some
    input is ever missed, affected verdicts would stay stale silently and
    nothing would ever say so. This bounds that failure to
    ``_VERDICT_STAMP_MAX_AGE_DAYS`` instead of forever, at a cost of one full
    pass a week — which the operator no longer waits on, since the pass runs
    in the background (46k option B).

    Unreadable state returns True: forcing the expensive-but-correct path is
    the right way to fail.
    """
    # The stamp is only trustworthy while the invalidation trigger exists to
    # release rows whose own inputs changed. Without it a stamped row would
    # keep a verdict derived from features it no longer has, and NOTHING would
    # say so. So the skip refuses to engage rather than run unguarded — the
    # optimisation cannot outlive the mechanism that keeps it correct.
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
    """Mark these events as needing re-classification (46k).

    Call from ANY path that rewrites a feature the classifier reads —
    cycle-pulse recount, volume recompute, waveform repair, exclusion-verdict
    changes. The global stamp cannot see per-row edits, so a row whose own
    inputs changed must be released explicitly or it keeps a verdict derived
    from features it no longer has.

    Clearing is always safe: the worst case is one event re-derived
    unnecessarily. NOT clearing is the unsafe direction.
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

    dev46 (46a/C2a): the pass is driven in CHUNKS, so its loop-carried state
    lives in one dict that is threaded through every batch instead of in
    function locals. Six counters plus the flush-veto tally — nothing else
    crosses a row boundary, which is exactly why the pass could be sliced.

    dev46 (46k): ``changed`` counts rows whose verdict actually DIFFERED from
    what was already stored — the number that decides whether this ~145 s boot
    pass earns its cost. Every other counter reports what the pass PRODUCED,
    which is why nobody could tell that a second boot nine minutes after the
    first re-derived 5,417 identical answers (observed 2026-08-17 19:22 vs
    19:31: the same events vetoed, in the same order, both times).
    """
    return {"scanned": 0, "matched": 0, "rule_matched": 0,
            "softener_matched": 0, "cleared": 0, "abstained": 0,
            "changed": 0, "veto_counts": {},
            # dev51 (3.2) — real buckets. Everything that was not softener or
            # plain k-NN used to be logged as "via rules", so a TinyModel hit,
            # a fingerprint hit and a k-NN-invariant hit were all invisible in
            # the reclassify summary — the one line that says which tier did
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


# dev51 (3.3) — the backlog slice: NULL-stamp rows first and newest-first
# (someone is waiting on those); the stale-stamp group OLDEST-first so the
# historical tail drains front-to-back instead of being starved by fresh
# invalidations. Kept as a constant so the ordering rule is unit-testable
# without standing up the whole reclassify context.
_BACKLOG_ORDER_BY = ("(verdict_stamp IS NOT NULL), "
                     "CASE WHEN verdict_stamp IS NULL THEN start_ts END DESC, "
                     "start_ts ASC")


def _forced_reopen_allowed(since_ts: Optional[str]) -> bool:
    """dev51 (3.4): the periodic full re-open may fire only from an
    UNWINDOWED pass (boot, the hourly backlog slice) — never from the hourly
    settle-window call, which used to re-open a whole circuit from inside a
    6-hour window."""
    return since_ts is None


# dev51 (3.5) — how often the SYNC reclassify path commits so the SQLite file
# write-lock is released. The default cadence (300 rows ≈ 26 s at ~87 ms/row)
# was far longer than a user save's 5 s busy timeout, so a label landing
# mid-slice was refused. ~40 rows ≈ 3-4 s keeps every hold under that timeout.
_SYNC_YIELD_EVERY_ROWS: int = 40


# dev46 (46k) — how many events one pass may re-derive. The backlog left by a
# global change (a new build, a rules re-fit) is drained a slice at a time by
# the hourly pass instead of in one burst, because NOBODY IS WAITING for it:
# a deploy needs the work done eventually, not now, and doing it at boot put
# it at the single worst moment — right when the operator wants to look at the
# add-on. ~400 costs a few seconds and drains 5,400 events overnight.
#
# Deliberately released rows (NULL stamp — new events, the settle window, a
# label's cluster push) are prioritised inside this budget, because those DO
# have someone waiting on them.
_VERDICT_BACKLOG_PER_PASS: int = 400

# dev46 (46k) — the chunk budget, in SECONDS, because that is what the C2a
# contract is actually about: how long one run_db call can hold the single DB
# worker before a queued page render notices. ~1 s is the threshold at which a
# wait stops being distinguishable from a normal request.
_CHUNK_TARGET_SECONDS: float = 1.0
# First chunk of a pass, before there is a measurement to aim with. Small on
# purpose: guessing low costs one extra round-trip, guessing high costs the
# operator a visible stall on exactly the boot they are watching.
_CHUNK_START_ROWS: int = 25
# Never go below this, or chunk overhead (a commit and a round-trip each) would
# dominate on a slow event.
_CHUNK_MIN_ROWS: int = 5


def _reclassify_prepare(conn: sqlite3.Connection, circuit: str, ha_tz=None,
                        since_ts: Optional[str] = None,
                        backlog_limit: Optional[int] = None):
    """Everything a reclassify pass computes ONCE, plus its candidate rows.

    dev46 (46a/C2a) — split out of ``reclassify_all_events_from_signatures``
    so the pass can be driven chunk-wise through ``run_db``. This half is the
    expensive-but-bounded part: signature training, the per-regime calib
    cache, the whole-circuit cycle detectors (washer / softener / dishwasher),
    usage baselines, the fingerprint library and the toilet cap. All of it is
    READ-ONLY for the row loop, which is what makes batching safe.

    Returns ``(ctx, rows)``.
    """
    # 1. Retrain the per-type centroids (for the Signatures UI display). The
    #    k-NN itself reads events directly, so this is purely cosmetic but keeps
    #    the Signatures page in sync.
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

    # 2. Backfill over unlabelled events. Query carries BOTH the legacy and the
    #    active-flow features so the matcher uses whichever it can (active when
    #    backfilled). An event now excluded_from_training carries no fixture
    #    identity → its matched_fixture_type is cleared (stale-match carry-forward).
    from .event_rules import (detect_dishwasher_cycles,
                              detect_softener_sessions, detect_washer_cycles,
                              get_home_timezone, parse_hhmm_to_minutes)
    from .rule_calibration import load_rule_calibration

    # Frozen per-home rule bands (empty dict → predicates use shipped defaults).
    # Regime-aware: bands are fitted per SUPPLY REGIME (migration 20260565), so
    # the per-event rule tier below resolves each event's calib by its
    # start_ts — a pre-pump event is judged by pre-pump bands even when the
    # pass runs today. The window-scanning cycle detectors (washer/dishwasher/
    # softener) take ONE calib per pass; they get the CURRENT regime's — the
    # regime where new events land. (v1 limitation: a full reprocess spanning
    # a regime boundary scans historical cycles with current bands; per-event
    # rules, where the observed staleness actually bit, are fully resolved.)
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
        # dev34: the regime-invariant rung's shape features (the rest of its
        # set is already covered by the tuples above).
        + _db._SIGNATURE_KNN_INVARIANT_FEATURES
        + ("has_pressure_transient",)     # the flush predicate's extra input
        # dev19: the edge tier reads these off the query dict (expanded inside
        # match_event_to_signature_knn); harmless extras for the rules tier.
        + ("onset_signature_json", "offset_signature_json")))
    circuit_type = _db.get_circuit_type(conn, circuit)
    # Windowed (periodic maturity re-check): bound the expensive per-event k-NN row
    # loop below to events >= since_ts, but give the detectors a lookback (>= the
    # softener's max session span) so a cycle straddling the window start is still seen
    # WHOLE — otherwise its in-window members would be wrongly retracted. since_ts=None
    # → full circuit (startup / manual reprocess).
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
    # dev.24 — water-softener sessions (hard-gated: enabled AND this circuit).
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
    # dev.39 — dishwasher cycles: a chain of gentle small fills the per-event
    # cycle-pulse rule misses. Exclude ids the washer/softener detectors already
    # claimed so a brine chain / laundry top-off can't be re-read as a dishwasher.
    dishwasher_ids = (
        detect_dishwasher_cycles(conn, circuit, since_ts=detector_since, calib=calib,
                                 exclude_ids=set(washer_ids) | set(softener_ids))
        if circuit_type != "zone" else {})
    # Phase 2.3 — re-score each scanned event against the FROZEN baseline (storage
    # only; reclassify NEVER notifies or shuts off). Baseline + sensitivity loaded
    # once for the whole pass; the extra SELECT columns the scorer needs are deduped
    # into the query so a column already in qfeats isn't selected twice.
    from .anomaly_baseline import load_usage_baselines
    _baselines = load_usage_baselines(conn, circuit)
    _sens = _db.get_sensitivity_config(conn, circuit)
    _SCORE_COLS = ("volume_litres_effective", "volume_litres", "duration_seconds",
                   "peak_flow_lpm", "is_pressure_restoration_phantom", "is_cross_talk",
                   "is_low_flow_dribble", "user_ignored",
                   "phantom_suppression_averted")

    # Fingerprint tier library (2026-07 audit Phase 3) — built ONCE per run,
    # fresh (no cache; a reclassify usually follows a label change). Gated by
    # the home_profile toggle; any failure just disables the tier for this run.
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

    # Toilet physics veto (dev17) — cap computed once for the whole pass.
    _toilet_cap = _db.get_toilet_flush_cap_litres(conn)

    where = "WHERE circuit = ? AND user_fixture_type IS NULL"
    qparams: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        qparams.append(since_ts)

    # dev46 (46k) — skip events whose verdict provably cannot have changed.
    #
    # The pass always stored its answer (matched_fixture_type) but never
    # stored whether that answer was still VALID, so every boot re-derived
    # every unlabelled event to discover almost none of them had moved
    # (2026-08-17: 151.7 s, 5,426 events, ~0 net changes on a repeat boot —
    # and the same events re-vetoed since 2026-07-26).
    #
    # A row is a candidate when its stamp is NULL (new, or explicitly released
    # by invalidate_verdict_stamps) or differs from the current one (code,
    # bands or labels moved). Everything else already holds the answer this
    # pass would compute. Skipping is therefore loss-free by construction, and
    # every uncertain case falls on the recompute side.
    stamp = compute_verdict_stamp(conn, circuit)
    # dev46 (46k) — the weekly backstop INVALIDATES; it does not bypass.
    #
    # It used to bypass the stamp filter for one pass. That worked while a
    # pass meant "every candidate", and broke the moment the backlog trickle
    # made every pass a slice: bypassing means rows already re-derived in this
    # sweep stay candidates, so the sweep re-does the same slice forever and
    # never finishes. Marking completion after one slice was no better — the
    # backstop would cover 400 of ~5,500 rows and restart its own clock,
    # becoming the permanent silent omission it exists to prevent.
    #
    # Clearing the stamps instead makes the sweep ordinary work: every row is
    # now genuinely un-derived, the normal filter picks it up, and the trickle
    # drains it at the usual rate with no special case anywhere. The clock
    # restarts HERE, at the start, because the invalidation is what guarantees
    # the coverage happens — the rows cannot be skipped again until they are
    # re-stamped, one slice at a time.
    # dev51 (3.4) — only the UNWINDOWED invocations (boot, the hourly backlog
    # slice) may fire the periodic full re-open. The hourly settle-window pass
    # also reached here and, when the week rolled over, re-opened the WHOLE
    # circuit from inside a 6-hour-windowed call — ~5,000 rows dumped back into
    # the backlog at once, ~12 h to re-drain at 400/hr, and the write lock held
    # by that pass for the duration (2026-08-31 23:21: "1440 still queued" and
    # a label save refused in the same hour).
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

    # dev46 (46k) — an event younger than the settle horizon is NOT DECIDED
    # YET, so it must not be stamped.
    #
    # maturity_recheck re-runs this pass hourly over recent events precisely
    # because cycle context arrives AFTER an individual event closes: a
    # dishwasher's third fill is what lets the cycle detector claim the first
    # one. Stamping a young event on its first look would mark that
    # conversation finished before it started, and the hourly pass would skip
    # it for the rest of the settle window — silently disabling the mechanism
    # (verified: passes 2 and 3 over a 1-hour-old event scanned nothing).
    #
    # Leaving them unstamped also IS the offline catch-up: an event that ages
    # past the horizon while the add-on is down was never stamped, so the next
    # pass picks it up with no special case.
    try:
        from .maturity_recheck import _SETTLE_HORIZON_HOURS as _settle_h
    except Exception:                       # noqa: BLE001 — never block a pass
        _settle_h = 6
    stamp_before_ts = (datetime.now(timezone.utc)
                       - timedelta(hours=_settle_h)).isoformat()
    select_cols = list(dict.fromkeys(
        ("id", "start_ts", "matched_fixture_type", "matched_via",
         "cycle_group_id", "excluded_from_training",
         "match_rejection_reason",       # dev33: abstention marker mark/retract
         "match_confidence",             # dev51 (3.1): so an unchanged model
                                         # verdict is not rewritten every pass
         "active_flow_segment_count")
        + qfeats + _SCORE_COLS))
    # dev46 (46k) — TRICKLE. With a budget, take the highest-priority slice
    # and leave the rest for the next pass.
    #
    # The inner ORDER BY ranks by WHO IS WAITING:
    #   verdict_stamp IS NULL first — deliberately released rows: a new event,
    #     one still settling, a peer freed by a label you just saved. Someone
    #     is looking at those.
    #   then start_ts DESC — within the global backlog, recent events first,
    #     because that is what the operator actually opens.
    #
    # The subquery picks the slice; the outer ORDER BY start_ts keeps the row
    # loop's existing ascending order, so batching changes WHICH rows a pass
    # sees, never how it processes them.
    backlog_remaining = 0
    if backlog_limit is not None and backlog_limit > 0:
        total = conn.execute("SELECT COUNT(*) FROM events " + where,
                             qparams).fetchone()[0]
        backlog_remaining = max(0, total - backlog_limit)
        # dev51 (3.3) — two queues, two directions. NULL-stamp rows (new,
        # settling, freed by a label) go first and NEWEST-first, because
        # someone is waiting on those. The STALE-stamp group — events a new
        # build or a promotion re-opened — drains OLDEST-first. Newest-first
        # there meant a steady trickle of fresh invalidations kept pushing the
        # historical tail to the back of every slice: the ~1,041 July events
        # were examined by no pass for weeks, not because the budget was too
        # small but because they never reached the front of it.
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
        # dev46 (46k) — the stamp every row this pass touches gets written
        # with, and whether the stamp filter was bypassed for a due full pass.
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

    dev46 rule N2a: self-contained transaction. Every statement for this
    chunk, plus its commit, happens inside this one callable, so no foreign
    statement can land inside an open transaction. Chunk boundary =
    transaction boundary = where a queued page render gets to interleave.

    ``counters`` is mutated in place so the tallies survive across batches.
    The write-time ``user_fixture_type IS NULL`` guard inside
    ``set_event_matched_fixture_type`` (dev46 R1/N1) is what makes an
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
    # dev48 — burst context for the toilet rule's appliance veto. Computed ONCE
    # for the whole chunk: compute_for_events does a single windowed read over
    # the id set, so hoisting it out of the loop is the difference between one
    # query and one per event. MATURE here for the same reason the model tier
    # uses it below — in a batch pass every sibling is already in the table.
    # dev51 (3.1) — the model tier's confidence for the current row. Assigned
    # in the TinyModel branch; the writer only consumes it when the verdict's
    # provenance is 'tinymodel', so a value can never leak onto a row that
    # another tier claimed.
    new_conf = None
    _burst_ctx = {}
    try:
        from . import burst_features as _bf
        _burst_ctx = _bf.compute_for_events(
            conn, circuit, [r["id"] for r in rows], config=_bf.CONFIG_MATURE)
    except Exception as _bf_exc:                       # noqa: BLE001
        # No burst context means the veto abstains and the rule behaves exactly
        # as it did before it existed — never a reason to fail a reclassify.
        log.debug("burst context unavailable in reclassify: %s", _bf_exc)
    for r in rows:
        scanned += 1
        new_group = None
        if r["id"] in softener_ids:
            # Softener is checked BEFORE the excluded gate — a deliberate exception
            # to dev.23's "excluded → no identity": regen consumption is real, and
            # matched_* is written separately from the volume verdict, so a
            # dribble-flagged regen pulse keeps its verdict AND reads water_softener.
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
            from .config import pump_gates_active as _pga
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
                # Report the burst veto the same way the flush-physics floor is
                # reported: DEBUG per event, one INFO summary at the end. The
                # rule returns None either way, so without this a veto that
                # fires and a veto that never fires look identical from outside.
                _bw = toilet_burst_veto_reason(feats, _ev_calib, _pump, _ev_burst)
                if _bw:
                    log.debug("[%s] event %s: toilet match vetoed by burst "
                              "context — %s (vol=%s L)", circuit, r["id"], _bw,
                              r["volume_litres"])
                    _key = "in an appliance burst"
                    veto_counts[_key] = veto_counts.get(_key, 0) + 1
                # dev47 (47b) — TinyModel tier, in the same ladder position the
                # live path uses (anchors -> model -> fingerprint/k-NN). The
                # batch pass uses MATURE burst features: every event's siblings
                # are already in the table here, which is exactly the context
                # the live pass could not have.
                _tm_hit = None
                try:
                    from . import tinymodel as _tm
                    _tm_hit = _tm.classify(conn, circuit, r["id"], feats,
                                           burst_config=_tm.bf.CONFIG_MATURE)
                except Exception as _tm_exc:
                    log.debug("tinymodel tier unavailable in reclassify: %s",
                              _tm_exc)
                # Fingerprint tier (2026-07 audit Phase 3) — whole-waveform NN
                # against USER-labeled events, tight-threshold only. Sits between
                # the structural rules and the scalar k-NN: stronger evidence
                # than a scalar vote, weaker than cycle/session context above.
                if _tm_hit is not None:
                    # The model claimed it; the tiers below are its fallback,
                    # not its reviewers.
                    new_type = _db._canonical_fixture_type(_tm_hit[0])
                    new_via = "tinymodel" if new_type is not None else None
                    # dev51 (3.1) — carry the model's confidence to the row,
                    # as the live path does. Only this tier has one.
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
                    # dev34: distinguish the regime-invariant rung so its
                    # contribution is measurable (and separable) in the data.
                    new_via = (
                        None if new_type is None
                        else "knn_invariant"
                        if hit.get("match_source") == "invariant_features"
                        else "knn")
            # Toilet physics veto (dev17): whatever tier proposed 'toilet'
            # (rule / fingerprint / k-NN), the event must be physically able
            # to BE a flush. Vetoed → abstain (never re-guess another type).
            if new_type == "toilet":
                vfeats = dict(feats)
                vfeats["active_flow_segment_count"] = r["active_flow_segment_count"]
                why = toilet_veto_reason(vfeats, _toilet_cap)
                if why:
                    # DEBUG per event, one INFO summary at the end: ~60 of
                    # these fire per reclassify and they are the veto WORKING
                    # (investigated 2026-08-03: the recurring 2.2–2.8 L band
                    # is the labelled dishwasher's upper fill-pulse tail — 9
                    # user labels in-band, 0 of them toilet, every event has
                    # a neighbour within 30 min — so the floor is what keeps
                    # appliance pulses from being named flushes).
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
            # dev46 (46k) — this branch IS the pass's real output. Counting it
            # costs nothing and is the only way to see how much of the boot
            # pass was necessary.
            changed += 1
            if new_type is None and prev is not None:
                cleared += 1
        # dev46 (46k) — stamp EVERY row examined, including the ones whose
        # verdict was already right. That is the whole mechanism: without it
        # an event the classifier agrees with is never recorded as decided,
        # so it returns as a candidate on the next boot forever. `fb9d5fa5`
        # was re-derived on every boot since 2026-07-26 for exactly this
        # reason — the pass kept confirming an abstention it had no way to
        # remember making.
        #
        # The user_fixture_type re-check is the same N1 guard the verdict
        # write carries: a PATCH landing mid-pass must not have its row
        # stamped from premises that no longer hold.
        # ...but only once the event is old enough to be DECIDED. A row still
        # inside the settle horizon keeps its NULL stamp so the hourly
        # maturity re-check can keep re-evaluating it as cycle context lands.
        if r["start_ts"] and r["start_ts"] < _stamp_before:
            conn.execute(
                "UPDATE events SET verdict_stamp = ? "
                "WHERE id = ? AND user_fixture_type IS NULL",
                (_stamp, r["id"]))
        # dev33 (§2.1) — mark / retract classification-tier abstention so a
        # silent outage is measurable and a recovery is visible. Only ever
        # fills an EMPTY reason (artifact + cluster reasons are more specific),
        # and is retracted the moment any tier names the event.
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
        # Re-score against the frozen baseline + persist (storage only — no notify /
        # shut-off from a backfill). flagged=1 marks a genuine (non-artifact) anomaly.
        sfeats = {c: r[c] for c in _SCORE_COLS}
        sfeats["matched_fixture_type"] = new_type
        av = score_event_anomaly(sfeats, _baselines, _sens)
        conn.execute(
            "UPDATE events SET anomaly_score = ?, anomaly_type = ?, flagged = ? "
            "WHERE id = ?",
            (av.get("score"), av.get("anomaly_type"),
             1 if av.get("is_anomalous") else 0, r["id"]),
        )
        # dev46 (46a/C2a + 46k): WHO OWNS THE TRANSACTION decides this.
        #
        # Through run_db the chunk IS the transaction (rule N2a) and the one
        # commit below is correct: yielding here would sleep 30 ms holding the
        # single DB worker, delaying the very queue it exists to let through.
        #
        # But the SYNC entry point is called from inside run_isolated_write by
        # maturity_recheck, reprocess and training_manager — a PRIVATE
        # connection whose contract is spelled out in run_isolated_write's own
        # docstring: "the job's per-row commits release the file write-lock
        # between rows". C2a removed this call for the run_db path and silently
        # broke that contract for those three, so one long pass held the SQLite
        # write lock for its whole duration and every concurrent user save got
        # "database is locked" (observed 2026-08-18 22:47 onward: a label save
        # and the supply-regime sampler both refused for minutes).
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
    # ── Composite annotation (dev.39, step 1) ────────────────────────────────
    # Annotate sustained events with fixtures hidden inside them (a toilet flushed
    # mid-shower), then upgrade events that abstained but clearly contain a second
    # draw from "(none)" to "other"/composite. Wrapped so a waveform/JSON hiccup
    # can never undo the classification that already committed above.
    embedded_annotated = embedded_other = 0
    try:
        emb = _db.recompute_embedded_fixtures(conn, circuit, since_ts=since_ts)
        embedded_annotated = emb["annotated"]
    except Exception:                       # pragma: no cover - defensive
        log.exception("[%s] embedded-fixture annotation failed (classification "
                      "already committed)", circuit)
    # Re-promote abstained events that hide a real second draw (ANY embedded kind
    # — toilet, tap, …) to "other"/composite, from the STORED annotation so this
    # re-applies every run — the main loop above just cleared these to NULL, and
    # the embedded scan is incremental (won't re-report an already-annotated
    # event). Separate try from the scan above: a waveform hiccup there must not
    # also skip the re-promotion (that left prior 'other' events flickering to
    # NULL until the next reclassify). Decided from the PARSED JSON, not a LIKE,
    # so it can't drift from the detector's serialization or kind set.
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
        # dev46 (46k) — rows whose verdict actually differed from the stored
        # one. The pass's only real output; everything above is throughput.
        "events_changed": changed,
        # dev46 (46k) — what this pass deliberately did NOT do. A capped pass
        # that reports only what it processed reads as "everything is done".
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
    # dev46 (46k) — the boot pass costs ~145 s on this install, so what it
    # actually CHANGED is the evidence for whether it earns that.
    #
    # Read the two numbers together. The row loop abstains on composite events
    # (no single tier names them) and CLEARS their stored verdict; then
    # promote_embedded_composites, a few lines above, puts it straight back as
    # 'other'/'composite'. That round-trip is internal to one pass and leaves
    # the same final state, but it lands in `changed` — so `changed` on its
    # own OVERSTATES churn by roughly the composite count. Measured 2026-08-17
    # 21:00 boot: circuit_1 48 changed / 52 composites, circuit_2 1 / 1, i.e.
    # net change ~0 on both. Anyone using these numbers to judge the pass —
    # or to build a skip condition on it — needs both or they will conclude
    # the boot pass is doing real work when it is chasing its own tail.
    if scanned and changed <= embedded_other:
        log.info("[%s] reclassify: no NET verdict change — this pass "
                 "re-derived %d stored answer(s) and every write it made was "
                 "the composite round-trip", circuit, scanned)

    # dev46 (46k) — an UNFILTERED pass just covered every unlabelled event, so
    # restart the max-age clock. Only a forced pass may do this: a stamped
    # pass skips rows by design and must not be mistaken for full coverage,
    # or the backstop would never fire and the omission it guards against
    # would once again be permanent.
    # NOTE: the periodic full re-derive is marked at its START, in prepare,
    # where it clears the stamps. Nothing to record here — by the time this
    # runs the rows are already un-stamped and will drain like any others.
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
    """dev46 (46a/C2a) — ``reclassify_all_events_from_signatures`` in chunks.

    Same pass, same result, but the row loop is submitted to ``run_db`` one
    batch at a time instead of as a single multi-minute call. With ONE DB
    worker, a monolithic submission makes every queued page render wait for
    the whole pass; batching gives the queue a seam every ~batch rows.

    Mirrors ``ClusterEngine.backfill_unmatched_async`` — chunk = transaction =
    one run_db call. Prepare and finalize are their own submissions.
    """
    ctx, rows = await _db.run_db(_reclassify_prepare, conn, circuit, ha_tz,
                             since_ts, backlog_limit)
    counters = _new_reclassify_counters()
    # dev46 (46k) — ADAPTIVE batch, because a fixed row count is not a time
    # budget and the contract is stated in time: "no single run_db call holds
    # the worker for more than ~1 s" (C2a).
    #
    # batch=200 met that on the machine it was written on and missed it by 17x
    # on the target: a production event costs ~87 ms here, so 200 rows held the
    # single worker ~17 s. With a 400-row slice that is two chunks, and a page
    # render queued behind one waited ~17 s — the operator saw "pages are
    # ready" logged and then could not open a page for 37 s (2026-08-18 23:11).
    # Chunking that releases the worker twice is not chunking.
    #
    # So the driver measures its own throughput and re-aims each chunk at the
    # budget. Self-tuning beats a tuned constant here: per-event cost varies
    # with hardware, waveform size and how many tiers an event reaches, and no
    # single number is right for a dev laptop and a HA host at once.
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
    unlabelled event on ``circuit`` — STRUCTURAL RULES FIRST (dev.24 precedence:
    water-softener session, then dev.23's washer-cycle sweep, then the per-event
    toilet/dishwasher/shower/zone rules), k-NN as the residual. Each write stamps
    ``matched_via`` and ``cycle_group_id`` (the History rollup key, §7).

    ``ha_tz`` (the home timezone) is needed only for the water-softener regen-band
    match (local clock vs UTC-stored timestamps) — pass it from EVERY caller so
    the softener label is stable across reclassifies. Softener detection is
    hard-gated by ``home_profile.has_water_softener`` + ``softener_circuit``.

    NEVER touches user-labelled rows (WHERE user_fixture_type IS NULL). Writes
    the canonical matched type, or NULL on abstention — writing NULL clears a
    stale prior match (and its provenance), making the whole pass idempotent.
    Never writes 'other' (that is a display-only fallback).

    Returns counts: ``{"signatures_trained", "events_scanned", "events_matched",
    "events_rule_matched", "events_cleared", "events_abstained"}``.
    """
    ctx, rows = _reclassify_prepare(conn, circuit, ha_tz, since_ts,
                                    backlog_limit)
    counters = _new_reclassify_counters()
    # yield_lock=True: this entry point runs on a PRIVATE connection inside
    # run_isolated_write, where releasing the file write-lock periodically is
    # what lets a waiting user save through. See the note at the call site.
    _reclassify_chunk_sync(conn, circuit, rows, ctx, counters, yield_lock=True)
    return _reclassify_finalize(conn, circuit, since_ts, ctx, counters)
