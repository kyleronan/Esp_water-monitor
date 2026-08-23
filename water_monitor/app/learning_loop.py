"""dev47 (47c) — the continuous-learning loop.

THE GOAL THIS SERVES
--------------------
A home should need roughly a hundred labels in its first weeks and then run for
years without the operator policing it. The 2026-08-22 label-efficiency curves
showed why that cannot be a frozen model: every frozen classifier plateaus, and
the k-NN ladder actively DEGRADES as its pool grows across a supply-regime
change (.50 to .35 on the variant house). Homes change — this one grew a
booster pump mid-dataset — so the thing that has to be built is a loop.

THE LOOP
--------
1. Cycle detectors keep producing anchor exemplars forever, label-free.
2. Newly-labelled events (review card) join the pool.
3. A retrain produces a CHALLENGER; the referee decides whether it serves.
4. A swap invalidates a SCOPED set of stored verdicts, not the whole history.

WHY THE INVALIDATION IS SCOPED — AND A CORRECTION TO THE PLAN
-------------------------------------------------------------
The dev47 plan says the 46k verdict stamp "gains the model hash". Implemented
literally that is self-defeating: ``compute_verdict_stamp`` is a GLOBAL
fingerprint, so putting the model hash in it makes every stored verdict stale
the instant a model is retrained — a full-history re-derive on every retrain,
which is exactly what the plan's own F2 finding forbids. It is the same trap
the stamp's docstring already describes for the label pool ("labelling 3 events
re-derived 5,417 verdicts in 85 s and moved zero of them").

So the model hash stays OUT of the global stamp, and a retrain instead pushes a
targeted invalidation over the set that can actually change:

  * events with no stored classification (the backlog),
  * machine verdicts whose confidence sits below the NEW threshold,
  * everything after the retrain (forward events are classified by the new
    model anyway).

User-labelled events are never touched, and neither are confident machine
verdicts from a model that just passed a non-inferiority test against the
frozen benchmark. Code changes still sweep everything, because
``_code_fingerprint`` is in the stamp — staleness cannot outlive a release.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from . import burst_features as bf
from . import tinymodel as tm
from .model_referee import RefereeConfig, RefereeVerdict, Score, decide

log = logging.getLogger(__name__)

# ── anchor pool policy (R3) ─────────────────────────────────────────────────
# The rejected policy was "anchors <= 2x user labels per class". It starves the
# classes anchors exist to serve: dishwasher and washing-machine are
# user-label-POOR precisely BECAUSE the cycle detectors handle them, so 2x0
# gives zero exemplars for the two best-taught fixtures in the house.
#
# Instead: a floor that guarantees every anchor-backed class real
# representation, and ceilings that stop the daily cycle detectors from
# out-massing a hundred-odd hand labels (they would, roughly 10:1 within two
# years, whatever per-label weight is applied).
ANCHOR_FLOOR_PER_CLASS: int = 50
ANCHOR_MAX_CLASS_FRACTION: float = 2.0 / 3.0
ANCHOR_MAX_POOL_FRACTION: float = 0.5

# ── which detectors may teach ───────────────────────────────────────────────
# A detector earns the right to contribute training exemplars by DEMONSTRATING
# precision on current-era events — it is not granted by a constant.
#
# Two measurements forced this shape (2026-08-22):
#
# 1. The plan assumed the cycle detectors were near-perfect teachers. Measured
#    archive-wide they are not: anchor precision totals 0.781, and no tier
#    reaches 0.97 even at its ceiling (assume every unlabelled claim correct).
#    So anchors carry real label noise and cannot be ingested unconditionally.
#
# 2. But the archive-wide figure estimates the WRONG THING. A machine verdict
#    is frozen when the operator labels that event (reclassify skips labelled
#    rows), so those verdicts are never re-derived and the archive averages
#    every code era ever shipped. Restricted to the current era,
#    dishwasher_cycle is 14/14 where archive-wide it reads 0.742 — the low
#    number describes code that has already been replaced.
#
# Hence: measure per tier, over the current detector era, and require a LOWER
# CONFIDENCE BOUND to clear the bar rather than a point estimate. Certifying a
# teacher on 3 events would repeat exactly the small-n error the referee's
# non-inferiority rule exists to avoid — and a tier with too little current-era
# evidence is simply not yet eligible, which is the honest state after any
# detector change.
ANCHOR_MIN_TIER_PRECISION: float = 0.80
ANCHOR_MIN_TIER_EVENTS: int = 10

# What each tier claims, so a claim can be scored against a user label.
ANCHOR_TIER_TARGET: Dict[str, str] = {
    "rule_shower": "shower_tub",
    "washer_cycle": "washing_machine",
    "dishwasher_cycle": "dishwasher",
    "rule_toilet": "toilet",
    "rule_dishwasher": "dishwasher",
    "softener_session": "water_softener",
}

ANCHOR_SOURCE = "anchor"
USER_SOURCES = ("user", "training", "direct")

_POOL_COLUMNS = (
    "id", "start_ts", "user_fixture_type", "fixture_label_source",
    "matched_fixture_type", "matched_via", "match_confidence",
) + tm.BASE_FEATURES


def measure_anchor_precision(conn: sqlite3.Connection, circuit: str,
                             since_ts: Optional[str] = None) -> Dict[str, dict]:
    """Per-tier precision against user labels, over the current detector era.

    Scored only on events the operator actually labelled, which is a biased
    sample (people label what looked wrong), so this UNDER-states precision.
    That direction is the safe one for a gate about who may teach.
    """
    from .event_rules import DETECTOR_ERA_START
    since = since_ts or DETECTOR_ERA_START
    out: Dict[str, dict] = {}
    for tier, target in ANCHOR_TIER_TARGET.items():
        row = conn.execute(
            "SELECT COUNT(*) n, "
            "       SUM(CASE WHEN LOWER(user_fixture_type)=? THEN 1 ELSE 0 END) ok "
            "FROM events WHERE circuit = ? AND matched_via = ? "
            "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
            "  AND start_ts >= ?", (target, circuit, tier, since)).fetchone()
        n, ok = int(row[0] or 0), int(row[1] or 0)
        out[tier] = {"correct": ok, "n": n,
                     "precision": round(ok / n, 4) if n else None,
                     "lower_bound": round(Score(ok, n).wilson_lower(), 4) if n else 0.0}
    return out


def anchor_eligible_tiers(conn: sqlite3.Connection, circuit: str,
                          min_precision: float = ANCHOR_MIN_TIER_PRECISION,
                          min_events: int = ANCHOR_MIN_TIER_EVENTS,
                          since_ts: Optional[str] = None) -> set:
    """Detectors that have EARNED the right to contribute exemplars.

    A tier qualifies only with enough current-era labelled events and a lower
    confidence bound clearing the bar. Right after a detector change this
    returns few tiers or none; that is correct, not a fault — the previous
    era's evidence describes code that no longer runs.
    """
    stats = measure_anchor_precision(conn, circuit, since_ts)
    eligible = {t for t, s in stats.items()
                if s["n"] >= min_events and s["lower_bound"] >= min_precision}
    if not eligible:
        log.info("[%s] no detector has demonstrated >= %.2f precision on "
                 "current-era labels yet; anchor ingestion stays closed",
                 circuit, min_precision)
    return eligible


@dataclass
class PoolStats:
    total: int
    user: int
    anchor: int
    per_class: Dict[str, int]
    anchors_dropped: int

    def as_dict(self) -> dict:
        return {"total": self.total, "user": self.user, "anchor": self.anchor,
                "per_class": self.per_class,
                "anchors_dropped": self.anchors_dropped}


@dataclass
class RetrainOutcome:
    status: str                      # trained | kept | ineligible | unavailable
    reason: str
    verdict: Optional[RefereeVerdict] = None
    artifact: Optional[tm.Artifact] = None
    pool: Optional[PoolStats] = None
    invalidated: int = 0

    def as_dict(self) -> dict:
        return {"status": self.status, "reason": self.reason,
                "model_hash": self.artifact.model_hash if self.artifact else None,
                "threshold": self.artifact.threshold if self.artifact else None,
                "pool": self.pool.as_dict() if self.pool else None,
                "invalidated": self.invalidated,
                "referee": self.verdict.describe() if self.verdict else None}


# ── pool assembly ───────────────────────────────────────────────────────────
def _regime_index(conn: sqlite3.Connection):
    try:
        from .supply_regime import get_regimes, resolve_regime_for_ts
        regimes = get_regimes(conn)
        return lambda ts: resolve_regime_for_ts(regimes, ts)
    except Exception as exc:                    # pre-migration or empty table
        log.debug("regime lookup unavailable (%s); using regime 0", exc)
        return lambda ts: 0


def _load_rows(conn: sqlite3.Connection, circuit: str, where: str,
               params: tuple) -> List[dict]:
    cols = ", ".join(_POOL_COLUMNS)
    return [dict(r) for r in conn.execute(
        f"SELECT {cols} FROM events WHERE circuit = ? AND {where}",
        (circuit, *params))]


def _finish_rows(conn: sqlite3.Connection, circuit: str, rows: List[dict],
                 config: str = bf.CONFIG_MATURE) -> List[dict]:
    """Attach burst features and the regime id, set-wise."""
    if not rows:
        return rows
    feats = bf.compute_for_events(conn, circuit, [r["id"] for r in rows],
                                  config=config)
    bf.attach(rows, feats)
    regime_of = _regime_index(conn)
    for r in rows:
        try:
            r[tm.REGIME_FEATURE] = regime_of(r["start_ts"]) or 0
        except Exception:
            r[tm.REGIME_FEATURE] = 0
    return rows


def build_training_pool(conn: sqlite3.Connection, circuit: str,
                        anchor_floor: int = ANCHOR_FLOOR_PER_CLASS,
                        class_fraction: float = ANCHOR_MAX_CLASS_FRACTION,
                        pool_fraction: float = ANCHOR_MAX_POOL_FRACTION,
                        anchor_since_ts: Optional[str] = None
                        ) -> Tuple[List[dict], PoolStats]:
    """User labels (all of them) plus a bounded, regime-stratified anchor set.

    ``anchor_since_ts`` overrides the detector era used to decide which tiers
    have earned the right to teach (default: ``DETECTOR_ERA_START``).

    Anchors are subsampled newest-first within each supply regime, so a pool
    trimmed for size still spans the home's pressure eras — trimming purely by
    recency would quietly drop every pre-pump exemplar and re-create the drift
    the regime feature exists to handle.
    """
    rows = _load_rows(
        conn, circuit,
        "user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
        "AND COALESCE(excluded_from_training,0) = 0 "
        "AND COALESCE(training_excluded_by_user,0) = 0 "
        "AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
        "AND COALESCE(is_low_flow_dribble,0) = 0 "
        "AND COALESCE(is_cross_talk,0) = 0 "
        "AND training_quarantine_reason IS NULL", ())
    for r in rows:
        r["_y"] = (r["user_fixture_type"] or "").strip().lower()
    rows = [r for r in rows if r["_y"]]

    user_rows = [r for r in rows
                 if (r["fixture_label_source"] or "direct") != ANCHOR_SOURCE]
    eligible_tiers = anchor_eligible_tiers(conn, circuit,
                                           since_ts=anchor_since_ts)
    anchor_rows = [r for r in rows
                   if (r["fixture_label_source"] or "direct") == ANCHOR_SOURCE
                   and (r.get("matched_via") or "") in eligible_tiers]
    rejected_anchors = sum(
        1 for r in rows
        if (r["fixture_label_source"] or "direct") == ANCHOR_SOURCE
        and (r.get("matched_via") or "") not in eligible_tiers)

    user_per_class: Dict[str, int] = {}
    for r in user_rows:
        user_per_class[r["_y"]] = user_per_class.get(r["_y"], 0) + 1

    kept_anchors: List[dict] = []
    dropped = rejected_anchors
    by_class: Dict[str, List[dict]] = {}
    for r in anchor_rows:
        by_class.setdefault(r["_y"], []).append(r)
    for cls, members in by_class.items():
        n_user = user_per_class.get(cls, 0)
        # ceiling: anchors may be at most `class_fraction` of the class total,
        # i.e. anchors <= frac/(1-frac) * user. The floor overrides it, so a
        # class with no hand labels still gets taught.
        ratio_cap = (int(n_user * class_fraction / max(1.0 - class_fraction, 1e-6))
                     if n_user else 0)
        cap = max(anchor_floor, ratio_cap)
        members.sort(key=lambda r: str(r.get("start_ts")), reverse=True)
        if len(members) <= cap:
            kept_anchors.extend(members)
            continue
        buckets: Dict[object, List[dict]] = {}
        for r in members:
            buckets.setdefault(r.get(tm.REGIME_FEATURE, 0), []).append(r)
        picked: List[dict] = []
        while len(picked) < cap and any(buckets.values()):
            for key in list(buckets):
                if len(picked) >= cap:
                    break
                if buckets[key]:
                    picked.append(buckets[key].pop(0))
        dropped += len(members) - len(picked)
        kept_anchors.extend(picked)

    # whole-pool ceiling, applied after the per-class pass
    max_anchor_total = int(len(user_rows) * pool_fraction
                           / max(1.0 - pool_fraction, 1e-6)) if user_rows else 0
    floor_total = anchor_floor * len(by_class)
    max_anchor_total = max(max_anchor_total, floor_total)
    if len(kept_anchors) > max_anchor_total:
        kept_anchors.sort(key=lambda r: str(r.get("start_ts")), reverse=True)
        dropped += len(kept_anchors) - max_anchor_total
        kept_anchors = kept_anchors[:max_anchor_total]

    pool = user_rows + kept_anchors
    _finish_rows(conn, circuit, pool)
    per_class: Dict[str, int] = {}
    for r in pool:
        per_class[r["_y"]] = per_class.get(r["_y"], 0) + 1
    stats = PoolStats(total=len(pool), user=len(user_rows),
                      anchor=len(kept_anchors), per_class=per_class,
                      anchors_dropped=dropped)
    return pool, stats


# ── the referee's two references ────────────────────────────────────────────
def _score_artifact(art: tm.Artifact, rows: Sequence[dict]) -> Score:
    if not rows:
        return Score(0, 0)
    preds = tm.predict_many(art, rows)
    correct = sum(1 for r, (label, _) in zip(rows, preds)
                  if label is not None and label == r["_y"])
    return Score(correct, len(rows))


def split_holdout(pool: Sequence[dict], fraction: float = 0.25
                  ) -> Tuple[List[dict], List[dict]]:
    """Day-grouped split whose holdout carries USER labels only.

    Two invariants, and the interesting part is how they interact.

    DAY-GROUPED. A day is never split, because an appliance cycle's fills are
    near-duplicates of one another, and scoring the challenger on an event it
    effectively memorised is the degenerate comparison V6d measured (champion
    0.0 / challenger 1.0 in every row).

    USER-ONLY HOLDOUT. This holdout is the only measurement of precision
    against human truth anywhere in the loop: ``tm.train`` calibrates the
    serving threshold on it, and it is the referee's recent leg. An anchor row
    in it converts both into machine-vs-machine agreement — the same mistake
    that put "399/399 rule_toilet precision" into the dev47 plan's first
    revision, when the real figure was model-vs-model agreement on unlabelled
    events.

    Satisfying only the second gives a subtly broken split. Filtering anchors
    out of an already-built holdout leaves that day's ANCHOR rows sitting in
    ``train_rows`` while its user rows are scored — reintroducing, through the
    back door, exactly the near-duplicate leak the day-grouping exists to
    prevent. So the partition happens BEFORE the split: an anchor landing on a
    held-out day is DROPPED rather than moved across the boundary. That costs a
    little training signal (anchors are free and capped anyway) and buys an
    honest measurement, which is the scarcer thing.

    Held-out days are chosen among days that actually carry user labels. An
    anchor-only day would otherwise consume a holdout slot and contribute
    nothing to it, shrinking the very sample whose size already binds — the
    tier serves at the 0.90 grid ceiling because the Wilson lower bound at
    n≈200 will not clear the 0.85 target, so every holdout row is coverage.
    """
    def _day(row: dict) -> str:
        return str(row.get("start_ts"))[:10]

    def _is_user(row: dict) -> bool:
        return (row.get("fixture_label_source") or "direct") != ANCHOR_SOURCE

    user_days = sorted({_day(r) for r in pool if _is_user(r)})
    if len(user_days) < 4:
        return list(pool), []
    step = max(int(1 / max(fraction, 1e-6)), 2)
    held_days = set(user_days[::step])
    train = [r for r in pool if _day(r) not in held_days]
    hold = [r for r in pool if _day(r) in held_days and _is_user(r)]
    return train, hold


# ── scoped invalidation (see module docstring) ──────────────────────────────
def scoped_invalidation_ids(conn: sqlite3.Connection, circuit: str,
                            new_threshold: float,
                            trained_at: Optional[str] = None) -> List[str]:
    """Events a freshly-swapped model could plausibly answer differently."""
    ids: List[str] = []
    for r in conn.execute(
            "SELECT id FROM events WHERE circuit = ? "
            "  AND (user_fixture_type IS NULL OR user_fixture_type = '') "
            "  AND (matched_fixture_type IS NULL OR matched_fixture_type = '') "
            "  AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
            "  AND COALESCE(is_low_flow_dribble,0) = 0 "
            "  AND COALESCE(is_cross_talk,0) = 0", (circuit,)):
        ids.append(str(r[0]))
    for r in conn.execute(
            "SELECT id FROM events WHERE circuit = ? "
            "  AND (user_fixture_type IS NULL OR user_fixture_type = '') "
            "  AND matched_fixture_type IS NOT NULL "
            "  AND COALESCE(match_confidence, 0) < ?", (circuit, new_threshold)):
        ids.append(str(r[0]))
    if trained_at:
        for r in conn.execute(
                "SELECT id FROM events WHERE circuit = ? AND start_ts >= ? "
                "  AND (user_fixture_type IS NULL OR user_fixture_type = '')",
                (circuit, trained_at)):
            ids.append(str(r[0]))
    return sorted(set(ids))


def load_benchmark_ids(path: str) -> List[str]:
    """Event ids of the pinned frozen benchmark, if one is configured.

    The benchmark FILE lives outside the repo and outside the add-on's config:
    it is real events with real timestamps, i.e. a record of when this
    household used water. Only its hash is ever quoted. A home without one is
    normal — the referee then runs on the recent holdout alone and declines to
    decide when that is too small (R4a).
    """
    import json as _json
    try:
        with open(path, encoding="utf-8") as fh:
            return [str(x) for x in _json.load(fh).get("event_ids", [])]
    except (OSError, ValueError) as exc:
        log.warning("pinned benchmark %s unreadable (%s); referee will run "
                    "without its primary reference", path, exc)
        return []


# ── the retrain ─────────────────────────────────────────────────────────────
def retrain(conn: sqlite3.Connection, circuit: str, data_dir: str,
            benchmark_ids: Optional[Sequence[str]] = None,
            target_precision: float = tm.DEFAULT_TARGET_PRECISION,
            referee_config: Optional[RefereeConfig] = None,
            apply_invalidation: bool = True,
            reason: str = "") -> RetrainOutcome:
    """Train a challenger and let the referee decide whether it serves.

    Synchronous — the caller submits it through ``run_db`` (46a). Returns an
    outcome rather than raising for the ordinary "not yet" cases: a home below
    the graduation floor, or an image without scikit-learn, are STATES, and the
    k-NN ladder keeps serving in both.
    """
    if not tm.sklearn_available():
        return RetrainOutcome("unavailable",
                              "scikit-learn absent — kNN ladder serves")
    pool, stats = build_training_pool(conn, circuit)
    ok, why = tm.eligible(pool)
    if not ok:
        return RetrainOutcome("ineligible", why, pool=stats)

    bench_ids = {str(x) for x in (benchmark_ids or [])}
    benchmark = [r for r in pool if str(r["id"]) in bench_ids]
    trainable = [r for r in pool if str(r["id"]) not in bench_ids]
    train_rows, holdout = split_holdout(trainable)
    if not train_rows:
        return RetrainOutcome("ineligible", "no trainable rows after splits",
                              pool=stats)

    try:
        challenger = tm.train(train_rows, circuit, holdout=holdout,
                              target_precision=target_precision,
                              notes=reason)
    except tm.TinyModelUnavailable as exc:
        return RetrainOutcome("ineligible", str(exc), pool=stats)

    champion = tm.load(data_dir, circuit)
    if champion is None:
        art_path = tm.save(challenger, data_dir)
        invalidated = 0
        if apply_invalidation:
            invalidated = _invalidate(conn, circuit, challenger)
        log.info("[%s] first tinymodel artifact installed at %s", circuit, art_path)
        return RetrainOutcome("trained", "no incumbent — installed",
                              artifact=challenger, pool=stats,
                              invalidated=invalidated)

    bench_champ = _score_artifact(champion, benchmark)
    bench_chal = _score_artifact(challenger, benchmark)
    recent_champ = _score_artifact(champion, holdout)
    recent_chal = _score_artifact(challenger, holdout)
    verdict = decide(
        benchmark_champion=bench_champ, benchmark_challenger=bench_chal,
        recent_champion=recent_champ, recent_challenger=recent_chal,
        recent_holdout_ids=[str(r["id"]) for r in holdout],
        challenger_pool_ids=[str(r["id"]) for r in train_rows],
        config=referee_config)

    if not verdict.swap:
        log.warning("[%s] challenger %s REJECTED — %s. Incumbent %s keeps "
                    "serving.", circuit, challenger.model_hash, verdict.reason,
                    champion.model_hash)
        return RetrainOutcome("kept", verdict.reason, verdict=verdict,
                              artifact=champion, pool=stats)

    tm.save(challenger, data_dir)
    invalidated = _invalidate(conn, circuit, challenger) if apply_invalidation else 0
    log.info("[%s] challenger %s now serving (%s); %d verdict(s) queued for "
             "re-derivation", circuit, challenger.model_hash, verdict.reason,
             invalidated)
    return RetrainOutcome("trained", verdict.reason, verdict=verdict,
                          artifact=challenger, pool=stats,
                          invalidated=invalidated)


def _invalidate(conn: sqlite3.Connection, circuit: str,
                art: tm.Artifact) -> int:
    from .database import invalidate_verdict_stamps
    ids = scoped_invalidation_ids(conn, circuit, art.threshold, art.trained_at)
    if not ids:
        return 0
    return invalidate_verdict_stamps(conn, ids)
