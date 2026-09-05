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

# dev51 (1.5) — machine-propagated 'cycle' labels are ANCHORS, not user truth.
# They enter the anchor half of the pool (capped, per-tier precision gate,
# dropped from held-out days) instead of the user half, which also revives the
# label-free growth the loop was designed around: the pool hash can now move
# without a human labelling anything. Guarded by the live pre-check (≥ 100
# genuinely human pool-eligible labels; circuit_1 had 768 on 2026-09-01). If a
# home falls under that floor the one-line fallback is to set this False: the
# holdout still excludes 'cycle' (that never depends on this flag) and the
# pool treats 'cycle' as it did before.
DEMOTE_CYCLE_TO_ANCHOR: bool = True


def pool_machine_sources() -> tuple:
    """Sources that partition into the ANCHOR half of the training pool."""
    return tm.MACHINE_LABEL_SOURCES if DEMOTE_CYCLE_TO_ANCHOR else (ANCHOR_SOURCE,)


# A referee that rejects this many weekly challengers in a row has stopped
# improving. The V6d finding was that this state is SILENT — a frozen champion
# serves exactly like a healthy one — so the scheduler logs a WARNING and the
# Water Use page shows a banner. It never relaxes the referee.
STALL_STREAK: int = 4

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
    # dev51 — what the referee actually measured. Carried here so the ledger
    # (S2) can record it without re-plumbing the retrain.
    benchmark_hash: Optional[str] = None
    benchmark_requested_n: int = 0        # ids the import asked for
    benchmark_matched_n: int = 0          # of those, present in tonight's pool
    recent_holdout_n: int = 0             # day-grouped holdout, pre-cleaning
    recent_clean_n: int = 0               # rows on days NEITHER model trained on
    coverage_delta: Optional[float] = None    # challenger − champion, serving thr
    scores: Optional[dict] = None         # argmax + serving-threshold score sets
    challenger_hash: Optional[str] = None
    champion_hash: Optional[str] = None   # the incumbent BEFORE this decision

    @property
    def swapped(self) -> bool:
        return self.status in ("trained", "rolled_back")

    def as_dict(self) -> dict:
        return {"status": self.status, "reason": self.reason,
                "model_hash": self.artifact.model_hash if self.artifact else None,
                "threshold": self.artifact.threshold if self.artifact else None,
                "pool": self.pool.as_dict() if self.pool else None,
                "invalidated": self.invalidated,
                "referee": self.verdict.describe() if self.verdict else None,
                "challenger_hash": self.challenger_hash,
                "champion_hash": self.champion_hash,
                "benchmark_hash": self.benchmark_hash,
                "benchmark_requested_n": self.benchmark_requested_n,
                "benchmark_matched_n": self.benchmark_matched_n,
                "recent_holdout_n": self.recent_holdout_n,
                "recent_clean_n": self.recent_clean_n,
                "coverage_delta": self.coverage_delta,
                "scores": self.scores}


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

    machine = pool_machine_sources()

    def _is_anchor(r: dict) -> bool:
        return (r["fixture_label_source"] or "direct") in machine

    user_rows = [r for r in rows if not _is_anchor(r)]
    eligible_tiers = anchor_eligible_tiers(conn, circuit,
                                           since_ts=anchor_since_ts)
    anchor_rows = [r for r in rows
                   if _is_anchor(r)
                   and (r.get("matched_via") or "") in eligible_tiers]
    rejected_anchors = sum(
        1 for r in rows
        if _is_anchor(r) and (r.get("matched_via") or "") not in eligible_tiers)

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
def _score_artifact(art: tm.Artifact, rows: Sequence[dict],
                    threshold: Optional[float] = None) -> Score:
    """Accuracy with abstention counted as wrong.

    ``threshold=None`` scores at the artifact's own serving threshold — the
    number the operator experiences. ``threshold=0.0`` scores at argmax, which
    is what the referee compares (dev51): two artifacts that chose different
    serving thresholds differ in COVERAGE, and on this metric a coverage gap
    reads as a quality gap. The logged 0.096 "regression" that froze the
    champion for weeks was exactly that.
    """
    if not rows:
        return Score(0, 0)
    preds = tm.predict_many(art, rows, threshold=threshold)
    correct = sum(1 for r, (label, _) in zip(rows, preds)
                  if label is not None and label == r["_y"])
    return Score(correct, len(rows))


def _pack_score(s: Score) -> dict:
    return {"correct": s.correct, "total": s.total, "rate": round(s.rate, 4)}


def _serving_meta(art: tm.Artifact) -> dict:
    # ``achieved_precision``/``coverage`` are None when choose_threshold fell
    # back (nothing cleared the target) — that is a real state, kept nullable.
    return {"threshold": art.threshold,
            "achieved_precision": art.achieved_precision,
            "coverage": art.coverage_at_threshold,
            "threshold_fell_back": art.achieved_precision is None}


def score_sets(champion: tm.Artifact, challenger: tm.Artifact,
               benchmark: Sequence[dict], holdout: Sequence[dict]) -> dict:
    """Both score sets, both legs, both models.

    ``argmax`` is what the referee decides on. ``serving`` is what previous
    audits measured (the 71.7% agreement baseline was taken at serving
    threshold), kept so the two stay comparable in the ledger.
    """
    out: dict = {"argmax": {}, "serving": {}}
    for leg, rows in (("benchmark", benchmark), ("recent", holdout)):
        out["argmax"][leg] = {
            "n": len(rows),
            "champion": _pack_score(_score_artifact(champion, rows, threshold=0.0)),
            "challenger": _pack_score(_score_artifact(challenger, rows, threshold=0.0)),
        }
        out["serving"][leg] = {
            "n": len(rows),
            "champion": _pack_score(_score_artifact(champion, rows)),
            "challenger": _pack_score(_score_artifact(challenger, rows)),
        }
    out["serving"]["champion_threshold"] = _serving_meta(champion)
    out["serving"]["challenger_threshold"] = _serving_meta(challenger)
    return out


def coverage_delta(champion: tm.Artifact, challenger: tm.Artifact
                   ) -> Optional[float]:
    """Challenger minus champion coverage at their serving thresholds; None
    when either artifact's threshold calibration fell back. A warning signal
    for the ledger — never a referee input."""
    a, b = champion.coverage_at_threshold, challenger.coverage_at_threshold
    if a is None or b is None:
        return None
    return round(float(b) - float(a), 4)


def clean_recent_holdout(holdout: Sequence[dict], champion: tm.Artifact,
                         challenger: tm.Artifact) -> Tuple[List[dict], str]:
    """Holdout rows on days NEITHER model trained on (dev51).

    The recent leg used to score the champion on the same holdout as the
    challenger — but the champion was fitted on an earlier pool that generally
    INCLUDED those days (a different day stride, more labels since), while the
    challenger is holdout-free by construction. The leak check only guarded
    the challenger's side, so the incumbent was scored on memorised days and
    won every night. Day-granular because leakage is day-level — the same
    reason ``split_holdout`` groups by day.

    Legacy fallback: an artifact from before ``train_days`` existed reports an
    empty list, so the only honest "days it did not see" are days strictly
    after it was trained. On the FIRST retrain after this ships that set is
    normally EMPTY (the challenger trained on everything up to today), the leg
    abstains, and the referee keeps the incumbent — a known property of the
    first cycle, not a fault.
    """
    def _day(r: dict) -> str:
        return str(r.get("start_ts"))[:10]

    chal_days = set(challenger.train_days or [])
    if champion.train_days:
        champ_days = set(champion.train_days)
        clean = [r for r in holdout
                 if _day(r) not in champ_days and _day(r) not in chal_days]
        return clean, "train_days"
    cutoff = (champion.trained_at or "")[:10]
    clean = [r for r in holdout if _day(r) > cutoff and _day(r) not in chal_days]
    return clean, f"legacy champion — days after {cutoff or 'unknown'}"


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
        # dev51: 'cycle' rows are machine labels and never measure the model —
        # regardless of DEMOTE_CYCLE_TO_ANCHOR, which governs the POOL only.
        return not tm.is_machine_label(row)

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


def parse_benchmark_payload(data) -> Tuple[List[str], Optional[str]]:
    """``(event_ids, benchmark_hash)`` from a pinned-benchmark JSON document.

    ONE parser for the file loader and the Dev Tools import, so the two cannot
    drift. Ids are de-duplicated in first-seen order; the hash is whatever the
    document declares (the eval harness writes ``benchmark_hash``).
    """
    if not isinstance(data, dict):
        raise ValueError("benchmark JSON must be an object with an 'event_ids' list")
    raw = data.get("event_ids")
    if not isinstance(raw, list):
        raise ValueError("benchmark JSON has no 'event_ids' list")
    ids: List[str] = []
    seen: set = set()
    for x in raw:
        s = str(x).strip()
        if s and s not in seen:
            seen.add(s)
            ids.append(s)
    h = data.get("benchmark_hash")
    return ids, (str(h) if h else None)


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
            ids, _ = parse_benchmark_payload(_json.load(fh))
            return ids
    except (OSError, ValueError) as exc:
        log.warning("pinned benchmark %s unreadable (%s); referee will run "
                    "without its primary reference", path, exc)
        return []


_NO_BENCHMARK: dict = {"ids": [], "source_hash": None, "requested_n": 0}


def benchmark_ids_for_circuit(conn: Optional[sqlite3.Connection],
                              circuit: str) -> dict:
    """The imported benchmark for one circuit, from ``referee_benchmark``.

    ``{"ids": [...], "source_hash": ..., "requested_n": ...}``; the empty shape
    when nothing was imported, the table predates the DB, or ``conn`` is None.
    Empty ids make the referee's benchmark leg abstain — which (dev51) keeps
    the incumbent rather than promoting an unmeasured challenger.
    """
    if conn is None:
        return dict(_NO_BENCHMARK)
    try:
        ids = [str(r[0]) for r in conn.execute(
            "SELECT event_id FROM referee_benchmark WHERE circuit = ? "
            "ORDER BY event_id", (circuit,))]
        meta = conn.execute(
            "SELECT source_hash, requested_n FROM referee_benchmark_meta "
            "WHERE circuit = ?", (circuit,)).fetchone()
    except sqlite3.Error as exc:
        log.warning("[%s] referee benchmark unavailable (%s); benchmark leg "
                    "abstains", circuit, exc)
        return dict(_NO_BENCHMARK)
    if not ids:
        return dict(_NO_BENCHMARK)
    return {"ids": ids,
            "source_hash": meta[0] if meta else None,
            "requested_n": int(meta[1]) if meta else len(ids)}


def import_referee_benchmark(conn: sqlite3.Connection, circuit: str,
                             payload, now: Optional[str] = None) -> dict:
    """Replace the circuit's benchmark with the ids in ``payload``.

    Delete-then-insert on BOTH tables in one transaction, so a failed import
    leaves the previous benchmark intact rather than half of each. The
    document's ``benchmark_hash`` becomes ``source_hash`` — the one value the
    ledger quotes — and an unidentified document is refused: a benchmark whose
    provenance cannot be named is not a reference.
    """
    ids, source_hash = parse_benchmark_payload(payload)
    if not ids:
        raise ValueError("benchmark has no event ids")
    if not source_hash:
        raise ValueError("benchmark JSON carries no 'benchmark_hash' — "
                         "refusing to import an unidentified reference")
    stamp = now or datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("DELETE FROM referee_benchmark WHERE circuit = ?", (circuit,))
        conn.execute("DELETE FROM referee_benchmark_meta WHERE circuit = ?",
                     (circuit,))
        cur = conn.executemany(
            "INSERT INTO referee_benchmark (circuit, event_id, source_hash, "
            "imported_at) VALUES (?, ?, ?, ?)",
            [(circuit, i, source_hash, stamp) for i in ids])
        inserted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 \
            else len(ids)
        conn.execute(
            "INSERT INTO referee_benchmark_meta (circuit, source_hash, "
            "requested_n, imported_at) VALUES (?, ?, ?, ?)",
            (circuit, source_hash, len(ids), stamp))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    log.info("[%s] referee benchmark imported: %d id(s), hash %s", circuit,
             inserted, source_hash)
    return {"circuit": circuit, "source_hash": source_hash,
            "requested_n": len(ids), "inserted_n": int(inserted)}


# ── the retrain ─────────────────────────────────────────────────────────────
def retrain(conn: sqlite3.Connection, circuit: str, data_dir: str,
            benchmark_ids: Optional[Sequence[str]] = None,
            target_precision: float = tm.DEFAULT_TARGET_PRECISION,
            referee_config: Optional[RefereeConfig] = None,
            apply_invalidation: bool = True,
            reason: str = "",
            benchmark_meta: Optional[dict] = None) -> RetrainOutcome:
    """Train a challenger and let the referee decide whether it serves.

    Synchronous — the caller submits it through ``run_db`` (46a). Returns an
    outcome rather than raising for the ordinary "not yet" cases: a home below
    the graduation floor, or an image without scikit-learn, are STATES, and the
    k-NN ladder keeps serving in both.

    ``benchmark_meta`` (dev51) is the import record behind ``benchmark_ids`` —
    ``source_hash`` and ``requested_n`` — carried onto the outcome so a retrain
    can say how much of the pinned reference actually met tonight's pool.
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
    meta = benchmark_meta or {}
    bench_hash = meta.get("source_hash")
    requested_n = int(meta.get("requested_n") or len(bench_ids))
    bench_fields = dict(benchmark_hash=bench_hash,
                        benchmark_requested_n=requested_n,
                        benchmark_matched_n=len(benchmark))
    if bench_ids:
        # Quarantine, exclusion or deletion can shrink the leg silently; say so.
        log.info("[%s] referee benchmark: %d requested, %d matched the training "
                 "pool (hash %s)%s", circuit, requested_n, len(benchmark),
                 bench_hash or "?",
                 "" if len(benchmark) == requested_n
                 else " — the rest are quarantined, excluded or gone")
    else:
        log.info("[%s] referee benchmark: none imported — benchmark leg "
                 "abstains", circuit)

    train_rows, holdout = split_holdout(trainable)
    if not train_rows:
        return RetrainOutcome("ineligible", "no trainable rows after splits",
                              pool=stats, **bench_fields)

    try:
        challenger = tm.train(train_rows, circuit, holdout=holdout,
                              target_precision=target_precision,
                              notes=reason)
    except tm.TinyModelUnavailable as exc:
        return RetrainOutcome("ineligible", str(exc), pool=stats, **bench_fields)

    champion = tm.load(data_dir, circuit)
    if champion is None:
        art_path = tm.save(challenger, data_dir)
        invalidated = 0
        if apply_invalidation:
            invalidated = _invalidate(conn, circuit, challenger)
        log.info("[%s] first tinymodel artifact installed at %s", circuit, art_path)
        return RetrainOutcome("trained", "no incumbent — installed",
                              artifact=challenger, pool=stats,
                              invalidated=invalidated,
                              recent_holdout_n=len(holdout),
                              challenger_hash=challenger.model_hash,
                              **bench_fields)

    # dev51 — symmetric leak fix: score the recent leg only on days NEITHER
    # model trained on. See clean_recent_holdout for why the old comparison
    # was one-sided.
    clean_holdout, basis = clean_recent_holdout(holdout, champion, challenger)
    log.info("[%s] referee recent leg: holdout %d row(s), %d clean of both "
             "models' training days (%s)", circuit, len(holdout),
             len(clean_holdout), basis)

    # dev51 — threshold-fair scoring: the referee compares DISCRIMINATION
    # (argmax), never coverage. Serving precision stays choose_threshold's
    # contract; the serving-threshold scores are kept alongside for the ledger.
    # dev51 (gate finding, 2026-09-04) — the benchmark is RESERVED from
    # training as of dev51, but an incumbent fitted BEFORE that rule (no
    # train_days recorded) trained on those very rows and has memorised them:
    # the V6d gate measured a ~14-point head start on a set it had already
    # seen. Scoring such a champion on the benchmark is not a comparison, so
    # the leg abstains until the first dev51 promotion installs a clean one;
    # the recent leg (clean days after the champion's fit) decides meanwhile.
    bench_for_referee = benchmark
    if benchmark and not getattr(champion, "train_days", None):
        log.info("[%s] referee benchmark leg abstains: incumbent predates the "
                 "reserved benchmark (fitted %s, no training-day record) and "
                 "trained on those rows — the recent leg decides until the first "
                 "promotion", circuit, champion.trained_at)
        bench_for_referee = []
    bench_champ = _score_artifact(champion, bench_for_referee, threshold=0.0)
    bench_chal = _score_artifact(challenger, bench_for_referee, threshold=0.0)
    recent_champ = _score_artifact(champion, clean_holdout, threshold=0.0)
    recent_chal = _score_artifact(challenger, clean_holdout, threshold=0.0)
    verdict = decide(
        benchmark_champion=bench_champ, benchmark_challenger=bench_chal,
        recent_champion=recent_champ, recent_challenger=recent_chal,
        recent_holdout_ids=[str(r["id"]) for r in clean_holdout],
        challenger_pool_ids=[str(r["id"]) for r in train_rows],
        config=referee_config)

    sets = score_sets(champion, challenger, benchmark, clean_holdout)
    cov = coverage_delta(champion, challenger)
    if cov is not None and cov < -0.05:
        log.warning("[%s] challenger covers %.1f pts LESS than the incumbent at "
                    "its serving threshold (%.2f vs %.2f) — a warning, not a "
                    "veto", circuit, -100 * cov, challenger.threshold,
                    champion.threshold)
    measured = dict(recent_holdout_n=len(holdout),
                    recent_clean_n=len(clean_holdout),
                    coverage_delta=cov, scores=sets,
                    challenger_hash=challenger.model_hash,
                    champion_hash=champion.model_hash, **bench_fields)

    if not verdict.swap:
        log.warning("[%s] challenger %s REJECTED — %s. Incumbent %s keeps "
                    "serving.", circuit, challenger.model_hash, verdict.reason,
                    champion.model_hash)
        return RetrainOutcome("kept", verdict.reason, verdict=verdict,
                              artifact=champion, pool=stats, **measured)

    tm.save(challenger, data_dir)
    invalidated = _invalidate(conn, circuit, challenger) if apply_invalidation else 0
    log.info("[%s] challenger %s now serving (%s); %d verdict(s) queued for "
             "re-derivation", circuit, challenger.model_hash, verdict.reason,
             invalidated)
    return RetrainOutcome("trained", verdict.reason, verdict=verdict,
                          artifact=challenger, pool=stats,
                          invalidated=invalidated, **measured)


def _invalidate(conn: sqlite3.Connection, circuit: str,
                art: tm.Artifact) -> int:
    from .database import invalidate_verdict_stamps
    ids = scoped_invalidation_ids(conn, circuit, art.threshold, art.trained_at)
    if not ids:
        return 0
    return invalidate_verdict_stamps(conn, ids)


# ── the decision ledger (dev51, 1.6) ────────────────────────────────────────
# The jobs table prunes finished rows after two days, so "the referee has
# rejected every challenger for a month" left no trace anywhere. This is the
# record: one row per decision, never pruned, read by the weekly cadence, the
# Water Use page and the audit.
def _iso_week(ts: str) -> Optional[str]:
    try:
        return datetime.fromisoformat(str(ts)).strftime("%G-W%V")
    except (TypeError, ValueError):
        return None


def record_retrain_decision(conn: Optional[sqlite3.Connection], circuit: str,
                            trigger: str, out: RetrainOutcome,
                            now: Optional[str] = None) -> Optional[int]:
    """Append one ledger row. Best-effort by design: a ledger write must never
    turn a completed retrain into a failed one, so DB errors log and return
    None. ``conn`` may be None (the scheduler's tests stub the DB hop)."""
    if conn is None:
        return None
    import json as _json
    try:
        cur = conn.execute(
            "INSERT INTO retrain_ledger (circuit, decided_at, trigger, status, swap, "
            " challenger_hash, champion_hash, reason, benchmark_hash, benchmark_n, "
            " detail_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (circuit, now or datetime.now(timezone.utc).isoformat(), trigger,
             out.status, 1 if out.swapped else 0, out.challenger_hash,
             out.champion_hash, out.reason, out.benchmark_hash,
             out.benchmark_matched_n, _json.dumps(out.as_dict(), default=str)))
        conn.commit()
        return int(cur.lastrowid)
    except sqlite3.Error as exc:
        log.warning("[%s] retrain ledger write failed (non-fatal): %s", circuit, exc)
        return None


def weekly_retrain_recorded(conn: Optional[sqlite3.Connection],
                            iso_week: str) -> bool:
    """Has the WEEKLY pass already run in ``iso_week``, per the ledger?

    This is what makes the cadence restart-proof: the in-memory marker is lost
    on every redeploy, which is why production re-judged the same challenger on
    consecutive nights.
    """
    if conn is None:
        return False
    try:
        row = conn.execute(
            "SELECT decided_at FROM retrain_ledger WHERE trigger = 'weekly' "
            "ORDER BY id DESC LIMIT 1").fetchone()
    except sqlite3.Error:
        return False
    return bool(row) and _iso_week(row[0]) == iso_week


def learning_status(conn: Optional[sqlite3.Connection], circuit: str,
                    data_dir: Optional[str] = None) -> dict:
    """What the operator should be able to see about the loop, from the ledger.

    ``available`` is False when there is no decision on record (fresh install,
    pre-migration DB, or a home that has never been eligible) — the page then
    says nothing rather than something empty.
    """
    import json as _json
    out: dict = {"available": False}
    if conn is None:
        return out
    try:
        rows = conn.execute(
            "SELECT decided_at, trigger, status, swap, challenger_hash, "
            "       champion_hash, reason, detail_json FROM retrain_ledger "
            "WHERE circuit = ? ORDER BY id DESC LIMIT 60", (circuit,)).fetchall()
    except sqlite3.Error:
        return out
    if not rows:
        return out
    last = rows[0]
    streak = 0
    for r in rows:                              # newest first
        if r[2] == "kept":
            streak += 1
        elif r[2] in ("ineligible", "unavailable"):
            continue                            # not a decision about a challenger
        else:
            break
    last_swap_at = next((r[0] for r in rows if r[3]), None)
    days_since_swap = None
    if last_swap_at:
        try:
            dt = datetime.fromisoformat(str(last_swap_at))
            days_since_swap = max((datetime.now(timezone.utc) - dt).days, 0)
        except (TypeError, ValueError):
            pass
    try:
        detail = _json.loads(last[7] or "{}")
    except (TypeError, ValueError):
        detail = {}
    cov = detail.get("coverage_delta")
    serving_hash = None
    if data_dir:
        try:
            art = tm.load(data_dir, circuit)
            serving_hash = art.model_hash if art else None
        except Exception:                                   # noqa: BLE001
            serving_hash = None
    out.update({
        "available": True,
        "last_decided_at": last[0], "last_trigger": last[1],
        "last_status": last[2], "last_swapped": bool(last[3]),
        "last_reason": last[6],
        "kept_streak": streak, "stalled": streak >= STALL_STREAK,
        "stall_streak": STALL_STREAK,
        "last_swap_at": last_swap_at, "days_since_swap": days_since_swap,
        # after a swap the challenger serves (a rollback records the restored
        # model as the incumbent, with no challenger); after a keep the
        # incumbent does.
        "serving_hash": (serving_hash
                         or ((last[4] or last[5]) if last[3] else (last[5] or last[4]))),
        "coverage_delta": cov,
        "coverage_warning": (cov is not None and cov < -0.05),
        "benchmark_n": detail.get("benchmark_matched_n"),
        "decisions_on_record": len(rows),
    })
    return out


def rollback_serving_model(conn: sqlite3.Connection, circuit: str,
                           data_dir: str) -> dict:
    """Promote the retained previous artifact back to serving (dev51, 2.2).

    ``tm.rollback`` had existed since dev47 with no caller. Now that swaps can
    actually happen, an undo earns its keep: file swap, the same scoped
    invalidation a promotion performs (so verdicts the rolled-back model
    would answer differently are re-derived), and a ledger row so the
    operator's action is on the record beside the referee's.
    """
    art = tm.rollback(data_dir, circuit)
    if art is None:
        return {"status": "nothing_to_roll_back",
                "reason": "no retained previous model for this circuit"}
    invalidated = _invalidate(conn, circuit, art)
    out = RetrainOutcome("rolled_back", "operator rollback (Dev Tools)",
                         artifact=art, invalidated=invalidated,
                         champion_hash=art.model_hash)
    record_retrain_decision(conn, circuit, "rollback", out)
    log.warning("[%s] rolled back to tinymodel %s (trained %s); %d verdict(s) "
                "queued for re-derivation", circuit, art.model_hash,
                art.trained_at, invalidated)
    return {"status": "rolled_back", "model_hash": art.model_hash,
            "trained_at": art.trained_at, "invalidated": invalidated}
