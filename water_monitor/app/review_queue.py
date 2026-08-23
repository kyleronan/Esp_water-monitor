"""dev47 (47d) — the weekly review card: what the add-on asks, and how little.

THE CONTRACT
------------
A home should need roughly a hundred labels in its first weeks and then run for
years with an occasional confirmation. That only holds if the asking is bounded
and useful, so this module answers one question: given everything the ladder
could not confidently type this week, which TWELVE events are worth a person's
attention?

WHY A HARD CAP
--------------
Early in a home's life the abstain rate is high — a fifty-event card is not a
review queue, it is the daily chore this design exists to remove. The cap is 12,
composed as **10 identity questions + 2 anchor spot-checks**, and it does not
flex with backlog size. A backlog that outruns the card is fine: the events keep
their machine verdicts and the model improves from the ten answers it does get.

WHY UNCERTAINTY *AND* DIVERSITY
-------------------------------
Pure uncertainty sampling collapses onto whatever class is currently hardest —
ten near-identical dishwasher fills teach the model almost nothing beyond the
first. Diversity round-robins across the model's guessed classes so the card
spans the confusion rather than one corner of it.

WHY ANCHOR SPOT-CHECKS SHARE THE CAP
------------------------------------
The cycle detectors teach the model (their claims become training exemplars)
and are deliberately excluded from every eval denominator, because scoring a
model against its own teacher is circular. That leaves no measurement of the
teacher at all — so two claimed cycle members ride the card each week as the
only live check on anchor precision. They cost identity slots on purpose: a
silently-drifting teacher is worse than one unanswered question.

"NOT SURE" IS A REAL ANSWER
---------------------------
Distinct from 'other'. 'other' means *confirmed not any listed fixture* and is
a trainable class; "not sure" records nothing and never enters training.
Without that distinction 'other' silently becomes "whatever the model was
unsure about", redefined at every retrain — a class that means something
different every week is worse than no class.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

CARD_CAP: int = 12
IDENTITY_SLOTS: int = 10
ANCHOR_SLOTS: int = 2

# Events younger than this may still gain burst context from siblings that have
# not happened yet (47a), so asking about them now risks asking about an event
# the add-on is about to answer correctly by itself.
MATURITY_WINDOW_S: float = 2 * 3600.0

# There is deliberately no "not sure" ANSWER constant here. An earlier draft
# carried ANSWER_NOT_SURE plus an answer_is_trainable() guard, for an inline
# answering flow this card intentionally does not have — it points at History
# instead, because two ways to label one event is how they drift apart. The
# guard those symbols promised is real, but it lives in the schema, not here:
# History's Ignore sets `user_ignored`, load_candidates() excludes it from the
# card, and it folds into `excluded_from_training`, which build_training_pool()
# filters on. Keeping an unused copy here made the guard look absent while it
# was working, so it is gone; this note is what it replaced.

KIND_IDENTITY = "identity"
KIND_ANCHOR = "anchor_check"


@dataclass
class CardItem:
    event_id: str
    kind: str
    start_ts: str
    volume_litres: Optional[float]
    duration_seconds: Optional[float]
    proposed: Optional[str] = None
    confidence: Optional[float] = None
    via: Optional[str] = None
    rationale: str = ""

    def as_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class ReviewCard:
    circuit: str
    generated_at: str
    items: List[CardItem] = field(default_factory=list)
    n_candidates: int = 0
    n_anchor_candidates: int = 0
    truncated: int = 0

    def as_dict(self) -> dict:
        return {"circuit": self.circuit, "generated_at": self.generated_at,
                "items": [i.as_dict() for i in self.items],
                "n_candidates": self.n_candidates,
                "n_anchor_candidates": self.n_anchor_candidates,
                "truncated": self.truncated}


def _cutoff(now: Optional[datetime] = None) -> str:
    return ((now or datetime.now(timezone.utc))
            - timedelta(seconds=MATURITY_WINDOW_S)).isoformat()


def load_candidates(conn: sqlite3.Connection, circuit: str,
                    since: Optional[str] = None,
                    now: Optional[datetime] = None) -> List[dict]:
    """Unlabelled, mature, non-artifact events the ladder could not settle.

    "Could not settle" is either an outright abstention (no stored fixture
    type) or a verdict the tier itself flagged as low confidence. Artifact rows
    are excluded because they moved no real water and asking about them wastes
    the scarcest resource here, which is the operator's attention.
    """
    params: list = [circuit, _cutoff(now)]
    where = ("WHERE circuit = ? AND start_ts <= ? "
             "AND (user_fixture_type IS NULL OR user_fixture_type = '') "
             "AND COALESCE(user_ignored,0) = 0 "
             "AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
             "AND COALESCE(is_low_flow_dribble,0) = 0 "
             "AND COALESCE(is_cross_talk,0) = 0")
    if since:
        where += " AND start_ts >= ?"
        params.append(since)
    return [dict(r) for r in conn.execute(
        "SELECT id, start_ts, volume_litres, duration_seconds, "
        "       matched_fixture_type, matched_via, match_confidence "
        f"FROM events {where} ORDER BY start_ts DESC", params)]


def load_anchor_candidates(conn: sqlite3.Connection, circuit: str,
                           since: Optional[str] = None,
                           now: Optional[datetime] = None) -> List[dict]:
    """Cycle-detector claims eligible for a spot-check.

    Only members CLAIMED by a cycle detector qualify — the point is to measure
    the teacher, not the model.
    """
    params: list = [circuit, _cutoff(now)]
    where = ("WHERE circuit = ? AND start_ts <= ? "
             "AND (user_fixture_type IS NULL OR user_fixture_type = '') "
             "AND matched_via IN ('dishwasher_cycle','washer_cycle', "
             "                    'softener_session') "
             "AND COALESCE(user_ignored,0) = 0")
    if since:
        where += " AND start_ts >= ?"
        params.append(since)
    return [dict(r) for r in conn.execute(
        "SELECT id, start_ts, volume_litres, duration_seconds, "
        "       matched_fixture_type, matched_via, match_confidence "
        f"FROM events {where} ORDER BY start_ts DESC", params)]


def select_identity_items(candidates: Sequence[dict],
                          limit: int = IDENTITY_SLOTS) -> List[CardItem]:
    """Uncertainty-first, round-robined across proposed classes.

    Sorting by confidence alone would fill the card with one class; the
    round-robin spends the ten questions across the confusion instead. Events
    with no proposal at all (a full abstention) are their own bucket and get a
    fair share, because "the add-on has no idea" is exactly the case where an
    answer teaches the most.
    """
    buckets: Dict[str, List[dict]] = {}
    for c in sorted(candidates,
                    key=lambda c: (c.get("match_confidence") is not None,
                                   c.get("match_confidence") or 0.0)):
        key = c.get("matched_fixture_type") or "<abstained>"
        buckets.setdefault(key, []).append(c)
    order = sorted(buckets, key=lambda k: (k != "<abstained>", -len(buckets[k])))
    picked: List[CardItem] = []
    while len(picked) < limit and any(buckets[k] for k in order):
        for key in order:
            if len(picked) >= limit:
                break
            if not buckets[key]:
                continue
            c = buckets[key].pop(0)
            conf = c.get("match_confidence")
            picked.append(CardItem(
                event_id=str(c["id"]), kind=KIND_IDENTITY,
                start_ts=str(c["start_ts"]),
                volume_litres=c.get("volume_litres"),
                duration_seconds=c.get("duration_seconds"),
                proposed=c.get("matched_fixture_type"),
                confidence=conf, via=c.get("matched_via"),
                rationale=("no confident match" if conf is None
                           else f"low confidence ({conf:.2f})")))
    return picked


def select_anchor_items(candidates: Sequence[dict],
                        limit: int = ANCHOR_SLOTS) -> List[CardItem]:
    """Spread the spot-checks across detectors, newest first."""
    buckets: Dict[str, List[dict]] = {}
    for c in candidates:
        buckets.setdefault(str(c.get("matched_via")), []).append(c)
    picked: List[CardItem] = []
    while len(picked) < limit and any(buckets.values()):
        for key in sorted(buckets):
            if len(picked) >= limit:
                break
            if buckets[key]:
                c = buckets[key].pop(0)
                picked.append(CardItem(
                    event_id=str(c["id"]), kind=KIND_ANCHOR,
                    start_ts=str(c["start_ts"]),
                    volume_litres=c.get("volume_litres"),
                    duration_seconds=c.get("duration_seconds"),
                    proposed=c.get("matched_fixture_type"),
                    confidence=c.get("match_confidence"),
                    via=c.get("matched_via"),
                    rationale=f"spot-check of {key} — the only live "
                              "measurement of anchor precision"))
    return picked


def build_card(conn: sqlite3.Connection, circuit: str,
               since: Optional[str] = None,
               now: Optional[datetime] = None) -> ReviewCard:
    """Assemble one week's card. Synchronous; submit via ``run_db`` (46a)."""
    candidates = load_candidates(conn, circuit, since, now)
    anchors = load_anchor_candidates(conn, circuit, since, now)
    anchor_items = select_anchor_items(anchors)
    identity_items = select_identity_items(
        candidates, limit=CARD_CAP - len(anchor_items))
    card = ReviewCard(
        circuit=circuit,
        generated_at=(now or datetime.now(timezone.utc)).isoformat(),
        items=identity_items + anchor_items,
        n_candidates=len(candidates), n_anchor_candidates=len(anchors),
        truncated=max(len(candidates) - len(identity_items), 0))
    if card.truncated:
        log.info("[%s] review card: %d of %d candidates shown (cap %d); the "
                 "rest keep their machine verdicts", circuit,
                 len(identity_items), len(candidates), CARD_CAP)
    return card
