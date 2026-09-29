"""The weekly review card: what the add-on asks, and how little.

THE CONTRACT
------------
A home should need roughly a hundred labels in its first weeks and then run for
years with an occasional confirmation. So the card asks about at most TEN
events a week, picked from the last seven days, and only events the app is
less than ``REVIEW_MIN_CONFIDENCE`` sure of. As the model learns, fewer events
fall below the bar; what is left is the genuinely odd — a pressure washer on
the hose bib for the first time.

WHY THE CARD IS FROZEN FOR THE WEEK
-----------------------------------
The picks are stored once a week (``refresh_card``, from the nightly pass) and
the card only ever SHRINKS until the next one: answered, ignored, or since
named confidently by a newer model. Re-picking on every read backfilled each
answer from the backlog, so the card never went away.

WHAT "BELOW THE BAR" MEANS PER TIER
-----------------------------------
Only the model tier records a per-event confidence. The structural tiers (rules,
cycle detectors, fingerprint) are hard-gated and count as settled. The k-NN is
the fallback the ladder reaches only after the model declined — below its own
serving threshold — so its verdicts are below the bar by construction, and so
is an event no tier could name.

WHY UNCERTAINTY *AND* DIVERSITY
-------------------------------
Pure uncertainty sampling collapses onto whatever class is currently hardest —
ten near-identical dishwasher fills teach the model almost nothing beyond the
first. Diversity round-robins across the proposed classes so the card spans the
confusion rather than one corner of it.

"NOT SURE" IS A REAL ANSWER
---------------------------
Distinct from 'other'. 'other' means *confirmed not any listed fixture* and is
a trainable class; "not sure" records nothing and never enters training.
Without that distinction 'other' silently becomes "whatever the model was
unsure about", redefined at every retrain — a class that means something
different every week is worse than no class.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

CARD_CAP: int = 10
WINDOW_DAYS: int = 7
REVIEW_MIN_CONFIDENCE: float = 0.80

# A new card is due this long after the last one. An hour short of a week so
# the nightly pass's start-time jitter cannot push a card to day eight.
_CARD_PERIOD = timedelta(days=WINDOW_DAYS) - timedelta(hours=1)

# Events younger than this may still gain burst context from siblings that have
# not happened yet, so asking about them now risks asking about an event
# the add-on is about to answer correctly by itself.
MATURITY_WINDOW_S: float = 2 * 3600.0

# Verdicts reached only after the model declined (see the module docstring).
_FALLBACK_TIERS = ("knn", "knn_invariant")

# There is deliberately no "not sure" ANSWER constant here. This card has no
# inline answering flow; it points at History instead, because two ways to
# label one event is how they drift apart. History's Ignore sets
# `user_ignored`, which this predicate excludes, and it folds into
# `excluded_from_training`, which build_training_pool() filters on.
_BELOW_BAR_WHERE = (
    "circuit = ? "
    "AND (user_fixture_type IS NULL OR user_fixture_type = '') "
    "AND COALESCE(user_ignored,0) = 0 "
    "AND COALESCE(excluded_from_training,0) = 0 "
    "AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
    "AND COALESCE(is_low_flow_dribble,0) = 0 "
    "AND COALESCE(is_cross_talk,0) = 0 "
    "AND (matched_fixture_type IS NULL OR matched_fixture_type = '' "
    f"     OR matched_via IN ({','.join('?' * len(_FALLBACK_TIERS))}) "
    "     OR (matched_via = 'tinymodel' AND COALESCE(match_confidence,0) < ?))")
_COLS = ("id, start_ts, volume_litres, duration_seconds, "
         "matched_fixture_type, matched_via, match_confidence")


@dataclass
class CardItem:
    event_id: str
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
    generated_at: Optional[str]
    items: List[CardItem] = field(default_factory=list)
    n_candidates: int = 0          # the below-bar pool the picks came from
    truncated: int = 0

    def as_dict(self) -> dict:
        return {"circuit": self.circuit, "generated_at": self.generated_at,
                "items": [i.as_dict() for i in self.items],
                "n_candidates": self.n_candidates, "truncated": self.truncated}


def _below_bar_params(circuit: str) -> list:
    return [circuit, *_FALLBACK_TIERS, REVIEW_MIN_CONFIDENCE]


def load_candidates(conn: sqlite3.Connection, circuit: str,
                    since: Optional[str] = None,
                    now: Optional[datetime] = None) -> List[dict]:
    """Mature, below-the-bar events a person could still answer.

    ``since`` defaults to ``WINDOW_DAYS`` before ``now``: recent enough that the
    operator can remember what happened.
    """
    now = now or datetime.now(timezone.utc)
    since = since or (now - timedelta(days=WINDOW_DAYS)).isoformat()
    cutoff = (now - timedelta(seconds=MATURITY_WINDOW_S)).isoformat()
    return [dict(r) for r in conn.execute(
        f"SELECT {_COLS} FROM events WHERE {_BELOW_BAR_WHERE} "
        "AND start_ts >= ? AND start_ts <= ? ORDER BY start_ts DESC",
        [*_below_bar_params(circuit), since, cutoff])]


def _item(c: dict) -> CardItem:
    conf = c.get("match_confidence")
    via = c.get("matched_via")
    if not c.get("matched_fixture_type"):
        why = "no tier could name it"
    elif via == "tinymodel":
        why = f"model {conf or 0:.0%} sure"
    else:
        why = "k-NN fallback after the model declined"
    return CardItem(event_id=str(c["id"]), start_ts=str(c["start_ts"]),
                    volume_litres=c.get("volume_litres"),
                    duration_seconds=c.get("duration_seconds"),
                    proposed=c.get("matched_fixture_type") or None,
                    confidence=conf, via=via, rationale=why)


def select_items(candidates: Sequence[dict],
                 limit: int = CARD_CAP) -> List[CardItem]:
    """Uncertainty-first, round-robined across proposed classes.

    Events no tier could name are their own bucket and go first, because "the
    add-on has no idea" is exactly the case where an answer teaches the most.
    """
    buckets: Dict[str, List[dict]] = {}
    for c in sorted(candidates,
                    key=lambda c: (c.get("match_confidence") is not None,
                                   c.get("match_confidence") or 0.0)):
        buckets.setdefault(c.get("matched_fixture_type") or "<abstained>",
                           []).append(c)
    order = sorted(buckets, key=lambda k: (k != "<abstained>", -len(buckets[k])))
    picked: List[CardItem] = []
    while len(picked) < limit and any(buckets[k] for k in order):
        for key in order:
            if len(picked) >= limit:
                break
            if buckets[key]:
                picked.append(_item(buckets[key].pop(0)))
    return picked


def build_card(conn: sqlite3.Connection, circuit: str,
               now: Optional[datetime] = None) -> ReviewCard:
    """Pick a fresh card from the last week. Read-only; ``refresh_card`` stores it."""
    now = now or datetime.now(timezone.utc)
    candidates = load_candidates(conn, circuit, now=now)
    items = select_items(candidates)
    return ReviewCard(circuit=circuit, generated_at=now.isoformat(), items=items,
                      n_candidates=len(candidates),
                      truncated=max(len(candidates) - len(items), 0))


def refresh_card(conn: sqlite3.Connection, circuit: str,
                 now: Optional[datetime] = None) -> Optional[ReviewCard]:
    """Store a new card when the last one is a week old (or there is none).

    Returns the new card, or None when the current one is not due yet. An empty
    card is stored too, so a quiet week stays quiet until the next one.
    Commits; the caller holds the write lock.
    """
    now = now or datetime.now(timezone.utc)
    row = conn.execute("SELECT generated_at FROM review_card WHERE circuit = ?",
                       (circuit,)).fetchone()
    if row is not None:
        try:
            if now - datetime.fromisoformat(row[0]) < _CARD_PERIOD:
                return None
        except (TypeError, ValueError):
            pass                                    # unreadable → re-pick
    card = build_card(conn, circuit, now=now)
    conn.execute(
        "INSERT OR REPLACE INTO review_card (circuit, generated_at, "
        "event_ids_json, pool_n) VALUES (?, ?, ?, ?)",
        (circuit, card.generated_at,
         json.dumps([i.event_id for i in card.items]), card.n_candidates))
    conn.commit()
    log.info("[%s] review card: %d picked from %d event(s) below %.0f%% "
             "confidence in the last %d days", circuit, len(card.items),
             card.n_candidates, REVIEW_MIN_CONFIDENCE * 100, WINDOW_DAYS)
    return card


def current_card(conn: sqlite3.Connection, circuit: str) -> ReviewCard:
    """This week's stored picks that still need an answer, in pick order.

    Never backfills: an item leaves when it is labelled, ignored, or named
    confidently by a newer model, and nothing takes its place.
    """
    row = conn.execute(
        "SELECT generated_at, event_ids_json, pool_n FROM review_card "
        "WHERE circuit = ?", (circuit,)).fetchone()
    if row is None:
        return ReviewCard(circuit=circuit, generated_at=None)
    try:
        ids = [str(i) for i in json.loads(row[1] or "[]")]
    except (TypeError, ValueError):
        ids = []
    items: List[CardItem] = []
    if ids:
        open_rows = {str(r["id"]): dict(r) for r in conn.execute(
            f"SELECT {_COLS} FROM events WHERE {_BELOW_BAR_WHERE} "
            f"AND id IN ({','.join('?' * len(ids))})",
            [*_below_bar_params(circuit), *ids])}
        items = [_item(open_rows[i]) for i in ids if i in open_rows]
    return ReviewCard(circuit=circuit, generated_at=row[0], items=items,
                      n_candidates=int(row[2] or 0))
