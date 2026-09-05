"""dev53 — the referee benchmark: ONE selection rule, ONE hash, app-side.

The referee's benchmark leg scores every challenger against a frozen set of
this home's own labelled events. Until dev53 that set was pinned by hand on a
developer machine (``tools/eval_tinymodel.py --pin-benchmark``) and pasted into
a Dev Tools card, which meant a second home never got one at all: its
benchmark leg abstained forever and the referee ran single-legged. This module
lets the add-on pin — and, deliberately, re-pin — its own.

It is deliberately pure: stdlib plus ``tinymodel.is_machine_label``. Every
selection path (the weekly auto-pin, the Dev Tools button, the Water Use
prompt, AND the dev-box tool) must call ``select_benchmark`` so that identical
rows yield an identical id set and hash. Two things used to differ between the
tool and the app and would have silently produced different benchmarks from
the same data — the tool grouped days by Denver-local date while every
app-side day grouping uses the UTC ``start_ts[:10]`` slice, and the tool's row
loader skipped the quarantine/exclusion filters the training pool applies.
Both are settled here: UTC days, human-source pool-eligible rows only.

SIZING IS DERIVED, NEVER FIXED. Pinning removes rows from training, and
eligibility is enforced twice on the way to a model: ``tm.eligible(pool)`` on
the full pool, and again inside ``tm.train`` on the rows that survive the
reservation AND the every-4th-day holdout split. A pin that leaves fewer than
``MIN_USER_LABELS`` human rows in that final set does not degrade quietly — it
makes every retrain return ``ineligible`` until more labels arrive, i.e. it
stalls the loop it exists to keep turning. ``plan_size`` encodes that:

    0.75 × (H − B_reserved) ≥ 100     →     H ≥ B_reserved + 134

plus headroom for the ordinary churn that removes rows from the pool after the
pin (30 left this home's pool in two weeks), plus a floor for the recent leg's
holdout so the benchmark can never starve the other leg into abstention.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from . import tinymodel as tm

# ── policy constants (operator-confirmed 2026-09-05; see the dev53 plan) ────
PIN_DAY_FRACTION: float = 0.25     # aim for ~25 % of the human pool, whole days
PIN_CAP: int = 150                 # never reserve more than this many events
PIN_FLOOR: int = 60                # below this the leg cannot veto anything real
PIN_HEADROOM: int = 15             # human labels of slack above the floor at pin time
# 100 human rows must survive the ~25 % holdout split: ceil(100 / 0.75) = 134
MIN_TRAINABLE_HUMAN: int = 134
# the recent leg abstains under 12 rows; keep a margin so a pin never starves it
HOLDOUT_FLOOR: int = 20
HOLDOUT_FRACTION: float = 0.25     # split_holdout takes user_days[::4]
# ⇒ the first pin is only allowed with this many human pool-eligible labels
PIN_MIN_HUMAN_LABELS: int = PIN_FLOOR + MIN_TRAINABLE_HUMAN + PIN_HEADROOM   # 209

REPIN_DECAY_RATIO: float = 0.70    # matched/requested below this → re-pin prompt
DECAY_REPORT_RATIO: float = 0.85   # below this → informational line only
REPIN_GROWTH_FACTOR: int = 2       # human labels ≥ this × pinned_from_n → growth prompt


# ── the canonical basis ─────────────────────────────────────────────────────
def day_of(row: dict) -> str:
    """UTC calendar day of an event — the SAME slice ``split_holdout``,
    ``clean_recent_holdout`` and ``tm.train``'s ``train_days`` use."""
    return str(row.get("start_ts") or "")[:10]


def human_rows(rows: Iterable[dict]) -> List[dict]:
    """Human-source rows only. Machine labels ('cycle', 'anchor') are never
    pinned: a benchmark of the cycle detectors' own output would measure the
    model against its teacher, not against truth."""
    return [r for r in rows if not tm.is_machine_label(r)]


def benchmark_hash(ids: Iterable[str]) -> str:
    """sha256 of the newline-joined, lexicographically sorted ids, first 16 hex
    chars — byte-identical to what ``eval_tinymodel.pin_benchmark`` has always
    written, so a hash quoted in release notes stays comparable."""
    return hashlib.sha256("\n".join(sorted(str(i) for i in ids)).encode()).hexdigest()[:16]


def benchmark_resolution(n: int, rate: float = 0.72,
                         margin: float = 0.02, z: float = 1.6448536269514722) -> float:
    """Smallest regression (accuracy points) the referee can veto at size n."""
    if n <= 0:
        return 1.0
    se = math.sqrt(max(rate * (1 - rate), 1e-12) / n)
    return round(margin + z * math.sqrt(2) * se, 3)


# ── sizing ──────────────────────────────────────────────────────────────────
@dataclass
class SizePlan:
    human_n: int
    b_other: int                    # rows that stay reserved alongside this pin
    ceiling: int
    target: int
    ok: bool
    reason: str = ""

    def as_dict(self) -> dict:
        return {"human_n": self.human_n, "b_other": self.b_other,
                "ceiling": self.ceiling, "target": self.target,
                "ok": self.ok, "reason": self.reason}


def plan_size(human_n: int, b_other: int = 0, *, cap: int = PIN_CAP,
              floor: int = PIN_FLOOR, fraction: float = PIN_DAY_FRACTION,
              headroom: int = PIN_HEADROOM) -> SizePlan:
    """How large a pin may be, or why none is allowed.

    ``b_other`` is the size of a set that will stay reserved next to the new
    one — the ACTIVE set during a handover (a re-pin is written as pending and
    both are held out of training until the first promotion). Two constraints:
    the training rows that survive the holdout split must stay eligible, and
    the holdout itself must clear the recent leg's floor.
    """
    ceiling = min(cap, human_n - MIN_TRAINABLE_HUMAN - b_other)
    if ceiling < floor + headroom:
        need = floor + headroom + MIN_TRAINABLE_HUMAN + b_other
        return SizePlan(human_n, b_other, max(ceiling, 0), 0, False,
                        f"{human_n} human labels < {need} needed to reserve a "
                        f"{floor}-event benchmark with {headroom} of headroom"
                        + (f" beside the {b_other} already reserved" if b_other else "")
                        + " and keep the training pool eligible")
    target = max(floor, min(ceiling, int(round(fraction * human_n))))
    expected_holdout = HOLDOUT_FRACTION * (human_n - b_other - target)
    if expected_holdout < HOLDOUT_FLOOR:
        return SizePlan(human_n, b_other, ceiling, target, False,
                        f"a {target}-event benchmark would leave ~{expected_holdout:.0f} "
                        f"holdout rows for the recent leg (< {HOLDOUT_FLOOR})")
    return SizePlan(human_n, b_other, ceiling, target, True, "ok")


# ── selection ───────────────────────────────────────────────────────────────
@dataclass
class Selection:
    ids: List[str]                  # sorted
    days: List[str]                 # sorted UTC days
    hash: str
    human_n: int
    target: int
    ceiling: int
    class_counts: Dict[str, int] = field(default_factory=dict)

    @property
    def n(self) -> int:
        return len(self.ids)


def select_benchmark(rows: Sequence[dict], *, b_other: int = 0,
                     cap: int = PIN_CAP, floor: int = PIN_FLOOR,
                     fraction: float = PIN_DAY_FRACTION,
                     headroom: int = PIN_HEADROOM,
                     exclude_days: Optional[Iterable[str]] = None) -> Optional[Selection]:
    """Pick the benchmark from ``rows`` (already the human pool), or None.

    Whole UTC days are taken round-robin across the calendar
    (``days[::2] + days[1::2]``, the order the tool has always used) so no
    appliance cycle is split between the benchmark and the training pool. A
    day is added only if it FITS under the ceiling — the cap is a cap (the
    old tool tested the cap before adding a day, so "150" produced ~165) — and
    the loop stops once ``target`` is met. ``exclude_days`` lets an activation
    re-selection avoid the days the freshly promoted champion trained on, so
    the new set is clean for the model it will judge.
    """
    humans = human_rows(rows)
    plan = plan_size(len(humans), b_other, cap=cap, floor=floor,
                     fraction=fraction, headroom=headroom)
    if not plan.ok:
        return None
    skip = set(exclude_days or ())
    by_day: Dict[str, List[dict]] = {}
    for r in humans:
        d = day_of(r)
        if d and d not in skip:
            by_day.setdefault(d, []).append(r)
    days = sorted(by_day)
    chosen: List[str] = []
    picked: List[dict] = []
    for d in days[::2] + days[1::2]:
        if len(picked) >= plan.target:
            break
        if len(picked) + len(by_day[d]) > plan.ceiling:
            continue
        chosen.append(d)
        picked.extend(by_day[d])
    if len(picked) < floor:
        return None
    ids = sorted(str(r["id"]) for r in picked)
    counts: Dict[str, int] = {}
    for r in picked:
        y = str(r.get("_y") or r.get("user_fixture_type") or "").strip().lower()
        counts[y] = counts.get(y, 0) + 1
    return Selection(ids=ids, days=sorted(chosen), hash=benchmark_hash(ids),
                     human_n=len(humans), target=plan.target,
                     ceiling=plan.ceiling, class_counts=counts)


def build_payload(sel: Selection, note: str = "",
                  pinned_at: Optional[str] = None) -> dict:
    """The document shape the tool writes and ``parse_benchmark_payload``
    reads, so an app-built benchmark round-trips through the import path."""
    return {
        "benchmark_hash": sel.hash,
        "pinned_at": pinned_at or datetime.now(timezone.utc).isoformat(),
        "note": note,
        "n": sel.n,
        "n_days": len(sel.days),
        "resolution_pts": benchmark_resolution(sel.n),
        "class_counts": dict(sel.class_counts),
        "event_ids": list(sel.ids),
        "days": list(sel.days),
    }


# ── timestamps ──────────────────────────────────────────────────────────────
def ts_utc(value) -> Optional[datetime]:
    """Normalise a stored timestamp for ORDERING, or None if it cannot be read.

    Accepts a datetime, or a string with ``T`` OR a space between date and
    time (the codebase's known ``'T' > ' '`` string-comparison trap), a ``Z``
    suffix, and naive values (treated as UTC). Callers treat None as "cannot
    order" and FAIL CLOSED — abstain the leg, do not activate — because the
    failure mode of guessing is the one dev51's gate exists to prevent.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        if len(s) > 10 and s[10] == " ":
            s = s[:10] + "T" + s[11:]
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
