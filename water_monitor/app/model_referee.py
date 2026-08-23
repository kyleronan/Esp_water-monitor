"""dev47 (47c) — the model referee: one compare-against-frozen-reference rule.

Every unattended retrain produces a CHALLENGER. This module decides whether it
replaces the serving CHAMPION. The same mechanism also gates anchor-ingestion
admission and tier graduation, so all four decisions share one implementation
and one set of statistics.

WHY THE RULE LOOKS LIKE THIS (the V6d study, 2026-08-22)
--------------------------------------------------------
The obvious rule — "swap only if the challenger BEATS the champion on the
frozen benchmark AND on a recent labelled holdout" — was simulated and it
failed, badly and silently:

  * At realistic operator response rates (50%, 20% of carded events answered)
    it rejected EVERY swap. The model froze, the frozen model rejected more and
    more drifted events, those events fell off the health trend line, and the
    fixture-failure alarm NEVER FIRED in 21 simulated days. Detection died
    through the governance mechanism, not through the detector.

Both halves of that rule were wrong:

  1. **The frozen benchmark cannot certify an improvement.** It is pinned
     pre-drift, so by construction it contains none of the new information the
     challenger learned. Asking it "is the challenger better?" asks the wrong
     question of the wrong data — and at benchmark n≈58 the answer is ±2-4
     points of pure sampling noise, which a `>=` rule turns into a veto. The
     benchmark's real job is catching REGRESSIONS. So the test here is
     **non-inferiority**: swap unless the challenger is worse by more than the
     benchmark's own noise.

  2. **The "recent labelled holdout" was degenerate.** Newly labelled events
     are exactly the events the champion REJECTED (that is how they reached the
     review card), and the challenger TRAINED on them. Measured: champion 0.0,
     challenger 1.0, in every single comparison — a train-on-test result rigged
     for the challenger that never changed a decision. A recent-labelled leg is
     only meaningful on events held OUT of the challenger's pool, which is why
     :func:`decide` REFUSES a leaked holdout rather than scoring it.

Under the dev47 re-framing, fixture-failure detection lives DOWNSTREAM of
attribution (see 47i): the referee is deliberately a bystander for drift-class
failures, and a challenger that has absorbed drift while keeping events in
their own class is the DESIRED outcome. This module therefore guards model
quality only. It must never be made "smart" about drift — that is the health
monitor's job, against a frozen baseline it owns.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

log = logging.getLogger(__name__)

# z for a one-sided 95% bound. Small-n discipline (R4b): every comparison here
# is a proportion measured on tens of events, so decisions ride on interval
# arithmetic, never on point estimates.
Z_95 = 1.6448536269514722

# A challenger must be worse than this much BEYOND sampling noise before the
# benchmark vetoes it. Expressed in accuracy points; deliberately small — the
# noise term does the heavy lifting and this only stops a slow bleed of many
# tiny, individually-insignificant regressions.
DEFAULT_NONINFERIORITY_MARGIN = 0.02

# Below this many held-out events the recent-labelled leg is NO-CONTEST: it
# abstains and the benchmark decides alone (R4a). Scoring 1-3 events produces a
# number, not evidence.
DEFAULT_MIN_HOLDOUT = 12


@dataclass(frozen=True)
class Score:
    """A proportion measured on a finite sample."""

    correct: int
    total: int

    def __post_init__(self) -> None:
        if self.total < 0 or self.correct < 0 or self.correct > self.total:
            raise ValueError(f"invalid Score(correct={self.correct}, total={self.total})")

    @property
    def rate(self) -> float:
        return self.correct / self.total if self.total else 0.0

    @property
    def se(self) -> float:
        """Standard error of the proportion (0 for an empty sample)."""
        if self.total <= 0:
            return 0.0
        p = self.rate
        return math.sqrt(max(p * (1.0 - p), 1e-12) / self.total)

    def wilson_lower(self, z: float = Z_95) -> float:
        """Wilson score lower bound — well-behaved at p→0/1 and small n,
        unlike the normal approximation."""
        n = self.total
        if n <= 0:
            return 0.0
        p = self.rate
        d = 1.0 + z * z / n
        centre = p + z * z / (2 * n)
        half = z * math.sqrt(max(p * (1 - p) / n + z * z / (4 * n * n), 0.0))
        return max((centre - half) / d, 0.0)


@dataclass(frozen=True)
class RefereeConfig:
    noninferiority_margin: float = DEFAULT_NONINFERIORITY_MARGIN
    min_holdout: int = DEFAULT_MIN_HOLDOUT
    z: float = Z_95


@dataclass(frozen=True)
class LegVerdict:
    name: str
    outcome: str                      # "pass" | "veto" | "no_contest"
    champion: Optional[Score] = None
    challenger: Optional[Score] = None
    detail: str = ""

    @property
    def vetoed(self) -> bool:
        return self.outcome == "veto"


@dataclass(frozen=True)
class RefereeVerdict:
    swap: bool
    reason: str
    legs: tuple = field(default_factory=tuple)

    def describe(self) -> str:
        parts = [f"{leg.name}={leg.outcome}" for leg in self.legs]
        return f"{'SWAP' if self.swap else 'KEEP'} ({self.reason}; {', '.join(parts)})"


def _noninferior(champion: Score, challenger: Score, cfg: RefereeConfig,
                 name: str) -> LegVerdict:
    """Veto only when the challenger is worse beyond noise + margin.

    The drop is tested against the sampling noise of the DIFFERENCE of two
    proportions. Equality, improvement, and any regression inside the noise
    band all pass — that is the point (see module docstring, finding 1).
    """
    if challenger.total <= 0 or champion.total <= 0:
        return LegVerdict(name, "no_contest", champion, challenger,
                          "empty sample")
    drop = champion.rate - challenger.rate
    se_diff = math.sqrt(champion.se ** 2 + challenger.se ** 2)
    tolerance = cfg.noninferiority_margin + cfg.z * se_diff
    if drop > tolerance:
        return LegVerdict(
            name, "veto", champion, challenger,
            f"worse by {drop:.3f} > tolerance {tolerance:.3f} "
            f"(margin {cfg.noninferiority_margin:.3f} + {cfg.z:.2f}·se {se_diff:.3f})")
    return LegVerdict(
        name, "pass", champion, challenger,
        f"drop {drop:+.3f} within tolerance {tolerance:.3f}")


def decide(
    *,
    benchmark_champion: Score,
    benchmark_challenger: Score,
    recent_champion: Optional[Score] = None,
    recent_challenger: Optional[Score] = None,
    recent_holdout_ids: Optional[Iterable] = None,
    challenger_pool_ids: Optional[Iterable] = None,
    config: Optional[RefereeConfig] = None,
) -> RefereeVerdict:
    """Decide whether the challenger replaces the champion.

    Both legs are NON-INFERIORITY tests: the challenger ships unless it is
    measurably worse. The frozen benchmark is the primary guard (it is the only
    reference the challenger did not learn from); the recent-labelled leg is a
    secondary guard that abstains when its sample is too small.

    ``recent_holdout_ids`` / ``challenger_pool_ids`` are the anti-degeneracy
    check. If both are supplied and they intersect, the "holdout" contains
    events the challenger trained on and scoring it is meaningless — this
    raises rather than returning a verdict, because a leaked holdout is a
    programming error, not a runtime condition. V6d measured this exact leak
    producing champion 0.0 / challenger 1.0 in every comparison.
    """
    cfg = config or RefereeConfig()

    if recent_holdout_ids is not None and challenger_pool_ids is not None:
        leaked = set(recent_holdout_ids) & set(challenger_pool_ids)
        if leaked:
            raise ValueError(
                f"recent holdout leaks into the challenger's training pool "
                f"({len(leaked)} shared id(s), e.g. {sorted(map(str, leaked))[:3]}). "
                "Score the challenger on events held OUT of its pool, or pass "
                "no recent leg at all (NO-CONTEST).")

    legs = [_noninferior(benchmark_champion, benchmark_challenger, cfg, "benchmark")]

    if (recent_challenger is None or recent_champion is None
            or recent_challenger.total < cfg.min_holdout):
        n = 0 if recent_challenger is None else recent_challenger.total
        legs.append(LegVerdict("recent", "no_contest", recent_champion,
                               recent_challenger,
                               f"holdout n={n} < min {cfg.min_holdout}"))
    else:
        legs.append(_noninferior(recent_champion, recent_challenger, cfg, "recent"))

    vetoes = [leg for leg in legs if leg.vetoed]
    if vetoes:
        return RefereeVerdict(False, f"{vetoes[0].name}: {vetoes[0].detail}",
                              tuple(legs))
    return RefereeVerdict(True, "no leg vetoed", tuple(legs))


def score_predictions(rows: Sequence, predictions: Sequence,
                      truth_key=lambda r: r.get("user_fixture_type")) -> Score:
    """Convenience: count exact-match predictions over rows.

    ``predictions`` may contain ``None`` for abstentions; an abstention counts
    against the model, matching how the ladder is scored everywhere else.
    """
    if len(rows) != len(predictions):
        raise ValueError("rows and predictions differ in length")
    correct = sum(1 for r, p in zip(rows, predictions)
                  if p is not None and p == truth_key(r))
    return Score(correct, len(rows))
