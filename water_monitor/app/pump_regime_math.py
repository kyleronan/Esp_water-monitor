"""Pure math for pump-regime detection and recharge-cycle leak estimation.

Shared by the offline validation study (tools/validate_pump_regime.py) and,
once the study passes, the Phase 3 nightly worker — the study validates THIS
code, not a copy (edge-signature lesson: what ships must be what was studied).

All functions operate on a 1 Hz-resampled pressure (and optionally flow)
series for one analysis window. No I/O, no DB, no asyncio.

Constants below are PRIORS from the 2026-07 ESYBOX incident (53–65 PSI
sawtooth, ~222 s recharge period, sd 3.46 overnight vs 0.47 static); the
study tunes them and its tuned values are authoritative.

SCOPE (v1): detection recognizes the CYCLING signature only
(vfd_constant_pressure profile). The decay-ramp fit is study/classifier-only —
it must NOT feed a detection verdict (municipal diurnal drift can fit a slow
ramp; see the pump plan, round-3 #6). switch_tank homes (~1 cycle/day, period
1e4–1e5 s) are structurally invisible to in-window periodicity search — their
leak signature is the ramp, handled in the switch_tank follow-up.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import numpy as np

# ── Priors (study-tunable) ─────────────────────────────────────────────────────
PUMP_MIN_P2P_PSI: float = 3.0      # sawtooth band floor (incident ≈ 12 PSI)
# A sawtooth of band B has sd ≈ B/√12 ≈ 0.29·B, so the p2p floor of 3.0
# implies sd ≈ 0.87 — the plan's original 1.5 prior contradicted its own p2p
# prior and rejected a clean 4 PSI-band pump (study tuning, 2026-07-21).
# Static supply measured sd 0.47 stays well below 0.8.
PUMP_MIN_SD_PSI: float = 0.8
PUMP_PERIOD_MIN_S: float = 60.0    # disjoint from pulsing_supply's 1–6 s band
PUMP_PERIOD_MAX_S: float = 1800.0
AUTOCORR_MIN_PROMINENCE: float = 0.4
AUTOCORR_STRONG_PROMINENCE: float = 0.6   # allows the reduced 3-cycle criterion
PUMP_MIN_CYCLES: int = 5           # or >=3 with strong prominence (plan #12)
PUMP_MIN_CYCLES_STRONG: int = 3

# Recharge-rise segmentation: a cut-in→cut-out upswing is a fast, mostly
# monotonic rise. (The incident's recharges climb ~10 PSI in ~10–20 s.)
RISE_MIN_PSI: float = 1.5
RISE_MAX_SECONDS: float = 60.0
RISE_DIP_TOLERANCE_PSI: float = 0.3   # brief counter-dips inside a rise

# Flow-phase alignment: max mean flow tolerated in the run-up window before a
# rise for it to still count as a pump recharge (trailing ffill artifacts stay
# under this; a real draw's flow is well above it).
ALIGN_MAX_PRE_FLOW_LPM: float = 0.2

# Decay-ramp fit (study/classifier-only in v1 — never a detection verdict).
RAMP_MIN_R2: float = 0.9
RAMP_MIN_PSI_PER_HR: float = 0.5   # plan round-4 #7: tank C≈1.5–2 L/PSI at
                                   # 30 L/day decays only ~0.6–0.8 PSI/h

# Quiet-window requirement for regime analysis. STUDY FINDING (2026-07-21):
# strict zero-flow never happens on a pump+leak home — the recharge slugs
# themselves meter flow every cycle (~7% duty at the incident's 222 s period),
# so the first study run found ZERO quiet windows on the post-install night.
# "Quiet" therefore means: no SUSTAINED draw (no flow run longer than
# QUIET_MAX_DRAW_SECONDS) and low overall duty — brief recharge slugs are
# tolerated; real fixture use is not.
QUIET_WINDOW_MIN_SECONDS: float = 20 * 60.0
QUIET_MAX_DRAW_SECONDS: float = 60.0    # longest tolerated flow run
QUIET_MAX_DUTY: float = 0.20            # max fraction of seconds with flow


def sustained_flow_busy_mask(flow_1hz_lpm: Sequence[float],
                             max_draw_s: float = QUIET_MAX_DRAW_SECONDS,
                             ) -> np.ndarray:
    """Boolean mask marking seconds inside a SUSTAINED draw (a contiguous
    flow>0 run longer than ``max_draw_s``). Brief runs (recharge slugs,
    icemaker blips) are NOT busy — they belong in the quiet analysis."""
    f = np.asarray(flow_1hz_lpm, dtype=float) > 0.0
    busy = np.zeros(f.size, dtype=bool)
    i = 0
    while i < f.size:
        if not f[i]:
            i += 1
            continue
        j = i
        while j < f.size and f[j]:
            j += 1
        if (j - i) > max_draw_s:
            busy[i:j] = True
        i = j
    return busy


def quiet_windows(flow_1hz_lpm: Sequence[float],
                  min_len_s: float = QUIET_WINDOW_MIN_SECONDS,
                  max_draw_s: float = QUIET_MAX_DRAW_SECONDS,
                  max_duty: float = QUIET_MAX_DUTY,
                  ) -> List[Tuple[int, int]]:
    """[(start, end)] spans usable for regime analysis: no sustained draw
    inside, length >= min_len_s, and overall flow duty <= max_duty (a string
    of back-to-back sub-60 s draws is still usage, not quiet)."""
    f = np.asarray(flow_1hz_lpm, dtype=float)
    busy = sustained_flow_busy_mask(f, max_draw_s)
    spans: List[Tuple[int, int]] = []
    start: Optional[int] = None
    for i in range(busy.size + 1):
        b = busy[i] if i < busy.size else True
        if not b and start is None:
            start = i
        elif b and start is not None:
            if i - start >= min_len_s:
                seg = f[start:i]
                if float((seg > 0).mean()) <= max_duty:
                    spans.append((start, i))
            start = None
    return spans


@dataclass
class RechargeRise:
    """One detected cut-in→cut-out pressure upswing (indices into the 1 Hz
    window; psi values at the endpoints)."""
    start_idx: int
    end_idx: int
    start_psi: float
    end_psi: float

    @property
    def rise_psi(self) -> float:
        return self.end_psi - self.start_psi

    @property
    def duration_s(self) -> int:
        return self.end_idx - self.start_idx


@dataclass
class RegimeVerdict:
    detected: bool
    period_s: Optional[float]
    prominence: Optional[float]
    sd_psi: float
    p2p_psi: float
    cycles: int
    # Ramp diagnostics — NEVER part of `detected` (v1 scope).
    ramp_slope_psi_per_hr: float
    ramp_r2: float
    # Median spacing between consecutive (aligned) recharge rises. PREFER this
    # over period_s for reporting/trending: production night 2026-07-25 showed
    # the autocorrelation locking onto a strong 62 s sub-harmonic while the 18
    # actual rises were ~259 s apart — detection was unaffected (cycles +
    # prominence carried it) but the raw autocorr period would have corrupted
    # the banner copy and the Phase 5 period-shrink trend.
    cycle_spacing_s: Optional[float] = None

    @property
    def reported_period_s(self) -> Optional[float]:
        return self.cycle_spacing_s or self.period_s


def find_dominant_period(pressure_1hz: Sequence[float],
                         min_s: float = PUMP_PERIOD_MIN_S,
                         max_s: float = PUMP_PERIOD_MAX_S,
                         ) -> Tuple[Optional[float], float]:
    """Dominant sawtooth period via normalized autocorrelation.

    Returns (period_seconds, prominence) where prominence is the normalized
    autocorrelation value (0..1) at the best in-band local-maximum lag;
    (None, 0.0) when no in-band local max exists or the window is too short
    (need >= 2× min lag of data to see even one repetition).
    """
    x = np.asarray(pressure_1hz, dtype=float)
    n = x.size
    lo, hi = int(min_s), int(min(max_s, n // 2))
    if n < 2 * lo or hi <= lo:
        return None, 0.0
    x = x - x.mean()
    denom = float(np.dot(x, x))
    if denom <= 0.0:
        return None, 0.0
    # Autocorrelation for lags [lo, hi] (direct dot products — window sizes
    # here are a few thousand samples, this is fast enough and dependency-free).
    ac = np.array([float(np.dot(x[:-k], x[k:])) / denom for k in range(lo, hi)])
    if ac.size < 3:
        return None, 0.0
    # Local maxima strictly inside the band.
    peaks = np.flatnonzero((ac[1:-1] > ac[:-2]) & (ac[1:-1] >= ac[2:])) + 1
    if peaks.size == 0:
        return None, 0.0
    best = peaks[int(np.argmax(ac[peaks]))]
    return float(lo + best), float(ac[best])


def segment_recharge_rises(pressure_1hz: Sequence[float],
                           min_rise_psi: float = RISE_MIN_PSI,
                           max_seconds: float = RISE_MAX_SECONDS,
                           dip_tolerance_psi: float = RISE_DIP_TOLERANCE_PSI,
                           ) -> List[RechargeRise]:
    """Detect fast, mostly monotonic pressure upswings (pump recharges).

    Walks the series accumulating rising runs; a run tolerates brief
    counter-dips up to ``dip_tolerance_psi`` below its running max. A run
    qualifies when its net rise >= min_rise_psi within <= max_seconds.
    """
    x = np.asarray(pressure_1hz, dtype=float)
    rises: List[RechargeRise] = []
    i = 0
    n = x.size
    while i < n - 1:
        if x[i + 1] <= x[i]:
            i += 1
            continue
        start = i
        run_max = x[i]
        j = i + 1
        while j < n:
            if x[j] >= run_max:
                run_max = x[j]
                j += 1
            elif run_max - x[j] <= dip_tolerance_psi:
                j += 1          # small dip inside the rise — keep going
            else:
                break
        # Trim the run's end back to the last index that set run_max, so a
        # tolerated trailing dip isn't counted as part of the rise.
        end = start + int(np.argmax(x[start:j])) if j > start else start
        if (x[end] - x[start] >= min_rise_psi
                and (end - start) <= max_seconds and end > start):
            rises.append(RechargeRise(start, end, float(x[start]), float(x[end])))
        i = max(end, start + 1)
    return rises


def rise_is_flow_aligned(flow_1hz_lpm: Sequence[float],
                         rise: RechargeRise) -> bool:
    """True when metered flow coincides with the pressure RISE (pump pushing
    water in) rather than preceding it (a draw whose end lets pressure
    recover). STUDY FINDING (2026-07-21): without this phase test, post-draw
    recoveries on ordinary nights read as 'recharge cycles' (a leaky toilet
    fill valve produced a clean 62 s periodicity with prominence 0.98 on a
    static-supply night) and draw volumes inflate the slug math ~4×."""
    f = np.asarray(flow_1hz_lpm, dtype=float)
    dur = max(rise.duration_s, 5)
    pre_lo = max(rise.start_idx - dur, 0)
    seg = f[rise.start_idx:rise.end_idx + 1]
    if seg.size == 0:
        return False
    # The load-bearing half is the QUIET RUN-UP: between recharges the pump is
    # off and the sub-floor leak meters as zero, so a true recharge rise is
    # preceded by flow silence; a post-draw recovery is preceded by the draw
    # itself (softener regen pulse trains produced clean 62-79 s periodicity
    # with prominence 0.98 — study finding #3). A during-coverage test was
    # tried and REJECTED: the metered slug (1-10 s) is much shorter than the
    # pressure rise it causes (10-40 s), so real recharges failed it.
    before = float(f[pre_lo:rise.start_idx].mean()) \
        if rise.start_idx > pre_lo else 0.0
    has_flow_in_rise = bool((seg > 0.0).any())
    return has_flow_in_rise and before <= ALIGN_MAX_PRE_FLOW_LPM


def aligned_rises(pressure_1hz: Sequence[float],
                  flow_1hz_lpm: Sequence[float]) -> List[RechargeRise]:
    """Recharge rises that pass the flow-phase test."""
    return [r for r in segment_recharge_rises(pressure_1hz)
            if rise_is_flow_aligned(flow_1hz_lpm, r)]


def fit_decay_ramp(pressure_1hz: Sequence[float]) -> Tuple[float, float]:
    """Least-squares linear fit over the window.

    Returns (slope_psi_per_hr, r_squared). Slope is signed (a decaying
    window yields a negative slope; callers compare against
    -RAMP_MIN_PSI_PER_HR). R² of the linear model vs the mean model.
    """
    y = np.asarray(pressure_1hz, dtype=float)
    n = y.size
    if n < 3:
        return 0.0, 0.0
    t = np.arange(n, dtype=float)
    slope_per_s, intercept = np.polyfit(t, y, 1)
    pred = slope_per_s * t + intercept
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return float(slope_per_s * 3600.0), r2


def detect_pump_regime(pressure_1hz: Sequence[float],
                       flow_1hz_lpm: Optional[Sequence[float]] = None,
                       ) -> RegimeVerdict:
    """Full cycling-signature verdict for one quiet window.

    ``detected`` requires ALL of: p2p >= PUMP_MIN_P2P_PSI, sd >=
    PUMP_MIN_SD_PSI, an in-band dominant period with prominence >=
    AUTOCORR_MIN_PROMINENCE, and a period-scaled cycle count (>=5, or >=3
    with prominence >= 0.6 — plan finding #12).

    When ``flow_1hz_lpm`` is given (production path — quiet windows tolerate
    brief draws), cycles are the FLOW-PHASE-ALIGNED rises only and at least
    half of all rises must be aligned — the discriminator that separates pump
    recharges (flow DURING the rise) from post-draw recoveries (flow before
    it). Without flow (pressure-only callers/synthetics) raw rises are used.
    Ramp diagnostics ride along but never influence the verdict (round-3 #6).
    """
    x = np.asarray(pressure_1hz, dtype=float)
    sd = float(x.std()) if x.size else 0.0
    p2p = float(x.max() - x.min()) if x.size else 0.0
    period, prominence = find_dominant_period(x)
    all_rises = segment_recharge_rises(x)
    if flow_1hz_lpm is not None:
        rises = [r for r in all_rises
                 if rise_is_flow_aligned(flow_1hz_lpm, r)]
        aligned_ok = (len(rises) >= max(1, len(all_rises) // 2))
    else:
        rises = all_rises
        aligned_ok = True
    cycles = len(rises)
    spacings = [float(b.start_idx - a.start_idx)
                for a, b in zip(rises, rises[1:])]
    cycle_spacing = float(np.median(spacings)) if spacings else None
    slope, r2 = fit_decay_ramp(x)

    detected = (
        p2p >= PUMP_MIN_P2P_PSI
        and sd >= PUMP_MIN_SD_PSI
        and period is not None
        and prominence >= AUTOCORR_MIN_PROMINENCE
        and aligned_ok
        and (cycles >= PUMP_MIN_CYCLES
             or (cycles >= PUMP_MIN_CYCLES_STRONG
                 and prominence >= AUTOCORR_STRONG_PROMINENCE))
    )
    return RegimeVerdict(detected=detected, period_s=period,
                         prominence=prominence, sd_psi=sd, p2p_psi=p2p,
                         cycles=cycles, ramp_slope_psi_per_hr=slope,
                         ramp_r2=r2, cycle_spacing_s=cycle_spacing)


# ── Leak estimation (2b) ───────────────────────────────────────────────────────

def capacitance_samples(pressure_1hz: Sequence[float],
                        flow_1hz_lpm: Sequence[float],
                        rises: Optional[List[RechargeRise]] = None,
                        ) -> List[Tuple[float, float, float]]:
    """Per-recharge hydraulic capacitance samples.

    For each recharge rise, integrates metered flow over the rise (L) and
    divides by the pressure gain (PSI). Returns [(mid_psi, slug_litres,
    capacitance_l_per_psi), ...]. NOTE (plan round-3 #1): the slug volume is
    METERED — capacitance inherits any meter over-registration 1:1; absolute
    calibration must come from street-meter/utility ground truth.
    """
    p = np.asarray(pressure_1hz, dtype=float)
    f = np.asarray(flow_1hz_lpm, dtype=float)
    if rises is None:
        rises = segment_recharge_rises(p)
    out: List[Tuple[float, float, float]] = []
    for r in rises:
        if r.end_idx >= f.size or r.rise_psi <= 0:
            continue
        slug_l = float(f[r.start_idx:r.end_idx + 1].sum()) / 60.0  # L/min @1 Hz
        if slug_l <= 0:
            continue
        mid_psi = 0.5 * (r.start_psi + r.end_psi)
        out.append((mid_psi, slug_l, slug_l / r.rise_psi))
    return out


def estimate_leak_rate_lph(pressure_1hz: Sequence[float],
                           flow_1hz_lpm: Sequence[float],
                           ) -> dict:
    """Leak-rate estimates for one ZERO-USAGE window, by two rearrangements
    of the same metered quantity (they agree by construction — the study's
    independent check is external ground truth, never these two vs each
    other):

    - 'cycle_lph': ROBUST per-cycle math — median slug × 3600 / median period
      (study finding: a raw slug-volume total over the window is inflated by
      any tolerated real micro-draws; medians over flow-phase-ALIGNED rises
      resist that contamination — the naive total is reported as
      'gross_flow_lph' for diagnosis).
    - 'capacitance_lph': median inter-recharge decay slope × median
      capacitance (generalizes to single-ramp/tank homes).
    Plus diagnostics: cycles, median period, median slug L, median C.
    """
    p = np.asarray(pressure_1hz, dtype=float)
    f = np.asarray(flow_1hz_lpm, dtype=float)
    hours = p.size / 3600.0
    rises = [r for r in segment_recharge_rises(p)
             if rise_is_flow_aligned(f, r)]
    caps = capacitance_samples(p, f, rises)
    gross_flow_lph = float(f.sum()) / 60.0 / hours if hours > 0 else 0.0

    # Median decay slope between consecutive recharges (PSI/h, positive).
    decay_slopes: List[float] = []
    for a, b in zip(rises, rises[1:]):
        seg = p[a.end_idx:b.start_idx]
        if seg.size >= 30:
            s, r2 = fit_decay_ramp(seg)
            if s < 0 and r2 >= 0.5:
                decay_slopes.append(-s)
    med_decay = float(np.median(decay_slopes)) if decay_slopes else 0.0
    med_cap = float(np.median([c for _, _, c in caps])) if caps else 0.0
    cap_lph = med_decay * med_cap

    periods = [float(b.start_idx - a.start_idx)
               for a, b in zip(rises, rises[1:])]
    med_slug = float(np.median([s for _, s, _ in caps])) if caps else 0.0
    med_period = float(np.median(periods)) if periods else 0.0
    cycle_lph = (med_slug * 3600.0 / med_period) if med_period > 0 else 0.0
    return {
        "cycle_lph": cycle_lph,
        "capacitance_lph": cap_lph,
        "gross_flow_lph": gross_flow_lph,
        "cycles": len(rises),
        "median_period_s": float(np.median(periods)) if periods else None,
        "median_slug_l": float(np.median([s for _, s, _ in caps])) if caps else None,
        "median_capacitance_l_per_psi": med_cap or None,
        "median_decay_psi_per_hr": med_decay or None,
    }


def classify_cross_circuit(pressure_1hz: Sequence[float],
                           flow_1hz_lpm: Sequence[float],
                           ) -> Tuple[str, int, Optional[float]]:
    """Phase 5b — verdict for the UNTESTED circuit's window while its sibling
    ran a valve-closed leak test. Returns (verdict, cycles, period_s).

    Any registered flow on this circuit demotes to 'not_applicable' (an
    icemaker fill / softener pulse here produces cycling that says nothing
    about a leak — plan round-1 #3). With flow silent, recharge rises are
    counted WITHOUT the flow-phase alignment test: the leak's slugs meter
    through the OTHER (tested) circuit's meter or below this one's floor, so
    this circuit sees the pressure sawtooth with zero flow — pressure-only
    cycling here is exactly the signal. >=3 in-band-spaced rises =>
    'untested_side'; else 'quiet'.
    """
    f = np.asarray(flow_1hz_lpm, dtype=float)
    if f.size and bool((f > 0.0).any()):
        return "not_applicable", 0, None
    p = np.asarray(pressure_1hz, dtype=float)
    rises = segment_recharge_rises(p)
    spacings = [float(b.start_idx - a.start_idx)
                for a, b in zip(rises, rises[1:])]
    in_band = [s for s in spacings
               if PUMP_PERIOD_MIN_S <= s <= PUMP_PERIOD_MAX_S]
    if len(rises) >= 3 and len(in_band) >= 2:
        return ("untested_side", len(rises),
                float(np.median(in_band)))
    return "quiet", len(rises), None


# ── Synthetic traces (2a switch_tank sweep + tests) ────────────────────────────

def synth_vfd_sawtooth(hours: float, period_s: float, low_psi: float,
                       high_psi: float, noise_sd: float = 0.1,
                       recharge_s: float = 15.0, seed: int = 0) -> np.ndarray:
    """1 Hz VFD low-flow cycling: slow decay high→low then fast recharge."""
    rng = np.random.default_rng(seed)
    n = int(hours * 3600)
    out = np.empty(n)
    band = high_psi - low_psi
    decay_s = max(period_s - recharge_s, 1.0)
    t_in_cycle = 0.0
    level = high_psi
    for i in range(n):
        if t_in_cycle < decay_s:
            level = high_psi - band * (t_in_cycle / decay_s)
        else:
            level = low_psi + band * ((t_in_cycle - decay_s) / recharge_s)
        out[i] = level
        t_in_cycle += 1.0
        if t_in_cycle >= decay_s + recharge_s:
            t_in_cycle = 0.0
    return out + rng.normal(0.0, noise_sd, n)


def synth_switch_tank_ramp(hours: float, start_psi: float,
                           capacitance_l_per_psi: float, leak_lpd: float,
                           cut_in_psi: float, cut_out_psi: float,
                           noise_sd: float = 0.1, seed: int = 0) -> np.ndarray:
    """1 Hz switch+tank trace: slow decay at leak/C PSI/h, recharge step to
    cut_out whenever cut_in is crossed (typically 0–1 steps per night)."""
    rng = np.random.default_rng(seed)
    n = int(hours * 3600)
    slope_psi_per_s = (leak_lpd / 24.0 / 3600.0) / capacitance_l_per_psi
    out = np.empty(n)
    level = start_psi
    for i in range(n):
        level -= slope_psi_per_s
        if level <= cut_in_psi:
            level = cut_out_psi          # ~instant recharge step
        out[i] = level
    return out + rng.normal(0.0, noise_sd, n)
