"""dev47 (47b) — the per-home TinyModel classification tier.

WHY A MODEL AT ALL, WHEN THERE IS ALREADY A LADDER
--------------------------------------------------
The k-NN ladder's feature scales and thresholds were fitted by leave-one-out
sweeps on THIS home's archive. The 2026-08-22 variant-house study measured what
that costs elsewhere: on four synthetic homes whose fixtures are shaped
differently, a per-home-trained gradient booster beat the ladder by 13-18
points, and the ladder gained nothing even on the EASIER homes. The constants
are not wrong; they are simply this house's, and there is no constant that is
every house's. A model refitted per home has no such constants to go stale.

WHAT THIS TIER IS NOT
---------------------
It is not the failure detector. A degrading fixture stays in its own class and
the model will happily learn its new shape — that is desirable, because fixture
health is measured DOWNSTREAM of attribution against a frozen baseline (47i),
where absorbed events remain visible. Nothing here should ever be made "smart"
about drift.

It is also not mandatory. The tier reports itself unavailable when scikit-learn
is absent or the home has too few labels, and the ladder serves as before. That
is the staged bootstrap, and it is why an import failure is a log line rather
than an outage.

THE ARTIFACT LIFECYCLE
----------------------
An artifact is written atomically (temp file + rename) and the previous one is
retained as last-known-good. Both halves matter: a half-written artifact whose
hash has already invalidated stored verdicts, with no loadable model to replace
them, is a livelock — the system would be unable to classify AND unable to fall
back. Retention also gives 47i a rollback target when a health alert opens.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from . import burst_features as bf

log = logging.getLogger(__name__)

# ── the feature set (must match what the harness measures) ──────────────────
BASE_FEATURES: tuple = (
    "volume_litres", "duration_seconds", "true_avg_flow_lpm", "avg_flow_lpm",
    "peak_flow_lpm", "steady_state_fraction", "flow_variability",
    "cycle_pulse_count", "flow_on_ratio", "flow_rise_rate_lpm_s",
    "flow_fall_rate_lpm_s", "opening_step_lpm", "time_to_90pct_flow_seconds",
    "pressure_delta_psi", "pre_event_pressure_psi", "hour_sin", "hour_cos",
    "is_weekend",
)
LOG_FEATURES: frozenset = frozenset({
    "volume_litres", "duration_seconds", "true_avg_flow_lpm", "avg_flow_lpm",
    "peak_flow_lpm", "time_to_90pct_flow_seconds",
})
# The supply-regime id conditions the model on the home's pressure era instead
# of forcing one geometry across a pump install (which is what silently killed
# cluster matching for twelve days in July).
REGIME_FEATURE = "supply_regime_id"
FEATURES: tuple = BASE_FEATURES + bf.FEATURE_NAMES + (REGIME_FEATURE,)

FEATURE_SET_VERSION = "dev47.1"

# Model hyper-parameters. These are the exact values every dev47 measurement
# used; changing one makes the stored benchmark numbers incomparable, so they
# are pinned here rather than exposed as settings.
MODEL_PARAMS: dict = {
    "max_iter": 150,
    "learning_rate": 0.1,
    "max_leaf_nodes": 15,
    "min_samples_leaf": 5,
    "l2_regularization": 1.0,
    "random_state": 0,
}

# Minimum USER labels before the tier is eligible. Anchor exemplars do not
# count (47c): they are distilled from the cycle detectors, so counting them
# would let a home "graduate" on its own teacher's output within a week.
MIN_USER_LABELS: int = 100
MIN_LABELS_PER_CLASS: int = 3

# Precision-first thresholding (F8). The operator contract is precision, so
# precision is what stays fixed and coverage floats. The bound is a lower
# confidence bound, not the point estimate: at n≈100-500 an uncorrected
# estimate picks a threshold that looks good on the sample and is not.
DEFAULT_TARGET_PRECISION: float = 0.85
THRESHOLD_GRID: tuple = (0.30, 0.40, 0.50, 0.55, 0.60, 0.65, 0.70,
                         0.75, 0.80, 0.85, 0.90)
FALLBACK_THRESHOLD: float = 0.60

ARTIFACT_FILENAME = "tinymodel.json"
PREVIOUS_FILENAME = "tinymodel.previous.json"


class TinyModelUnavailable(RuntimeError):
    """The tier cannot serve — not an error, a state (see staged bootstrap)."""


def sklearn_available() -> bool:
    try:
        import sklearn  # noqa: F401
        return True
    except Exception:                      # pragma: no cover - env dependent
        return False


# ── feature extraction ──────────────────────────────────────────────────────
def transform(feature: str, value) -> float:
    """log1p the right-skewed features, identity for the rest.

    Applied identically at fit and at predict — the same contract as
    ``database._sig_transform``. Non-numeric and non-finite become NaN, which
    the booster handles natively; they are NOT coerced to 0.0, because 0 is a
    meaningful value for most of these columns and a missing reading is not.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if not math.isfinite(v):
        return float("nan")
    if feature in LOG_FEATURES:
        return math.log1p(max(v, 0.0))
    return v


def row_vector(row: dict, features: Sequence[str] = FEATURES) -> List[float]:
    return [transform(f, row.get(f)) for f in features]


def design_matrix(rows: Sequence[dict], features: Sequence[str] = FEATURES,
                  zero_filled: Optional[Sequence[str]] = None):
    import numpy as np
    if not rows:
        return np.empty((0, len(features)))
    X = np.array([row_vector(r, features) for r in rows], dtype=float)
    for j, f in enumerate(features):
        if zero_filled and f in zero_filled:
            X[:, j] = np.nan_to_num(X[:, j], nan=0.0)
    return X


def absent_columns(rows: Sequence[dict],
                   features: Sequence[str] = FEATURES) -> List[str]:
    """Features with no value at all across the pool.

    Not the same as "missing on some rows" — the booster handles that natively.
    An entirely-absent column has no distribution to bin, and a home is
    perfectly entitled to have one (a sensor it lacks, a feature its firmware
    never reports).
    """
    import numpy as np
    if not rows:
        return []
    X = np.array([row_vector(r, features) for r in rows], dtype=float)
    return [f for j, f in enumerate(features) if bool(np.all(np.isnan(X[:, j])))]


# ── the artifact ────────────────────────────────────────────────────────────
@dataclass
class Artifact:
    """Everything needed to serve, plus everything needed to explain a swap."""

    model_hash: str
    trained_at: str
    feature_set_version: str
    features: List[str]
    classes: List[str]
    label_pool_hash: str
    class_counts: Dict[str, int]
    threshold: float
    achieved_precision: Optional[float]
    coverage_at_threshold: Optional[float]
    n_train: int
    circuit: str
    notes: str = ""
    # Feature columns that were entirely absent for this home at fit time and
    # were therefore zero-filled. Recorded so predict replays the SAME decision:
    # deciding it per batch would silently change the feature space between
    # train and serve. A booster cannot bin an all-NaN column, so without this
    # a home that never populates one feature cannot train a model at all.
    zero_filled: List[str] = field(default_factory=list)
    model_blob: Optional[str] = None            # base64 joblib/pickle payload
    _estimator: object = field(default=None, repr=False, compare=False)

    def to_json(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return d

    @classmethod
    def from_json(cls, d: dict) -> "Artifact":
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__
                 and not k.startswith("_")}
        return cls(**known)

    def estimator(self):
        if self._estimator is None:
            self._estimator = _deserialize(self.model_blob)
        return self._estimator


def _serialize(estimator) -> str:
    import base64
    import pickle
    return base64.b64encode(pickle.dumps(estimator)).decode("ascii")


def _deserialize(blob: Optional[str]):
    if not blob:
        raise TinyModelUnavailable("artifact carries no model payload")
    import base64
    import pickle
    return pickle.loads(base64.b64decode(blob))


def label_pool_hash(rows: Sequence[dict]) -> str:
    """Identity of the training pool: which events, with which labels.

    Ids alone are not enough — relabelling one event changes what the model
    should learn without changing the id set.
    """
    h = hashlib.sha256()
    for r in sorted(rows, key=lambda r: str(r.get("id"))):
        h.update(f"{r.get('id')}={r.get('_y')};".encode())
    return h.hexdigest()[:16]


def _model_hash(pool_hash: str, classes: Sequence[str], threshold: float) -> str:
    h = hashlib.sha256()
    h.update(f"fs={FEATURE_SET_VERSION};pool={pool_hash};".encode())
    h.update(f"params={sorted(MODEL_PARAMS.items())};".encode())
    h.update(f"classes={sorted(classes)};thr={threshold:.4f}".encode())
    return h.hexdigest()[:16]


# ── threshold selection ─────────────────────────────────────────────────────
def _wilson_lower(correct: int, total: int, z: float = 1.6448536269514722) -> float:
    if total <= 0:
        return 0.0
    p = correct / total
    d = 1.0 + z * z / total
    centre = p + z * z / (2 * total)
    half = z * math.sqrt(max(p * (1 - p) / total + z * z / (4 * total * total), 0.0))
    return max((centre - half) / d, 0.0)


def choose_threshold(scored: Sequence[Tuple[str, str, float]],
                     target_precision: float = DEFAULT_TARGET_PRECISION
                     ) -> Tuple[float, Optional[float], Optional[float]]:
    """Pick the LOWEST threshold whose held-out precision clears the target.

    ``scored`` is (truth, predicted, pmax) on data the model did not train on.
    Lowest-clearing rather than highest-precision: the operator contract is a
    precision floor, and past that point every extra point of precision is paid
    for in coverage — i.e. in events that become review-card questions instead
    of answers.

    Returns (threshold, achieved_precision, coverage). When nothing clears the
    target the most selective grid point is returned with its measured
    precision, so the caller can see it fell short rather than silently
    serving a model that cannot meet its contract.
    """
    if not scored:
        return FALLBACK_THRESHOLD, None, None
    best = None
    for thr in THRESHOLD_GRID:
        picked = [(t, p) for t, p, q in scored if q >= thr]
        if not picked:
            continue
        correct = sum(1 for t, p in picked if t == p)
        lower = _wilson_lower(correct, len(picked))
        stats = (thr, round(correct / len(picked), 4),
                 round(len(picked) / len(scored), 4))
        if lower >= target_precision:
            return stats
        best = stats
    return best if best else (FALLBACK_THRESHOLD, None, None)


# ── training ────────────────────────────────────────────────────────────────
def eligible(rows: Sequence[dict]) -> Tuple[bool, str]:
    """Is this home ready for the model tier? USER labels only (47c)."""
    user_rows = [r for r in rows
                 if (r.get("fixture_label_source") or "direct") != "anchor"]
    if len(user_rows) < MIN_USER_LABELS:
        return False, (f"{len(user_rows)} user labels < {MIN_USER_LABELS} "
                       "— kNN ladder still serves")
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["_y"]] = counts.get(r["_y"], 0) + 1
    usable = [c for c, n in counts.items() if n >= MIN_LABELS_PER_CLASS]
    if len(usable) < 2:
        return False, f"only {len(usable)} class(es) with >= {MIN_LABELS_PER_CLASS} labels"
    return True, f"{len(user_rows)} user labels across {len(usable)} classes"


def train(rows: Sequence[dict], circuit: str,
          holdout: Optional[Sequence[dict]] = None,
          target_precision: float = DEFAULT_TARGET_PRECISION,
          notes: str = "") -> Artifact:
    """Fit an artifact. ``rows`` need ``_y`` (the label) plus the feature keys.

    ``holdout`` is used ONLY to pick the operating threshold, and must be
    disjoint from ``rows``: a threshold chosen on training data is chosen on
    memorised answers and will not survive contact with new events.
    """
    if not sklearn_available():
        raise TinyModelUnavailable("scikit-learn is not installed in this image")
    ok, why = eligible(rows)
    if not ok:
        raise TinyModelUnavailable(why)
    if holdout:
        overlap = {str(r.get("id")) for r in rows} & {str(r.get("id")) for r in holdout}
        if overlap:
            raise ValueError(
                f"threshold holdout overlaps the training pool ({len(overlap)} "
                "shared id(s)); pick the threshold on data the model did not see")

    from sklearn.ensemble import HistGradientBoostingClassifier

    empty = absent_columns(rows)
    if empty:
        log.info("[%s] %d feature(s) absent for this home, zero-filled: %s",
                 circuit, len(empty), ", ".join(empty))
    X = design_matrix(rows, FEATURES, zero_filled=empty)
    y = [r["_y"] for r in rows]
    clf = HistGradientBoostingClassifier(**MODEL_PARAMS)
    clf.fit(X, y)

    scored: List[Tuple[str, str, float]] = []
    if holdout:
        import numpy as np
        proba = clf.predict_proba(
            design_matrix(holdout, FEATURES, zero_filled=empty))
        classes = list(clf.classes_)
        for r, p in zip(holdout, proba):
            j = int(np.argmax(p))
            scored.append((r["_y"], classes[j], float(p[j])))
    threshold, precision, coverage = choose_threshold(scored, target_precision)

    counts: Dict[str, int] = {}
    for label in y:
        counts[label] = counts.get(label, 0) + 1
    pool_hash = label_pool_hash(rows)
    art = Artifact(
        model_hash=_model_hash(pool_hash, list(clf.classes_), threshold),
        trained_at=datetime.now(timezone.utc).isoformat(),
        feature_set_version=FEATURE_SET_VERSION,
        features=list(FEATURES),
        classes=list(clf.classes_),
        label_pool_hash=pool_hash,
        class_counts=counts,
        threshold=threshold,
        achieved_precision=precision,
        coverage_at_threshold=coverage,
        n_train=len(rows),
        circuit=circuit,
        notes=notes,
        zero_filled=list(empty),
        model_blob=_serialize(clf),
    )
    art._estimator = clf
    log.info("[%s] tinymodel trained: %d events, %d classes, threshold %.2f "
             "(precision %s, coverage %s), hash %s", circuit, len(rows),
             len(counts), threshold, precision, coverage, art.model_hash)
    return art


# ── prediction ──────────────────────────────────────────────────────────────
def predict_one(art: Artifact, row: dict) -> Tuple[Optional[str], float]:
    """(label, confidence). ``None`` means abstain — the ladder continues.

    An abstention is a real answer here: it is what routes an event to the
    review card instead of guessing, and the threshold that produces it was
    chosen to hold a measured precision floor.
    """
    label, conf = _predict_batch(art, [row])[0]
    return label, conf


def _predict_batch(art: Artifact, rows: Sequence[dict]
                   ) -> List[Tuple[Optional[str], float]]:
    if not rows:
        return []
    import numpy as np
    clf = art.estimator()
    proba = clf.predict_proba(
        design_matrix(rows, art.features, zero_filled=art.zero_filled))
    classes = list(clf.classes_)
    out: List[Tuple[Optional[str], float]] = []
    for p in proba:
        j = int(np.argmax(p))
        conf = float(p[j])
        out.append((classes[j] if conf >= art.threshold else None, conf))
    return out


def predict_many(art: Artifact, rows: Sequence[dict]
                 ) -> List[Tuple[Optional[str], float]]:
    return _predict_batch(art, rows)


# ── persistence ─────────────────────────────────────────────────────────────
def artifact_path(data_dir: str, circuit: str) -> str:
    return os.path.join(data_dir, f"{circuit}.{ARTIFACT_FILENAME}")


def previous_path(data_dir: str, circuit: str) -> str:
    return os.path.join(data_dir, f"{circuit}.{PREVIOUS_FILENAME}")


def save(art: Artifact, data_dir: str) -> str:
    """Write atomically, retaining the outgoing artifact as last-known-good.

    Temp file + rename: a reader either sees the whole old artifact or the
    whole new one, never a truncated file. The retained copy is what makes a
    health-alert rollback (47i) and a load-failure fallback possible.
    """
    os.makedirs(data_dir, exist_ok=True)
    path = artifact_path(data_dir, art.circuit)
    if os.path.exists(path):
        try:
            shutil.copy2(path, previous_path(data_dir, art.circuit))
        except OSError as exc:              # retention is best-effort
            log.warning("could not retain previous artifact: %s", exc)
    fd, tmp = tempfile.mkstemp(dir=data_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(art.to_json(), fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def load(data_dir: str, circuit: str, allow_previous: bool = True
         ) -> Optional[Artifact]:
    """Load the serving artifact, falling back to last-known-good.

    A corrupt current artifact must not take the tier down while a perfectly
    good previous one sits beside it — that combination (unloadable model,
    already-invalidated verdicts) is the livelock the atomic write exists to
    prevent, and this is its second line of defence.
    """
    for path, is_prev in ((artifact_path(data_dir, circuit), False),
                          (previous_path(data_dir, circuit), True)):
        if is_prev and not allow_previous:
            continue
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                art = Artifact.from_json(json.load(fh))
            if art.feature_set_version != FEATURE_SET_VERSION:
                log.warning("artifact %s was built for feature set %s, this "
                            "build serves %s — ignoring it", path,
                            art.feature_set_version, FEATURE_SET_VERSION)
                continue
            if is_prev:
                log.warning("serving the PREVIOUS tinymodel artifact for %s "
                            "(current one failed to load)", circuit)
            return art
        except Exception as exc:
            log.warning("tinymodel artifact %s unreadable (%s)", path, exc)
    return None


def rollback(data_dir: str, circuit: str) -> Optional[Artifact]:
    """Promote the retained artifact back to serving (47i health rollback)."""
    prev = previous_path(data_dir, circuit)
    if not os.path.exists(prev):
        log.warning("no retained artifact to roll back to for %s", circuit)
        return None
    cur = artifact_path(data_dir, circuit)
    try:
        os.replace(prev, cur)
    except OSError as exc:
        log.error("tinymodel rollback failed for %s: %s", circuit, exc)
        return None
    art = load(data_dir, circuit, allow_previous=False)
    if art:
        log.warning("[%s] rolled back to tinymodel %s (trained %s)",
                    circuit, art.model_hash, art.trained_at)
    return art


# ── the serving tier ────────────────────────────────────────────────────────
# One entry point for both classification paths (the live pipeline and the
# batch reclassify), so they cannot drift apart. Everything about it is
# best-effort: an absent artifact, an absent scikit-learn, or a corrupt model
# all mean "this tier abstains", never "classification fails". The ladder below
# it is a complete classifier on its own — that is the staged bootstrap, and it
# is what lets this ship before the image question is settled.
_ARTIFACT_CACHE: Dict[str, tuple] = {}


def _artifact_for(data_dir: str, circuit: str) -> Optional[Artifact]:
    """Load with an mtime-keyed cache — the live path classifies every event,
    and re-reading a pickled model per event would be absurd."""
    path = artifact_path(data_dir, circuit)
    try:
        stamp = os.path.getmtime(path)
    except OSError:
        _ARTIFACT_CACHE.pop(circuit, None)
        return None
    cached = _ARTIFACT_CACHE.get(circuit)
    if cached and cached[0] == stamp:
        return cached[1]
    art = load(data_dir, circuit)
    if art is not None:
        _ARTIFACT_CACHE[circuit] = (stamp, art)
    return art


def invalidate_cache(circuit: Optional[str] = None) -> None:
    """Drop the cached artifact (call after a retrain swaps one in)."""
    if circuit is None:
        _ARTIFACT_CACHE.clear()
    else:
        _ARTIFACT_CACHE.pop(circuit, None)


def classify(conn, circuit: str, event_id: str, features: dict,
             data_dir: Optional[str] = None,
             burst_config: str = bf.CONFIG_IMMATURE
             ) -> Optional[Tuple[str, float]]:
    """(label, confidence) from the model tier, or None to abstain.

    ``burst_config`` is ``immature`` on the live path — a fill's siblings have
    not happened yet — and ``mature`` on the deferred re-classify, which is the
    whole reason that second pass exists.

    Never raises. A model tier that can break the event pipeline would be worse
    than no model tier at all.
    """
    try:
        if data_dir is None:
            from .config import DATA_DIR
            data_dir = str(DATA_DIR)
        art = _artifact_for(data_dir, circuit)
        if art is None:
            return None
        row = dict(features)
        row["id"] = event_id
        feats = bf.compute_for_events(conn, circuit, [event_id],
                                      config=burst_config)
        bf.attach([row], feats)
        if REGIME_FEATURE not in row:
            try:
                from .supply_regime import get_regimes, resolve_regime_for_ts
                row[REGIME_FEATURE] = resolve_regime_for_ts(
                    get_regimes(conn), row.get("start_ts")) or 0
            except Exception:
                row[REGIME_FEATURE] = 0
        label, conf = predict_one(art, row)
        if label is None:
            return None
        return label, conf
    except Exception as exc:                 # pragma: no cover - defensive
        log.warning("[%s] tinymodel tier failed (non-fatal): %s", circuit, exc)
        return None
