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
import hmac
import json
import logging
import math
import os
import secrets
import shutil
import stat
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
    # dev48 — the rate a draw runs at once running (migration 20260812).
    # Deliberately NOT in LOG_FEATURES, which is where its sibling flow rates
    # sit: the +2.3 point measurement was taken untransformed, and a boosted
    # TREE is invariant to any monotone transform of a single feature anyway,
    # so log-compressing it could only make the number harder to reproduce.
    "flow_plateau_lpm",
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

# dev48.0 adds flow_plateau_lpm. Bumping this is what makes every stored
# artifact unservable until it retrains — correct, because a model fitted
# without the column cannot be scored against rows that have it.
FEATURE_SET_VERSION = "dev48.0"

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

# dev51 (1.5) — which `fixture_label_source` values are MACHINE labels. 'anchor'
# was the designed value and is never actually written; 'cycle' is what
# propagate_cycle_label writes, and until dev51 it counted as human truth in
# both the training pool and the referee's holdout — the dev40 bad-machine-
# label lesson, one level up. 'training' (the wizard) is deliberate human
# ground truth and stays human. One predicate, used by eligibility, the pool
# partition and the holdout, so the three cannot disagree.
MACHINE_LABEL_SOURCES: tuple = ("anchor", "cycle")


def is_machine_label(row: dict) -> bool:
    return (row.get("fixture_label_source") or "direct") in MACHINE_LABEL_SOURCES

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

# ── artifact authenticity (2.16) ────────────────────────────────────────────
# The estimator is persisted as a pickle, and `pickle.loads` on the live event
# path is arbitrary code execution on whoever can write the artifact file. That
# is not theoretical here: a Home Assistant add-on backup captures /data
# verbatim, so a crafted backup carries a chosen artifact and it executes on the
# next classification — inside a container that holds SUPERVISOR_TOKEN and
# drives the main water valve, with no admin ever opening the UI.
#
# `skops.io` is upstream's recommended pickle replacement and would be the
# textbook fix. It is deliberately NOT used: this add-on's Dockerfile records
# three failed attempts at moving the scikit-learn / numpy / river dependency
# set, so adding another ML dependency to close this is disproportionate to the
# hole. An HMAC over the payload closes it with the standard library.
#
# The key follows the `csrf_server_secret` shape (database.py): 256 bits of
# hex, created once on first use, NEVER regenerated automatically — rotating it
# would invalidate every stored artifact and force an unnecessary retrain. It
# lives beside the artifacts in /data rather than in the DB because save/load
# take a data_dir, not a connection, and threading a connection through the
# serving path for this would be worse than the file.
SECRET_FILENAME = "tinymodel.secret"

# Legacy (pre-2.16) artifacts carry no signature. They are REFUSED, not
# accepted-once-and-re-signed. "Accept once" is precisely the attacker's
# happy path: the crafted backup plants an *unsigned* artifact, and nothing
# distinguishes it from a genuinely old one — re-signing would bless it with
# this install's key and make the compromise permanent. The cost of refusing
# is one retrain, which the learning loop performs on its own schedule, and
# the k-NN ladder is a complete classifier while that happens (staged
# bootstrap). A security control whose bypass is "have no signature" is not a
# control.
ALLOW_UNSIGNED_LEGACY_ARTIFACTS = False


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
    ``database._knn_transform``. Non-numeric and non-finite become NaN, which
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
    # dev51 — the local calendar days this artifact was fitted on. The referee
    # scores the recent leg only on days NEITHER model trained on; without this
    # the champion was scored on days it had memorised while the challenger was
    # scored clean, a one-directional bias that rejected every challenger.
    # Additive: artifacts written before dev51 load with an empty list and the
    # referee falls back to "days after trained_at". Deliberately NOT part of
    # model_hash — it describes the fit, it does not change it.
    train_days: List[str] = field(default_factory=list)
    model_blob: Optional[str] = None            # base64 joblib/pickle payload
    # HMAC-SHA256 of model_blob under this install's signing secret (2.16).
    # Stamped by ``save`` and checked by ``load``; an artifact that arrives
    # without one, or with one that does not verify, is never unpickled.
    model_signature: Optional[str] = None
    _estimator: object = field(default=None, repr=False, compare=False)
    # True only for an artifact this process fitted, or one whose signature
    # ``load`` verified. Gates the unpickle — so a code path that builds an
    # Artifact straight from untrusted JSON and calls ``estimator()`` still
    # cannot execute the payload.
    _trusted: bool = field(default=False, repr=False, compare=False)

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
            if not self._trusted:
                raise TinyModelUnavailable(
                    "refusing to deserialize an unverified tinymodel payload "
                    "— load() must verify its signature first")
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
    # Callers MUST have verified the signature first — see Artifact.estimator.
    return pickle.loads(base64.b64decode(blob))


# ── payload authenticity ────────────────────────────────────────────────────
def secret_path(data_dir: str) -> str:
    return os.path.join(data_dir, SECRET_FILENAME)


def get_or_create_signing_secret(data_dir: str) -> str:
    """The per-install artifact-signing key (see SECRET_FILENAME).

    Same contract as ``database.get_or_create_csrf_server_secret``: 64 hex
    characters, created on first use, never rotated automatically. Written
    temp-file-plus-rename so a concurrent reader never sees half a key, and
    chmod 0600 so the key is not readable by anything that merely gets a
    directory listing of /data.
    """
    path = secret_path(data_dir)
    try:
        with open(path, encoding="ascii") as fh:
            existing = fh.read().strip()
        if existing:
            return existing
    except OSError:
        pass
    os.makedirs(data_dir, exist_ok=True)
    fresh = secrets.token_hex(32)
    fd, tmp = tempfile.mkstemp(dir=data_dir, suffix=".tmp")
    try:
        os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(fd, "w", encoding="ascii") as fh:
            fh.write(fresh)
            fh.flush()
            os.fsync(fh.fileno())
        # os.replace is atomic, but two processes racing here would each keep
        # their own key. Re-read after the rename and defer to whoever landed
        # first, so the losing writer signs with the key that will be loaded.
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    with open(path, encoding="ascii") as fh:
        return fh.read().strip()


def _blob_signature(secret: str, blob: str) -> str:
    return hmac.new(secret.encode("ascii"), blob.encode("ascii"),
                    hashlib.sha256).hexdigest()


def verify_signature(art: "Artifact", data_dir: str) -> bool:
    """Constant-time check that this install signed exactly these bytes."""
    if not art.model_blob:
        return False
    if not art.model_signature:
        return bool(ALLOW_UNSIGNED_LEGACY_ARTIFACTS)
    try:
        secret = get_or_create_signing_secret(data_dir)
    except OSError as exc:                  # unwritable /data — fail closed
        log.error("cannot read the tinymodel signing secret (%s); refusing "
                  "to load a model payload", exc)
        return False
    return hmac.compare_digest(_blob_signature(secret, art.model_blob),
                               art.model_signature)


def _blob_digest(blob: Optional[str]) -> str:
    """Plain SHA-256 over the served bytes, folded into ``model_hash``."""
    return hashlib.sha256((blob or "").encode("ascii")).hexdigest()[:16]


def label_pool_hash(rows: Sequence[dict]) -> str:
    """Identity of the training pool: which events, with which labels.

    Ids alone are not enough — relabelling one event changes what the model
    should learn without changing the id set.
    """
    h = hashlib.sha256()
    for r in sorted(rows, key=lambda r: str(r.get("id"))):
        h.update(f"{r.get('id')}={r.get('_y')};".encode())
    return h.hexdigest()[:16]


def _model_hash(pool_hash: str, classes: Sequence[str], threshold: float,
                blob_digest: str = "") -> str:
    """Identity of the served artifact.

    2.16 — ``blob_digest`` is new and load-bearing: before it, the hash
    described the RECIPE (pool, params, classes, threshold) and not the bytes,
    so swapping ``model_blob`` for a different payload left the hash — and
    therefore every verdict keyed to it — completely unchanged. Covering the
    payload makes the hash an identity of what is actually served.

    A plain digest rather than the HMAC signature, deliberately: two identical
    fits of the same pool must still produce the same hash, or every retrain
    would invalidate its own predecessor's verdicts for no reason, and the
    referee could not compare a champion against a re-fit challenger.
    """
    h = hashlib.sha256()
    h.update(f"fs={FEATURE_SET_VERSION};pool={pool_hash};".encode())
    h.update(f"params={sorted(MODEL_PARAMS.items())};".encode())
    # str() every class: sklearn hands back numpy.str_, whose repr is
    # ``np.str_('shower')`` under numpy 2, while the same list read back out of
    # the artifact JSON is plain ``'shower'``. Before 2.16 nothing recomputed
    # the hash, so the two never met and the discrepancy was invisible; the
    # self-check below meets it on every load.
    h.update(f"classes={sorted(str(c) for c in classes)};"
             f"thr={threshold:.4f}".encode())
    h.update(f";blob={blob_digest}".encode())
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
    user_rows = [r for r in rows if not is_machine_label(r)]
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
    # Serialize BEFORE hashing — model_hash now covers the payload (2.16).
    blob = _serialize(clf)
    art = Artifact(
        model_hash=_model_hash(pool_hash, list(clf.classes_), threshold,
                               _blob_digest(blob)),
        trained_at=datetime.now(timezone.utc).isoformat(),
        feature_set_version=FEATURE_SET_VERSION,
        features=list(FEATURES),
        classes=[str(c) for c in clf.classes_],
        label_pool_hash=pool_hash,
        class_counts=counts,
        # (classes is stored below as plain str for the same round-trip
        # reason — see _model_hash.)
        threshold=threshold,
        achieved_precision=precision,
        coverage_at_threshold=coverage,
        n_train=len(rows),
        circuit=circuit,
        notes=notes,
        zero_filled=list(empty),
        train_days=sorted({str(r.get("start_ts"))[:10] for r in rows
                           if r.get("start_ts")}),
        model_blob=blob,
    )
    art._estimator = clf
    art._trusted = True                 # we fitted it; nothing to verify
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


def _predict_batch(art: Artifact, rows: Sequence[dict],
                   threshold: Optional[float] = None
                   ) -> List[Tuple[Optional[str], float]]:
    """``threshold`` overrides the artifact's serving threshold for this call
    only; ``0.0`` means argmax (never abstain). Default — no override — is the
    serving contract and is what every classification path uses. The override
    exists for the referee (dev51): two artifacts that chose different serving
    thresholds cannot be compared on an abstention-punishing metric, because a
    coverage difference reads as a quality difference."""
    if not rows:
        return []
    import numpy as np
    clf = art.estimator()
    proba = clf.predict_proba(
        design_matrix(rows, art.features, zero_filled=art.zero_filled))
    classes = list(clf.classes_)
    thr = art.threshold if threshold is None else float(threshold)
    out: List[Tuple[Optional[str], float]] = []
    for p in proba:
        j = int(np.argmax(p))
        conf = float(p[j])
        out.append((classes[j] if conf >= thr else None, conf))
    return out


def predict_many(art: Artifact, rows: Sequence[dict],
                 threshold: Optional[float] = None
                 ) -> List[Tuple[Optional[str], float]]:
    return _predict_batch(art, rows, threshold=threshold)


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

    2.16 — signing happens HERE rather than in ``train`` because this is the
    first point that knows which /data the artifact belongs to, and a key is
    per-install. An artifact that is never saved is never unpickled, so it
    never needs a signature.
    """
    os.makedirs(data_dir, exist_ok=True)
    if art.model_blob:
        art.model_signature = _blob_signature(
            get_or_create_signing_secret(data_dir), art.model_blob)
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
    # A retrain supersedes any earlier refusal. The (mtime, size) key would
    # normally notice on its own, but not if a coarse-resolution filesystem
    # reports the same mtime AND the new artifact happens to be the same size
    # — and the cost of that miss is the tier staying dark until a restart.
    forget_refusals()
    return path



# Artifacts we have already judged and refused, keyed by path -> (mtime_ns,
# size). load() is called PER EVENT (database.py, feature_extractor.py), so
# without this a refused artifact is re-opened, re-parsed and re-verified for
# every classification — and, as first deployed, re-logged at ERROR each time:
# hundreds of identical lines a minute during a reclassify, drowning the log
# the operator needs to see the actual refusal in. The stat key means a
# retrain (which writes a NEW file) is picked up immediately, so this caches
# the VERDICT, never the file.
_REFUSED: Dict[str, Tuple[int, int]] = {}


def _stat_key(path: str) -> Tuple[int, int]:
    st = os.stat(path)
    return (st.st_mtime_ns, st.st_size)


def forget_refusals() -> None:
    """Drop the refusal cache (called after a successful train/save)."""
    _REFUSED.clear()


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
            stat_key = _stat_key(path)
        except OSError:
            continue
        if _REFUSED.get(path) == stat_key:
            # Already judged, and the file has not changed since. Silent by
            # design: the reason was logged once when the verdict was reached.
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                art = Artifact.from_json(json.load(fh))
            if art.feature_set_version != FEATURE_SET_VERSION:
                log.warning("artifact %s was built for feature set %s, this "
                            "build serves %s — ignoring it", path,
                            art.feature_set_version, FEATURE_SET_VERSION)
                continue
            # 2.16 — authenticity, checked BEFORE anything can unpickle the
            # payload. A failure here is a tamper or a foreign artifact, not a
            # corruption: say so plainly, because the operator's next step
            # (retrain) differs from "the file is truncated".
            if not verify_signature(art, data_dir):
                _REFUSED[path] = stat_key
                if not art.model_signature:
                    # An artifact written before signing existed. This is an
                    # EXPECTED upgrade state, not a fault: it self-heals on the
                    # next retrain, and the k-NN/fingerprint rungs keep serving
                    # meanwhile (staged bootstrap). WARNING, not ERROR — an
                    # ERROR here trains the operator to ignore the level that a
                    # real tamper below needs.
                    log.warning(
                        "tinymodel artifact %s predates artifact signing — the "
                        "TinyModel tier is skipped for this circuit until the "
                        "next retrain writes a signed one. The other tiers are "
                        "unaffected. Logged once per artifact.", path)
                else:
                    # A signature that is present and WRONG is a different
                    # claim: a foreign artifact or a modified one.
                    log.error(
                        "tinymodel artifact %s is signed by a DIFFERENT key or "
                        "has been modified since it was written — refusing to "
                        "load its model payload.", path)
                continue
            # The hash must also still describe the bytes: a payload swap that
            # left model_hash alone would otherwise keep every verdict keyed to
            # the old, honest model.
            expected = _model_hash(art.label_pool_hash, art.classes,
                                   art.threshold, _blob_digest(art.model_blob))
            if art.model_hash != expected:
                _REFUSED[path] = stat_key
                log.error("tinymodel artifact %s does not match its own "
                          "model_hash (%s != %s) — refusing it", path,
                          art.model_hash, expected)
                continue
            art._trusted = True
            _REFUSED.pop(path, None)
            if is_prev:
                log.warning("serving the PREVIOUS tinymodel artifact for %s "
                            "(current one failed to load)", circuit)
            return art
        except Exception as exc:
            # Cached for the same reason as the refusals above: this branch is
            # also on the per-event path, so a corrupt or shape-changed
            # artifact would otherwise be re-parsed and re-logged for every
            # classification.
            try:
                _REFUSED[path] = _stat_key(path)
            except OSError:
                pass
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
