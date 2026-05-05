"""
Fixture clustering engine — Phase 2.

Reads stored events for a circuit, runs DBSCAN on a normalised feature
matrix, writes cluster centroids to fixture_clusters, and back-populates
cluster_id on the event rows.

Designed to run synchronously (called via run_in_executor from async code)
so it does not hold the event loop during the DB-heavy bulk operations.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sklearn.cluster import DBSCAN

from .fixtures import suggest_fixture_type

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MIN_EVENTS = 150          # minimum events before clustering is attempted
DBSCAN_EPS = 0.8          # initial epsilon (in normalised feature space)
DBSCAN_EPS_FALLBACK = 1.2 # retry epsilon if 0 clusters produced
DBSCAN_MIN_SAMPLES = 5    # minimum cluster size

CLUSTER_FEATURES = [
    "avg_flow_lpm",
    "duration_seconds",
    "volume_litres",
    "pressure_delta_psi",
    "flow_variability",
]

_BATCH = 500  # rows per UPDATE batch


# ── Data loading ──────────────────────────────────────────────────────────────

def load_events_for_clustering(
    db: sqlite3.Connection,
    circuit: str,
) -> List[Dict[str, Any]]:
    cols = ", ".join(["id"] + CLUSTER_FEATURES)
    rows = db.execute(
        f"""
        SELECT {cols}
        FROM events
        WHERE circuit = ?
          AND excluded_from_training = 0
          AND avg_flow_lpm IS NOT NULL
          AND duration_seconds > 0
        """,
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Normalisation ─────────────────────────────────────────────────────────────

def _normalise(
    matrix: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Z-score normalise per column. Columns with std=0 are left as-is."""
    means = matrix.mean(axis=0)
    stds  = matrix.std(axis=0)
    stds_safe = np.where(stds == 0, 1.0, stds)
    return (matrix - means) / stds_safe, means, stds


# ── DBSCAN wrapper ────────────────────────────────────────────────────────────

def _run_dbscan(X: np.ndarray, eps: float) -> np.ndarray:
    return DBSCAN(
        eps=eps, min_samples=DBSCAN_MIN_SAMPLES, metric="euclidean", n_jobs=-1
    ).fit_predict(X)


def run_dbscan(X_scaled: np.ndarray) -> np.ndarray:
    """Run DBSCAN, retrying with a wider eps if 0 clusters are produced."""
    labels = _run_dbscan(X_scaled, DBSCAN_EPS)
    n_clusters = len(set(labels) - {-1})
    if n_clusters == 0:
        log.debug("DBSCAN eps=%.1f produced 0 clusters, retrying eps=%.1f",
                  DBSCAN_EPS, DBSCAN_EPS_FALLBACK)
        labels = _run_dbscan(X_scaled, DBSCAN_EPS_FALLBACK)
    return labels


# ── Cluster statistics ────────────────────────────────────────────────────────

def compute_cluster_stats(
    events: List[Dict[str, Any]],
    labels: np.ndarray,
    means: np.ndarray,
    stds: np.ndarray,
) -> List[Dict[str, Any]]:
    """
    Group events by cluster label and compute per-cluster statistics.
    Returns a list of cluster stat dicts (noise label -1 is skipped).
    Centroids are back-transformed to original (physical) units.
    """
    groups: Dict[int, List[int]] = {}
    for idx, lbl in enumerate(labels):
        if lbl == -1:
            continue
        groups.setdefault(int(lbl), []).append(idx)

    stats = []
    for lbl, indices in groups.items():
        member_events = [events[i] for i in indices]
        matrix = np.array(
            [[e[f] for f in CLUSTER_FEATURES] for e in member_events],
            dtype=float,
        )
        centroid_norm = matrix.mean(axis=0)
        centroid_raw  = centroid_norm  # already in original scale (we normalised a copy)

        # Back-transform: centroid_norm is mean of original-scale values,
        # so it IS already in physical units. (We normalise for clustering
        # distance, but compute stats directly from the raw matrix.)
        centroid_dict = {f: float(centroid_raw[i])
                         for i, f in enumerate(CLUSTER_FEATURES)}
        std_dict = {f: float(matrix[:, i].std())
                    for i, f in enumerate(CLUSTER_FEATURES)}

        stats.append({
            "label":        lbl,
            "member_count": len(indices),
            "event_ids":    [e["id"] for e in member_events],
            "centroid":     centroid_dict,
            "feature_std":  std_dict,
        })

    return stats


# ── Persist clusters ──────────────────────────────────────────────────────────

def save_clusters(
    db: sqlite3.Connection,
    circuit: str,
    cluster_stats: List[Dict[str, Any]],
    circuit_type: str = "main",
) -> List[Tuple[int, List[str]]]:
    """
    Write cluster stats to fixture_clusters.
    Deletes unconfirmed clusters (fixture_id IS NULL or fixture not user_locked)
    before inserting, so user-confirmed fixtures survive a re-run.
    Returns list of (cluster_id, event_ids) for event back-population.
    """
    # Remove old unconfirmed clusters for this circuit
    db.execute(
        """
        DELETE FROM fixture_clusters
        WHERE circuit = ?
          AND (
            fixture_id IS NULL
            OR fixture_id NOT IN (
                SELECT id FROM fixtures WHERE user_locked = 1
            )
          )
        """,
        (circuit,),
    )

    now = datetime.now(timezone.utc).isoformat()
    result: List[Tuple[int, List[str]]] = []

    for seq, cs in enumerate(cluster_stats, start=1):
        n = cs["member_count"]
        if n < 15:
            confidence_level = "preliminary"
        elif n < 40:
            confidence_level = "learning"
        else:
            confidence_level = "confirmed"

        suggested_type, suggested_confidence = suggest_fixture_type(
            cs["centroid"], circuit_type
        )

        db.execute(
            """
            INSERT INTO fixture_clusters (
                id, circuit, centroid, feature_std,
                member_count, suggested_type, suggested_confidence,
                confidence_level, publish_to_ha, created_at, last_match_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                seq,
                circuit,
                json.dumps(cs["centroid"]),
                json.dumps(cs["feature_std"]),
                n,
                suggested_type,
                suggested_confidence,
                confidence_level,
                now,
                now,
            ),
        )
        result.append((seq, cs["event_ids"]))

    return result


# ── Event back-population ─────────────────────────────────────────────────────

def write_event_cluster_ids(
    db: sqlite3.Connection,
    circuit: str,
    id_map: Dict[str, Optional[int]],
) -> None:
    """Write cluster_id back to events in batches of _BATCH rows."""
    items = list(id_map.items())
    for start in range(0, len(items), _BATCH):
        batch = items[start : start + _BATCH]
        db.executemany(
            "UPDATE events SET cluster_id = ? WHERE id = ? AND circuit = ?",
            [(cid, eid, circuit) for eid, cid in batch],
        )


# ── Co-occurrence ─────────────────────────────────────────────────────────────

def build_cooccurrence(
    db: sqlite3.Connection,
    circuit: str,
    window_seconds: float = 120,
) -> None:
    """
    Rebuild cluster_cooccurrence from event sequence for this circuit.
    Two consecutive events are "co-occurring" if the gap between them is
    <= window_seconds.  Clears existing rows for the circuit first.
    """
    rows = db.execute(
        """
        SELECT cluster_id, start_ts, end_ts
        FROM events
        WHERE circuit = ? AND cluster_id IS NOT NULL
        ORDER BY start_ts ASC
        """,
        (circuit,),
    ).fetchall()

    db.execute(
        "DELETE FROM cluster_cooccurrence WHERE circuit = ?", (circuit,)
    )

    if len(rows) < 2:
        return

    pairs: Dict[Tuple[int, int], List[float]] = {}
    for i in range(len(rows) - 1):
        curr = rows[i]
        nxt  = rows[i + 1]
        try:
            end_ts   = datetime.fromisoformat(curr["end_ts"] or curr["start_ts"])
            start_ts = datetime.fromisoformat(nxt["start_ts"])
            gap = (start_ts - end_ts).total_seconds()
        except (TypeError, ValueError):
            continue
        if 0 <= gap <= window_seconds:
            key = (int(curr["cluster_id"]), int(nxt["cluster_id"]))
            pairs.setdefault(key, []).append(gap)

    now = datetime.now(timezone.utc).isoformat()
    for (from_id, to_id), gaps in pairs.items():
        median_gap = float(np.median(gaps))
        db.execute(
            """
            INSERT INTO cluster_cooccurrence
                (circuit, from_cluster_id, to_cluster_id, count,
                 median_gap_seconds, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (circuit, from_id, to_id, len(gaps), median_gap, now),
        )


# ── Main entry point ──────────────────────────────────────────────────────────

def run_clustering(
    db: sqlite3.Connection,
    circuit: str,
    circuit_type: str = "main",
) -> Dict[str, Any]:
    """
    Full clustering pipeline for one circuit.

    1. Load events
    2. Normalise feature matrix
    3. Run DBSCAN
    4. Compute cluster stats
    5. Save clusters (preserving user_locked fixtures)
    6. Write cluster_id back to events
    7. Rebuild co-occurrence table

    Raises ValueError if fewer than MIN_EVENTS qualifying events exist.
    Returns a summary dict: {n_events, n_clusters, n_noise, cluster_ids}.
    """
    events = load_events_for_clustering(db, circuit)
    n_events = len(events)

    if n_events < MIN_EVENTS:
        raise ValueError(
            f"only {n_events} events (need {MIN_EVENTS}) — clustering skipped"
        )

    # Build feature matrix (rows = events, cols = CLUSTER_FEATURES)
    matrix = np.array(
        [[e[f] if e[f] is not None else 0.0 for f in CLUSTER_FEATURES]
         for e in events],
        dtype=float,
    )

    X_scaled, means, stds = _normalise(matrix)
    labels = run_dbscan(X_scaled)

    n_noise    = int(np.sum(labels == -1))
    n_clusters = len(set(labels) - {-1})

    cluster_stats = compute_cluster_stats(events, labels, means, stds)

    # Build event→cluster_id map (None for noise)
    label_map: Dict[str, Optional[int]] = {e["id"]: None for e in events}
    for lbl, idx_list in zip(
        [cs["label"] for cs in cluster_stats],
        [cs["event_ids"] for cs in cluster_stats],
    ):
        for eid in idx_list:
            label_map[eid] = lbl  # temporary DBSCAN label, remapped below

    # Persist and get (sequential_id, event_ids) mapping
    cluster_id_pairs = save_clusters(db, circuit, cluster_stats, circuit_type)

    # Remap event IDs to the new sequential cluster IDs
    final_map: Dict[str, Optional[int]] = {e["id"]: None for e in events}
    for seq_id, event_ids in cluster_id_pairs:
        for eid in event_ids:
            final_map[eid] = seq_id

    write_event_cluster_ids(db, circuit, final_map)
    build_cooccurrence(db, circuit)

    db.commit()

    cluster_ids = [seq_id for seq_id, _ in cluster_id_pairs]
    log.info(
        "[%s] clustering complete — %d events → %d clusters, %d noise",
        circuit, n_events, n_clusters, n_noise,
    )

    return {
        "n_events":   n_events,
        "n_clusters": n_clusters,
        "n_noise":    n_noise,
        "cluster_ids": cluster_ids,
    }
