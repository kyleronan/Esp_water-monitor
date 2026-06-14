"""Leave-one-out evaluation of the k-NN fixture-type matcher (database._knn_vote).

READ-ONLY: opens the DB with mode=ro. Mirrors match_event_to_signature_knn's
active-vs-legacy selection, reusing the LIVE feature tuples / scales / constants
from water_monitor.app.database — so it reflects the current code, and re-running
after a constant change shows that change's effect. Reports overall accuracy,
coverage, and per-class recall; supports sweeping the dev.22 constants without
editing source via --cycle-scale / --max-per-class.

Usage:
    py -3 water_monitor/tools/eval_knn_classifier.py <db_path> [circuit]
        [--cycle-scale X] [--max-per-class N]
"""
import argparse
import os
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from water_monitor.app import database as db  # noqa: E402


def _load_labelled(conn, circuit):
    feats = list(dict.fromkeys(
        db._SIGNATURE_MATCH_FEATURES + db._SIGNATURE_KNN_ACTIVE_FEATURES
        + ("has_pressure_transient",)))   # the rules tier's flush predicate input
    cols = ["id", "user_fixture_type", "integration_quality"] + feats
    rows = conn.execute(
        f"SELECT {', '.join(cols)} FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND user_fixture_type <> '' "
        "  AND COALESCE(excluded_from_training, 0) = 0",
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _predict_loo(held, others):
    """Mirror match_event_to_signature_knn: active-flow first (when the query has
    active features and enough active-labelled neighbours), else legacy."""
    has_active = all(held.get(f) is not None for f in
                     ("true_avg_flow_lpm", "active_flow_duration_seconds", "flow_on_ratio"))
    if has_active:
        active = [(db._canonical_fixture_type(o["user_fixture_type"]), o) for o in others
                  if o.get("true_avg_flow_lpm") is not None
                  and o.get("active_flow_duration_seconds") is not None
                  and o.get("flow_on_ratio") is not None
                  and (o.get("integration_quality") or "ok") == "ok"]
        active = [(t, o) for t, o in active if t]
        if len(active) >= db._SIGNATURE_KNN_MIN_TOTAL_LABELS:
            hit = db._knn_vote(active, held, db._SIGNATURE_KNN_ACTIVE_FEATURES,
                               db._SIGNATURE_KNN_ACTIVE_SCALES,
                               db._SIGNATURE_KNN_ACTIVE_LOG_FEATURES)
            if hit is not None:
                return hit["fixture_type"]
    legacy = [(db._canonical_fixture_type(o["user_fixture_type"]), o) for o in others]
    legacy = [(t, o) for t, o in legacy if t]
    if len(legacy) < db._SIGNATURE_KNN_MIN_TOTAL_LABELS:
        return None
    hit = db._knn_vote(legacy, held, db._SIGNATURE_MATCH_FEATURES,
                       db._SIGNATURE_KNN_SCALES, db._SIGNATURE_LOG_FEATURES)
    return hit["fixture_type"] if hit else None


def evaluate(db_path, circuit, cycle_scale=None, max_per_class=None,
             with_rules=False):
    """LOO evaluation. ``with_rules`` prepends the dev.23 structural rules tier
    (washer-cycle sweep + per-event rules) before the k-NN residual — the
    production reclassify order. The washer-cycle set is computed once from
    features/timestamps only (label-free; see test_detector_is_label_free), so
    the leave-one-out stays honest."""
    if cycle_scale is not None:
        db._SIGNATURE_KNN_ACTIVE_SCALES["cycle_pulse_count"] = cycle_scale
        db._SIGNATURE_KNN_SCALES["cycle_pulse_count"] = cycle_scale
    if max_per_class is not None and hasattr(db, "_SIGNATURE_KNN_MAX_PER_CLASS"):
        db._SIGNATURE_KNN_MAX_PER_CLASS = max_per_class

    washer_ids = {}
    softener_ids = {}
    circuit_type = "fixture"
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        labelled = _load_labelled(conn, circuit)
        if with_rules:
            from water_monitor.app.event_rules import (
                detect_softener_sessions, detect_washer_cycles,
                parse_hhmm_to_minutes)
            circuit_type = db.get_circuit_type(conn, circuit)
            if circuit_type != "zone":
                washer_ids = detect_washer_cycles(conn, circuit)
            # dev.24 — water-softener sessions (label-free, configured band).
            # Guarded: a pre-dev.24 export DB lacks the home_profile softener
            # columns. tz=None compares the regen band in the stored (UTC) clock,
            # so set softener_regen_start to the UTC-equivalent time when evaling.
            try:
                prof = db.get_home_profile(conn)
                if (prof is not None and prof["has_water_softener"]
                        and (prof["softener_circuit"] or "main") == circuit):
                    band = parse_hhmm_to_minutes(prof["softener_regen_start"])
                    if band is not None:
                        softener_ids = detect_softener_sessions(conn, circuit, band)
            except (KeyError, IndexError):
                pass   # pre-dev.24 schema — no softener config
    finally:
        conn.close()

    total = correct = abstained = 0
    cls_total = defaultdict(int)
    cls_correct = defaultdict(int)
    confusion = defaultdict(lambda: defaultdict(int))
    if with_rules:
        from water_monitor.app.event_rules import rule_classify_event
    for i, held in enumerate(labelled):
        actual = db._canonical_fixture_type(held["user_fixture_type"])
        if not actual:
            continue
        pred = None
        if with_rules:
            if held["id"] in softener_ids:
                pred = "water_softener"
            elif held["id"] in washer_ids:
                pred = "washing_machine"
            else:
                rule_hit = rule_classify_event(held, circuit_type)
                if rule_hit is not None:
                    pred = rule_hit[0]
        if pred is None:
            pred = _predict_loo(held, labelled[:i] + labelled[i + 1:])
        total += 1
        cls_total[actual] += 1
        if pred is None:
            abstained += 1
            confusion[actual]["<abstain>"] += 1
        else:
            confusion[actual][pred] += 1
            if pred == actual:
                correct += 1
                cls_correct[actual] += 1
    return {"total": total, "correct": correct, "abstained": abstained,
            "cls_total": dict(cls_total), "cls_correct": dict(cls_correct),
            "confusion": {k: dict(v) for k, v in confusion.items()}}


# ── Held-out / k-fold eval: home-FITTED rule bands vs FROZEN defaults ───────────
# The plan's eval gate: scored against EXPLICIT (user/training) labels only — never
# auto-labels (circular) — and the held-out events are excluded from their fold's
# fit. Per-event rule tier only (toilet/dishwasher/shower/zone); washer & softener
# are DB-window cycle detectors not covered by this gate.
_RULE_TYPES = frozenset({"toilet", "dishwasher", "shower_tub", "irrigation_zone"})


def _kfold_core(pool_rows, explicit, circuit_type, k):
    """Pure k-fold comparison (deterministic round-robin folds, no RNG). Returns
    ``(agg, avg_fit_keys)``. ``pool_rows`` is every event (the fit pool, weighted by
    provenance); ``explicit`` is the user/training test set. Held-out events are
    removed from their fold's fit so they never leak into their own band."""
    from water_monitor.app.event_rules import rule_classify_event
    from water_monitor.app.rule_calibration import fit_rule_constants_from_rows
    test = [e for e in explicit
            if db._canonical_fixture_type(e.get("user_fixture_type")) in _RULE_TYPES]
    folds = [[] for _ in range(k)]
    for idx, ev in enumerate(sorted(test, key=lambda e: str(e["id"]))):
        folds[idx % k].append(ev)
    agg = {m: {"total": 0, "fired": 0, "correct": 0,
               "cls_total": defaultdict(int), "cls_correct": defaultdict(int)}
           for m in ("fitted", "frozen")}
    fit_key_counts = []
    for fold in folds:
        if not fold:
            continue
        held_ids = {e["id"] for e in fold}
        train_rows = [r for r in pool_rows if r.get("id") not in held_ids]
        fitted_calib, _ = fit_rule_constants_from_rows(train_rows)
        fit_key_counts.append(len(fitted_calib))
        for ev in fold:
            actual = db._canonical_fixture_type(ev.get("user_fixture_type"))
            for mode, calib in (("fitted", fitted_calib), ("frozen", None)):
                hit = rule_classify_event(ev, circuit_type, calib=calib)
                pred = hit[0] if hit else None
                a = agg[mode]
                a["total"] += 1
                a["cls_total"][actual] += 1
                if pred is not None:
                    a["fired"] += 1
                    if pred == actual:
                        a["correct"] += 1
                        a["cls_correct"][actual] += 1
    avg = sum(fit_key_counts) / len(fit_key_counts) if fit_key_counts else 0.0
    return agg, avg


def kfold_rule_eval(db_path, circuit, k=5):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        circuit_type = db.get_circuit_type(conn, circuit)
        pool_rows = [dict(r) for r in conn.execute(
            "SELECT id, user_fixture_type, fixture_label_source, matched_fixture_type, "
            "       matched_via, "
            "       COALESCE(excluded_from_training,0) AS excluded_from_training, "
            "       volume_litres, duration_seconds, peak_flow_lpm "
            "FROM events WHERE circuit = ?", (circuit,)).fetchall()]
        explicit = [dict(r) for r in conn.execute(
            "SELECT id, user_fixture_type, volume_litres, duration_seconds, "
            "       peak_flow_lpm, cycle_pulse_count, has_pressure_transient, "
            "       pressure_delta_psi "
            "FROM events WHERE circuit = ? "
            "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
            "  AND COALESCE(excluded_from_training,0)=0 "
            "  AND fixture_label_source IN ('user','training') "
            "ORDER BY id", (circuit,)).fetchall()]
    finally:
        conn.close()
    return _kfold_core(pool_rows, explicit, circuit_type, k)


def _print_kfold(agg, avg_fit_keys, k):
    print(f"\n=== k-fold (k={k}) rule-tier eval: home-fitted vs frozen-default ===")
    print("  scored on explicit (user/training) labels only; per-event rules only")
    print("  (washer/softener cycle detectors are not covered by this gate)")
    for mode in ("fitted", "frozen"):
        a = agg[mode]
        t = a["total"] or 1
        print(f"  {mode:7s} accuracy={a['correct']}/{a['total']}={a['correct']/t:.3f}"
              f"  coverage={a['fired']}/{a['total']}={a['fired']/t:.3f}")
    print(f"  avg fitted bands per fold: {avg_fit_keys:.1f}")
    ok = agg["fitted"]["correct"] >= agg["frozen"]["correct"]
    print("  GATE: " + ("PASS — fitted >= frozen (ship the fit)"
                        if ok else "FAIL — fitted < frozen (keep defaults / re-tune)"))
    for mode in ("fitted", "frozen"):
        a = agg[mode]
        print(f"  -- {mode} per-class recall --")
        for c in sorted(a["cls_total"]):
            n = a["cls_total"][c]
            okc = a["cls_correct"].get(c, 0)
            print(f"     {c:16s} {okc:3d}/{n:<3d} = {okc / n:.3f}")


def _print(res, label):
    t = res["total"] or 1
    cov = res["total"] - res["abstained"]
    print(f"\n=== {label} ===")
    print(f"events={res['total']}  accuracy={res['correct']}/{res['total']}="
          f"{res['correct']/t:.3f}  coverage={cov}/{res['total']}={cov/t:.3f}")
    print("per-class recall (correct / total):")
    for c in sorted(res["cls_total"]):
        n = res["cls_total"][c]
        ok = res["cls_correct"].get(c, 0)
        print(f"  {c:18s} {ok:3d}/{n:<3d} = {ok/n:.3f}   "
              f"conf={res['confusion'].get(c, {})}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("circuit", nargs="?", default="main")
    ap.add_argument("--cycle-scale", type=float, default=None)
    ap.add_argument("--max-per-class", type=int, default=None)
    ap.add_argument("--with-rules", action="store_true",
                    help="prepend the dev.23 structural rules tier (production order)")
    ap.add_argument("--kfold", type=int, default=None,
                    help="held-out k-fold eval: home-FITTED rule bands vs FROZEN "
                         "defaults, scored on explicit labels only (the fit gate)")
    a = ap.parse_args()
    if a.kfold:
        agg, avg = kfold_rule_eval(a.db_path, a.circuit, a.kfold)
        _print_kfold(agg, avg, a.kfold)
        return
    res = evaluate(a.db_path, a.circuit, a.cycle_scale, a.max_per_class,
                   with_rules=a.with_rules)
    lbl = (f"{a.circuit} cycle_scale={a.cycle_scale} "
           f"max_per_class={a.max_per_class} rules={a.with_rules}")
    _print(res, lbl)


if __name__ == "__main__":
    main()
