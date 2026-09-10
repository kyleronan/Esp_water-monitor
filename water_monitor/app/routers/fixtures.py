"""Fixtures router."""
from __future__ import annotations

import json

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import ingress_redirect, startup_gate
from ..circuit_compat import resolve_circuit
from ..config import DATA_DIR, DB_PATH, DEV_TOOLS
from ..database import (
    cleanup_composite_flags,
    coalesce_low_flow_events,
    find_orphaned_cluster_references,
    get_active_exclusion_window,
    get_all_cluster_stats,
    get_category_rollup,
    get_clusters_with_fixtures,
    get_orphaned_fixtures,
    get_write_lock,
    recompute_all_user_label_suggestions,
    recompute_cycle_pulse_counts,
    resuggest_all_clusters,
    run_db,
    run_isolated_write,
    snapshot_database)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/fixtures")


def _orch(request: Request):
    return request.app.state.orchestrator


def _tmpl(request: Request):
    return request.app.state.templates


def _valid_circuit(circuit: str, request: Request) -> str:
    """FastAPI dependency — normalises legacy aliases then validates against configured circuits."""
    circuit = resolve_circuit(circuit)
    cfg = _orch(request)._cfg
    if circuit not in {c.circuit for c in cfg.circuits}:
        raise HTTPException(status_code=404, detail=f"Unknown circuit: {circuit!r}")
    return circuit


_HEALTH_UNITS = {"volume_trend": "L", "duration_trend": "seconds"}


def _health_reference(base, signal):
    """The baseline scalar comparable to this signal's ``observed``, or None.

    None is a real answer here: unsolicited_refills counts events, class_share
    is a proportion and anchor_claim_rate is a rate, so none of them has a
    frozen median in the same units, and printing one would be worse than
    printing nothing.
    """
    if base is None:
        return None
    if signal == "volume_trend":
        return base.volume_median
    if signal == "duration_trend":
        return base.duration_median
    return None


# ── Page ──────────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def fixtures_page(request: Request, preview: bool = False):
    """Per-category rollup (one card per fixture type per circuit).

    The clustering engine continues to produce micro-clusters underneath; this
    page is a pure view-layer rollup that buckets events by their effective
    type via fixtures.normalize_fixture_type_for_circuit. Correction of
    mis-bucketed events happens on the History page.
    """
    # Lifetime rollups over every event — gate on startup.
    gated = startup_gate(request, "fixtures", "Water Use", "/fixtures")
    if gated is not None:
        return gated
    orch = _orch(request)
    from ..fixtures import FIXTURE_TYPE_LABELS

    # Time-range selector (top of page). Each option is a rolling window back
    # to HA-local midnight N days ago — the same helper the dashboard uses for
    # get_daily_volume, so the "today" boundary matches across pages.
    # "lifetime" = no lower bound (None). Unknown/hand-edited values clamp to
    # "today" so the page never 500s.
    _RANGE_DAYS = {"today": 0, "week": 7, "month": 30, "3months": 90,
                   "6months": 183, "year": 365, "2years": 730}
    _RANGE_LABELS = {"today": "today", "week": "past week", "month": "past month",
                     "3months": "past 3 months", "6months": "past 6 months",
                     "year": "past year", "2years": "past 2 years",
                     "lifetime": "all-time"}
    sel_range = request.query_params.get("range", "today")
    if sel_range not in _RANGE_DAYS and sel_range != "lifetime":
        sel_range = "today"
    range_start_utc = (None if sel_range == "lifetime"
                       else orch._local_midnight_utc(days_ago=_RANGE_DAYS[sel_range]))
    range_label = _RANGE_LABELS[sel_range]

    # This whole page is DB work with no awaits in the loop, so it goes over the
    # wall in ONE hop: per-circuit rollups, publish maps, exclusion windows,
    # orphan lists and the stale-link count. Run inline on the event loop they
    # contend with the DB worker statement by statement.
    circuits_ctx, stale_link_count, health_ctx = await run_db(
        _fixtures_page_payload, orch, range_start_utc)

    return _tmpl(request).TemplateResponse("fixtures.html", {
        "request":             request,
        "page":                "fixtures",
        "circuits":            circuits_ctx,
        "fixture_type_labels": FIXTURE_TYPE_LABELS,
        "preview":             preview,
        "sel_range":           sel_range,
        "range_label":         range_label,
        "stale_link_count":    stale_link_count,
        "health":              health_ctx,
        "dev_tools":           _dev_tools_enabled(),
    })


def _dev_tools_enabled() -> bool:
    return bool(DEV_TOOLS)


def _fixtures_page_payload(orch, range_start_utc):
    """Every DB read behind the Water Use page, in one DB-thread callable.

    Returns ``(circuits_ctx, stale_link_count, health_ctx)``. Extracted from
    the handler so no statement runs on the event-loop thread; the health and
    review reads ride the SAME hop for the same reason.
    """
    from ..fixtures import (FIXTURE_TYPE_LABELS,
                            fixture_user_selectable_types,
                            normalize_fixture_type_for_circuit,
                            zone_user_selectable_types)

    # Icon map — keep aligned with main.py:_FX_ICONS and the canonical type
    # set. (Duplicated rather than imported to keep main.py's
    # internal _FX_ICONS private; both are tiny dicts that share semantics.)
    fx_icons = {
        "toilet":          "\U0001F6BD",
        "shower_tub":      "\U0001F6BF",
        "tap":             "\U0001F6B0",
        "washing_machine": "\U0001F455",
        "dishwasher":      "\U0001F37D",
        "irrigation_zone": "\U0001F4A7",
        "other":           "❓",
    }

    circuits_ctx = []
    for circ_cfg in orch._cfg.circuits:
        c = circ_cfg.circuit
        circuit_kind = "zone" if circ_cfg.circuit_type == "zone" else "fixture"
        allowed = (zone_user_selectable_types() if circuit_kind == "zone"
                   else fixture_user_selectable_types())

        training = (
            orch.training_manager.get_training_info(c)
            if orch.training_manager
            else {"state": "idle"}
        )
        state = training.get("state", "idle")

        # Seed an empty canonical map — every allowed category gets a card,
        # even with zero events, so the user sees the full set on day one.
        categories = {
            t: {
                "type":                 t,
                "label":                FIXTURE_TYPE_LABELS.get(t, t.replace("_", " ").title()),
                "icon":                 fx_icons.get(t, "❓"),
                "range_volume_l":       0.0,
                "range_event_count":    0,
                "lifetime_volume_l":    0.0,
                "lifetime_event_count": 0,
                "last_seen_at":         None,
            }
            for t in allowed
        }

        # Pull the per-category rollup.
        raw_rows = get_category_rollup(orch.db, c, range_start_utc)

        # Merge SQL rows through the normalizer so legacy / wrong-kind types
        # fold into 'other' rather than producing extra cards.
        for row in raw_rows:
            typ = normalize_fixture_type_for_circuit(row["eff_type"], circuit_kind)
            bucket = categories[typ]   # guaranteed present by seed
            bucket["lifetime_volume_l"]    += row["lifetime_volume_l"]
            bucket["lifetime_event_count"] += row["lifetime_event_count"]
            bucket["range_volume_l"]       += row["range_volume_l"]
            bucket["range_event_count"]    += row["range_event_count"]
            bucket["last_seen_at"] = _max_iso(bucket["last_seen_at"],
                                              row["last_seen_at"])

        # Lifetime-desc order; empty cards last so the user sees real usage
        # first.
        ordered = sorted(
            categories.values(),
            key=lambda r: (r["lifetime_event_count"] == 0,
                           -r["lifetime_volume_l"], r["label"]),
        )

        circuits_ctx.append({
            "circuit":          c,
            "display_name":     circ_cfg.label,
            "training_state":   state,
            "categories":       ordered,
            "active_exclusion": get_active_exclusion_window(orch.db, c),
            # The orphan-relink workflow lives on a different surface, but the
            # warning is still useful here.
            "orphaned_fixtures": get_orphaned_fixtures(orch.db, c),
        })

    # Events whose fixture-group id points at a deleted group. The relink
    # banner only covers unbacked FIXTURES; live matching re-mints this class
    # against the dead in-memory ids (1,776 in prod by 2026-08-16).
    try:
        stale_link_count = orch.db.execute(
            """SELECT COUNT(*) FROM events e
               WHERE e.cluster_id IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM fixture_clusters fc
                   WHERE fc.circuit = e.circuit AND fc.id = e.cluster_id)"""
        ).fetchone()[0]
    except Exception:
        stale_link_count = 0

    # Fixture health and the review queue, on the SAME DB
    # hop as everything else this page reads. Both are strictly best-effort:
    # a home that has not pinned a baseline, or an install predating the
    # migration, must render the page exactly as before rather than 500.
    health_ctx = {"alerts": [], "queue": {}, "learning": {}, "overlaps": {}}
    try:
        from ..fixture_health import load_baseline, open_alerts
        from ..review_queue import build_card
        for circ in circuits_ctx:
            cid = circ["circuit"]
            # What the learning loop has been deciding, from the
            # ledger. Best-effort like everything else here; absent on a home
            # with no decision on record.
            try:
                from ..learning_loop import learning_status
                st = learning_status(orch.db, cid, str(DATA_DIR))
                if st.get("available"):
                    st["circuit_name"] = circ.get("display_name") or cid
                    health_ctx["learning"][cid] = st
            except Exception:                       # noqa: BLE001
                pass
            # Overlap groups where water is still counted twice. VISIBILITY
            # ONLY — this surface reports the litres; it applies no volume
            # policy of its own.
            try:
                from ..overlap_guard import summarize_overlap_groups
                from ..reprocess import _SPLIT_LOOKBACK_H
                _og = [g for g in summarize_overlap_groups(
                            orch.db, cid, retention_hours=_SPLIT_LOOKBACK_H)
                       if g["state"] != "resolved"]
                if _og:
                    health_ctx["overlaps"][cid] = {
                        "circuit_name": circ.get("display_name") or cid,
                        "groups": len(_og),
                        "litres": round(sum(g["excess_l"] for g in _og), 1),
                        "within_retention": sum(1 for g in _og if g["within_retention"]),
                    }
            except Exception:                       # noqa: BLE001
                pass
            for alert in open_alerts(orch.db, cid):
                detail = {}
                try:
                    detail = json.loads(alert.get("detail_json") or "{}")
                except (TypeError, ValueError):
                    detail = {}
                base = load_baseline(orch.db, cid, alert["fixture_type"])
                health_ctx["alerts"].append({
                    "id": alert["id"],
                    "circuit": cid,
                    "circuit_name": circ.get("display_name") or cid,
                    "fixture_type": alert["fixture_type"],
                    "signal": alert["signal"],
                    "opened_at": alert["opened_at"],
                    "observed": detail.get("observed"),
                    "threshold": detail.get("threshold"),
                    "events_to_fire": detail.get("events_to_fire"),
                    # The reference depends on WHICH signal fired. volume_trend
                    # and duration_trend share one rolling-median detector, so
                    # `observed` is litres for one and seconds for the other;
                    # pairing either with volume_median unconditionally prints
                    # a duration against a volume. Signals with no
                    # comparable scalar get None, and the template then omits the
                    # comparison rather than inventing one.
                    "baseline_median": _health_reference(base, alert["signal"]),
                    "unit": _HEALTH_UNITS.get(alert["signal"]),
                })
            try:
                card = build_card(orch.db, cid)
                if card.items:
                    # The card holds two DIFFERENT questions and the template
                    # has to tell them apart: identity slots are events the
                    # ladder could not type, anchor slots are events it typed
                    # confidently and wants confirmed. Describing all of them as
                    # "could not type confidently" is wrong for the anchors, in
                    # the direction that matters: it asks the operator to
                    # identify something already named.
                    from ..review_queue import KIND_ANCHOR
                    n_anchor = sum(1 for it in card.items
                                   if it.kind == KIND_ANCHOR)
                    health_ctx["queue"][cid] = {
                        "shown": len(card.items),
                        "n_anchor": n_anchor,
                        "n_identity": len(card.items) - n_anchor,
                        "waiting": card.n_candidates,
                        "circuit_name": circ.get("display_name") or cid,
                    }
            except Exception:                       # noqa: BLE001
                pass
    except Exception as exc:                        # noqa: BLE001
        log.debug("fixture-health context unavailable: %s", exc)

    return circuits_ctx, stale_link_count, health_ctx


def _max_iso(a, b):
    """Return the lexicographically-greater ISO timestamp, None-safe.

    Both inputs may be None; this just folds across rows during the rollup
    bucketing. events.start_ts is always written as UTC ISO with timezone
    suffix by feature_extractor, so a lex compare suffices.
    """
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


# ── Per-category publish toggle ─────────────────────────────────────────────

# ── The per-cluster routes (confirm, delete, merge, migrate,
#    forget-signature, relink) have no HTTP handlers. Their backend helpers in
#    database.py stay — cluster_engine and the tests consume them.
# ────────────────────────────────────────────────────────────────────────────


# ── Repair stale group links ─────────────────────────────────────────────────

@router.post("/health/{alert_id}/resolve")
async def resolve_health_alert(alert_id: int, request: Request):
    """Close a fixture-health alert, and say which of the two things happened.

    The reason code is not bookkeeping — it decides what the reference becomes:

    * ``fixture_repaired`` UNLOCKS the baseline, so the fixture is re-measured
      against how it behaves now. Correct after replacing a flapper: the old
      normal is gone.
    * ``false_alarm`` KEEPS the baseline. Correct when nothing was wrong —
      re-pinning here would quietly adopt the drifted behaviour as normal and
      guarantee the alert never fires again, which is the exact failure this
      whole feature exists to prevent.

    CSRF is enforced by the project-wide middleware.
    """
    orch = _orch(request)
    form = await request.form()
    reason = str(form.get("reason") or "").strip()
    from ..fixture_health import (REASON_REPAIRED,
                                  UNLOCK_REASONS, resolve_alert,
                                  unlock_baseline)
    if reason not in UNLOCK_REASONS:
        return ingress_redirect(request, "/fixtures?msg=error")

    def _job(conn):
        row = conn.execute(
            "SELECT circuit, fixture_type FROM fixture_health_alert "
            "WHERE id = ?", (alert_id,)).fetchone()
        if row is None:
            return False
        resolve_alert(conn, alert_id, reason)
        if reason == REASON_REPAIRED:
            # Unlock only. The nightly job re-pins from a CLOSED window, so the
            # new reference cannot be built from the days that are still
            # settling after the repair.
            unlock_baseline(conn, row["circuit"], row["fixture_type"], reason)
            conn.execute(
                "DELETE FROM fixture_baseline WHERE circuit = ? "
                "AND fixture_type = ?", (row["circuit"], row["fixture_type"]))
        conn.commit()
        return True

    try:
        async with get_write_lock():
            ok = await run_db(_job)
    except Exception as e:                          # noqa: BLE001
        log.error("resolve-health-alert failed: %s", e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    if not ok:
        return ingress_redirect(request, "/fixtures?msg=error")
    log.info("fixture-health alert %s resolved as %s", alert_id, reason)
    return ingress_redirect(request, "/fixtures?msg=health_resolved")


# ── The reference-set prompt ("Re-pin benchmark now" / "Not now") ───────────

@router.post("/repin-benchmark/{circuit}")
async def repin_benchmark(circuit: str, request: Request):
    """Operator-confirmed re-pin from the Water Use prompt.

    The trigger names WHY (regime / decay / growth / shrink) and is recorded
    on the ledger row. Over an active set the new selection lands as PENDING
    and takes over at the next model change-over; a refusal (pool headroom,
    open health alert) comes back as a message, never a silent no-op.
    """
    orch = _orch(request)
    form = await request.form()
    trigger = str(form.get("trigger") or "").strip()
    from ..learning_loop import REPIN_TRIGGER_REASONS, pin_benchmark_for_circuit
    if trigger not in REPIN_TRIGGER_REASONS:
        return ingress_redirect(request, "/fixtures?msg=error")
    circuit = resolve_circuit(circuit)
    try:
        async with get_write_lock():
            res = await run_db(pin_benchmark_for_circuit, orch.db, circuit,
                               trigger=trigger, source="auto",
                               reason=f"operator confirmed the {trigger} prompt on Water Use")
    except Exception as e:                          # noqa: BLE001
        log.error("[%s] benchmark re-pin failed: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    if res.get("status") == "refused":
        from urllib.parse import quote
        return ingress_redirect(
            request, f"/fixtures?msg=benchmark_refused&why={quote(str(res.get('reason') or ''))}")
    msg = "benchmark_pending" if res.get("status") == "pending" else "benchmark_pinned"
    return ingress_redirect(request, f"/fixtures?msg={msg}")


@router.post("/repin-benchmark/{circuit}/dismiss")
async def dismiss_repin_benchmark(circuit: str, request: Request):
    """'Not now' — silences this instance of the prompt only."""
    orch = _orch(request)
    form = await request.form()
    key = str(form.get("key") or "").strip()
    if not key:
        return ingress_redirect(request, "/fixtures?msg=error")
    circuit = resolve_circuit(circuit)
    from ..learning_loop import dismiss_repin_prompt
    try:
        async with get_write_lock():
            await run_db(dismiss_repin_prompt, orch.db, circuit, [key])
    except Exception as e:                          # noqa: BLE001
        log.error("[%s] dismissing the re-pin prompt failed: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(request, "/fixtures?msg=repin_dismissed")


@router.post("/repair-stale-links")
async def repair_stale_links(request: Request):
    """Null events whose fixture-group id points at a deleted group, then
    rebuild the in-memory engine so its id map (derived
    from those events' votes) can no longer resurrect the dead ids. Without
    the rebuild, live matching re-mints stale references immediately."""
    orch = _orch(request)
    engine = getattr(orch, "cluster_engine", None)
    # Refuse while the startup cluster work is still replaying: it holds a
    # pre-repair snapshot in executor threads, and whichever rebuild finishes
    # LAST owns the in-memory map. A repair clicked 15 s after a restart loses
    # that race and the cleared references come back.
    if not getattr(orch, "startup_cluster_work_done", True):
        return ingress_redirect(request, "/fixtures?msg=starting")
    try:
        # The write lock is async and is acquired OUTSIDE
        # run_db — never inside a callable on the single DB worker.
        async with get_write_lock():
            counts = await run_db(find_orphaned_cluster_references,
                                  orch.db, repair=True)
            if engine:
                for c in orch._cfg.circuits:
                    # RESET before replay. rebuild_from_db does not clear the
                    # live engine's model or river→DB id map (only _init_circuit
                    # makes them fresh), so replaying on top of a poisoned map
                    # leaves the dominant center's stale dead-cluster entry in
                    # place and the next backfill re-mints orphans through it
                    # (1,736 of them, ninety seconds after a repair).
                    engine.reset_circuit(c.circuit)
                    await run_db(engine.rebuild_from_db, c.circuit)
        log.info("repair-stale-links: %s (engine reset + rebuilt)", counts)
    except Exception as e:
        log.error("repair-stale-links failed: %s", e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(
        request,
        f"/fixtures?msg=links_repaired&fixed={counts['events_orphaned']}")


# ── Re-run clustering ─────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster")
async def retrigger_cluster(request: Request, circuit: str = Depends(_valid_circuit)):
    """Rebuild DBSTREAM state from DB — resets in-memory engine and replays
    the last 60 days so the fixture_clusters table reflects current history."""
    orch   = _orch(request)
    engine = getattr(orch, "cluster_engine", None)
    if not engine:
        return ingress_redirect(request, "/fixtures?msg=error")
    try:
        # Serialise against recompute/reclassify/other rebuilds — all share the
        # write lock so heavy DB writers never run concurrently on the engine's
        # shared connection. The lock is held OUTSIDE run_db.
        async with get_write_lock():
            count = await run_db(engine.rebuild_from_db, circuit)
        log.info("[%s] manual rebuild: %d events replayed", circuit, count)
        if count == 0:
            return ingress_redirect(request, "/fixtures?msg=too_few_events")
        msg = "reclustered"
    except Exception as e:
        log.error("[%s] re-cluster error: %s", circuit, e, exc_info=True)
        msg = "error"
    return ingress_redirect(request, f"/fixtures?msg={msg}")


# ── Activate fixtures (labelling → live) ──────────────────────────────────────

@router.post("/{circuit}/activate")
async def activate_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Transition labelling → live when the user is satisfied with their
    cluster labels.  No-op (with error flash) if circuit isn't currently
    in labelling state — typically a stale browser tab."""
    orch = _orch(request)
    tm = orch.training_manager
    if not tm:
        return ingress_redirect(request, "/fixtures?msg=error")
    ok = await tm.activate_fixtures(circuit)
    if not ok:
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(request, "/fixtures?msg=activated")


@router.post("/{circuit}/reclassify")
async def reclassify_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Retrain fixture-type signatures from the user's labels and backfill
    ``matched_fixture_type`` over every unlabelled event on this circuit.

    Idempotent and safe: never touches user-labelled rows, writes NULL on
    abstention (clearing stale matches), and never persists 'other'.
    """
    from ..reclassify import reclassify_all_events_from_signatures
    if get_write_lock().locked():
        return ingress_redirect(request, "/fixtures?msg=busy")
    try:
        res = await run_isolated_write(
            DB_PATH, lambda c: reclassify_all_events_from_signatures(c, circuit))
        log.info("[%s] manual reclassify: %s", circuit, res)
    except Exception as e:
        log.error("[%s] reclassify_circuit failed: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(
        request,
        f"/fixtures?msg=reclassified&matched={res['events_matched']}"
        f"&abstained={res['events_abstained']}&cleared={res['events_cleared']}",
    )


@router.post("/{circuit}/recompute")
async def recompute_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Re-derive volume + active-flow for this circuit's events from the raw HA
    flow history (applies the over-count fix retroactively). Retention-limited:
    events whose flow history has aged out keep their stored volume.
    """
    from datetime import datetime, timezone, timedelta

    orch = _orch(request)
    cfg = next((c for c in orch._cfg.circuits if c.circuit == circuit), None)
    if cfg is None or not getattr(cfg, "flow_sensor", None):
        return ingress_redirect(request, "/fixtures?msg=error")
    try:
        from ..volume_recompute import (build_flow_fetch,
                                         recompute_volume_and_active_flow)
        from ..reclassify import reclassify_all_events_from_signatures
        # Reject (don't silently queue for many seconds) if another heavy DB
        # write is already running — recompute/reclassify/recluster all share
        # the write lock. Best-effort UX guard; correctness is the lock itself.
        if get_write_lock().locked():
            return ingress_redirect(request, "/fixtures?msg=busy")
        rng = await run_db(
            lambda: orch.db.execute(
                "SELECT MIN(start_ts) mn, MAX(end_ts) mx FROM events "
                "WHERE circuit = ?", (circuit,),
            ).fetchone())
        if not rng or not rng["mn"]:
            return ingress_redirect(
                request, "/fixtures?msg=recomputed&recomputed=0&skipped=0&degraded=0")

        def _p(s):
            return datetime.fromisoformat(str(s).replace("Z", "+00:00"))

        retention_floor = datetime.now(timezone.utc) - timedelta(days=10)
        range_start = max(_p(rng["mn"]) - timedelta(minutes=15), retention_floor)
        range_end = _p(rng["mx"]) + timedelta(minutes=5)
        fetch = await build_flow_fetch(orch._ha, cfg.flow_sensor,
                                       range_start, range_end)

        # Run the whole recompute → cleanup → reclassify sequence on a single
        # private connection, serialised by the write lock (see
        # database.run_isolated_write) — this is what keeps concurrent
        # recomputes off a shared connection.
        def _job(conn):
            r = recompute_volume_and_active_flow(conn, circuit, fetch)
            cleanup_composite_flags(conn)
            # Coalesce low-flow sensor-chatter fragments (one sustained low
            # draw the turbine chopped into many tiny events) into one event each.
            # DESTRUCTIVE (merges + deletes rows) but volume-preserving — snapshot
            # the DB first, and only when there is actually something to merge (so
            # clean recomputes stay backup-free). Runs BEFORE reclassify so all
            # downstream sees merged events. This is the ONLY place coalesce runs —
            # never silently at startup (the recovery gate).
            plan = coalesce_low_flow_events(conn, circuit, dry_run=True)
            if plan["absorbed"]:
                snapshot_database(conn, DB_PATH, "pre-coalesce")
                cres = coalesce_low_flow_events(conn, circuit)
                log.info("[%s] coalesced %d low-flow fragment(s) into %d group(s)",
                         circuit, cres["absorbed"], cres["groups"])
            # Cycle-pulse backfill MUST precede reclassify so the matcher's
            # cycle_pulse_count feature is populated before it types events.
            recompute_cycle_pulse_counts(conn, circuit)
            reclassify_all_events_from_signatures(conn, circuit)
            resuggest_all_clusters(conn, circuit)                # heuristic clusters
            recompute_all_user_label_suggestions(conn, circuit)  # gated user-label clusters
            return r

        res = await run_isolated_write(DB_PATH, _job)
        log.info("[%s] manual recompute: %s", circuit, res)
    except Exception as e:
        log.error("[%s] recompute_circuit failed: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(
        request,
        f"/fixtures?msg=recomputed&recomputed={res['recomputed']}"
        f"&skipped={res['skipped']}&degraded={res['degraded']}",
    )


# ── JSON API ──────────────────────────────────────────────────────────────────

@router.get("/api/{circuit}/clusters")
async def api_clusters(request: Request, circuit: str = Depends(_valid_circuit)):
    db = _orch(request).db
    all_stats = get_all_cluster_stats(db, circuit)
    clusters = [{**cl, **all_stats.get(cl["id"], {})}
                for cl in get_clusters_with_fixtures(db, circuit)]
    return JSONResponse(clusters)
