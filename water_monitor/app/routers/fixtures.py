"""Fixtures router — Phase 2."""
from __future__ import annotations

import json

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import ingress_redirect, startup_gate
from ..circuit_compat import resolve_circuit

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
    """Sprint F: per-category rollup (one card per fixture type per circuit).

    The clustering engine continues to produce micro-clusters underneath; this
    page is a pure view-layer rollup that buckets events by their effective
    type via fixtures.normalize_fixture_type_for_circuit. Correction of
    mis-bucketed events happens on the History page (Sprint B feedback loop).
    """
    # dev46 (46c): lifetime rollups over every event — gate on startup.
    gated = startup_gate(request, "fixtures", "Water Use", "/fixtures")
    if gated is not None:
        return gated
    orch = _orch(request)
    from ..fixtures import FIXTURE_TYPE_LABELS

    # Time-range selector (top of page). Each option is a rolling window back
    # to HA-local midnight N days ago — the same helper the dashboard uses for
    # get_daily_volume, so the "today" boundary matches across pages.
    # "lifetime" = no lower bound (None). Unknown/hand-edited values clamp to
    # "today" so the page never 500s. Default "today" preserves prior behaviour.
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

    # dev46 (46a) — this whole page is DB work with no awaits in the loop, so
    # it goes over the wall in ONE hop: per-circuit rollups, publish maps,
    # exclusion windows, orphan lists and the stale-link count all ran inline
    # on the event loop before, contending with the DB worker statement by
    # statement.
    from ..database import run_db
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
    })


def _fixtures_page_payload(orch, range_start_utc):
    """Every DB read behind the Water Use page, in one DB-thread callable.

    Returns ``(circuits_ctx, stale_link_count, health_ctx)``. dev46 (46a):
    extracted from the handler so no statement runs on the event-loop thread —
    dev47's health and review reads ride the SAME hop for the same reason.
    """
    from ..database import (get_active_exclusion_window, get_category_rollup,
                            get_category_publish_map, get_orphaned_fixtures)
    from ..fixtures import (FIXTURE_TYPE_LABELS,
                            fixture_user_selectable_types,
                            normalize_fixture_type_for_circuit,
                            zone_user_selectable_types)

    # Icon map — keep aligned with main.py:_FX_ICONS and the post-Sprint-D
    # canonical type set. (Duplicated rather than imported to keep main.py's
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
                "publish_to_ha":        True,    # default; overwritten from map
            }
            for t in allowed
        }

        # Pull rollup + publish gate map.
        raw_rows = get_category_rollup(orch.db, c, range_start_utc)
        publish_map = get_category_publish_map(orch.db, c)

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

        # Per-category publish gate — missing rows default to True (publish on).
        for typ, bucket in categories.items():
            bucket["publish_to_ha"] = bool(publish_map.get(typ, True))

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
            # Sprint A banner — kept; the orphan-relink workflow lives on a
            # different surface but the warning is still useful here.
            "orphaned_fixtures": get_orphaned_fixtures(orch.db, c),
        })

    # dev42 (U4 cleanup) — events whose fixture-group id points at a deleted
    # group. The old "relink banner" only covers unbacked FIXTURES; this class
    # had no UI surface at all, while live matching kept re-minting them
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

    # dev47 (47i / 47d) — fixture health and the review queue, on the SAME DB
    # hop as everything else this page reads. Both are strictly best-effort:
    # a home that has not pinned a baseline, or an install predating the
    # migration, must render the page exactly as before rather than 500.
    health_ctx = {"alerts": [], "queue": {}}
    try:
        from ..fixture_health import load_baseline, open_alerts
        from ..review_queue import build_card
        for circ in circuits_ctx:
            cid = circ["circuit"]
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
                    # pairing either with volume_median unconditionally (as this
                    # did) prints a duration against a volume. Signals with no
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
                    # "could not type confidently" was wrong for the anchors —
                    # and wrong in the direction that matters, because it asks
                    # the operator to identify something already named.
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


# ── Sprint F: per-category publish toggle ────────────────────────────────────

@router.post("/{circuit}/category/{fixture_type}/publish")
async def category_publish_toggle(
    fixture_type: str,
    request: Request,
    circuit: str = Depends(_valid_circuit),
):
    """Flip the HA-publish gate for one (circuit, fixture_type).

    Validation, in order:
      1. CSRF — enforced by the project-wide middleware (POST routes covered).
      2. circuit exists — handled by the ``_valid_circuit`` dependency.
      3. circuit_kind resolved from the configured circuit_type ('zone' vs
         'fixture').
      4. fixture_type allowed for that kind — rejects e.g. ``dishwasher`` on
         a zone circuit or ``irrigation_zone`` on a fixture circuit.
      5. Unchecked checkbox (form field absent) → publish_to_ha = 0.

    On success: persist via ``set_category_publish`` (which also re-validates
    defensively) and call ``fixture_publisher.publish_category`` /
    ``retract_category`` so the HA entity flips immediately rather than
    waiting for the next 60 s state tick.
    """
    from ..database import set_category_publish
    from ..fixtures import (fixture_user_selectable_types,
                            zone_user_selectable_types)

    orch = _orch(request)
    circ_cfg = next((c for c in orch._cfg.circuits if c.circuit == circuit), None)
    if circ_cfg is None:
        # Defence-in-depth — _valid_circuit already returned 404 for
        # unknown circuits, but keep this branch obvious to a reader.
        raise HTTPException(status_code=404, detail=f"Unknown circuit: {circuit!r}")
    circuit_kind = "zone" if circ_cfg.circuit_type == "zone" else "fixture"
    allowed = set(zone_user_selectable_types() if circuit_kind == "zone"
                  else fixture_user_selectable_types())
    if fixture_type not in allowed:
        raise HTTPException(
            status_code=400,
            detail=(f"fixture_type {fixture_type!r} not allowed on "
                    f"{circuit_kind} circuit {circuit!r}"),
        )

    form = await request.form()
    publish_to_ha = 1 if form.get("publish_to_ha") == "1" else 0
    from ..database import run_db
    await run_db(set_category_publish, orch.db, circuit,      # dev46 (46a)
                 fixture_type, publish_to_ha)

    # Apply to the live HA broker immediately (no wait for the periodic tick).
    fp = getattr(orch, "_fixture_publisher", None) or getattr(orch, "fixture_publisher", None)
    if fp is not None:
        try:
            if publish_to_ha:
                fp.publish_category(circuit, fixture_type)
            else:
                fp.retract_category(circuit, fixture_type)
        except Exception as e:
            # Don't fail the request if the publisher is offline — the DB
            # state is what governs the next tick.
            log.warning("category_publish_toggle: publisher call failed "
                        "for %s/%s: %s", circuit, fixture_type, e)

    return ingress_redirect(request, f"/fixtures#cat-{fixture_type}")


# ── LEGACY per-cluster routes (kept registered but unlinked from the Sprint F
#    template). Backend helpers they call (upsert_fixture_from_cluster,
#    delete_cluster, merge_clusters, migrate_to_type_level_clusters,
#    delete_fixture_signature) remain consumed by cluster_engine and tests;
#    pruning these route handlers themselves is a future cleanup pass.
# ────────────────────────────────────────────────────────────────────────────


# ── Repair stale group links (dev42) ─────────────────────────────────────────

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
    from ..fixture_health import (REASON_FALSE_ALARM, REASON_REPAIRED,
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
        from ..database import get_write_lock, run_db
        async with get_write_lock():
            ok = await run_db(_job)
    except Exception as e:                          # noqa: BLE001
        log.error("resolve-health-alert failed: %s", e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    if not ok:
        return ingress_redirect(request, "/fixtures?msg=error")
    log.info("fixture-health alert %s resolved as %s", alert_id, reason)
    return ingress_redirect(request, "/fixtures?msg=health_resolved")


@router.post("/repair-stale-links")
async def repair_stale_links(request: Request):
    """dev42 (U4 cleanup) — null events whose fixture-group id points at a
    deleted group, then rebuild the in-memory engine so its id map (derived
    from those events' votes) can no longer resurrect the dead ids. Without
    the rebuild, live matching re-mints stale references immediately."""
    orch = _orch(request)
    from ..database import find_orphaned_cluster_references, get_write_lock
    engine = getattr(orch, "cluster_engine", None)
    # dev44 — refuse while the startup cluster work is still replaying: it
    # holds a pre-repair snapshot in executor threads, and whichever rebuild
    # finishes LAST owns the in-memory map. A repair clicked 15 s after a
    # restart lost exactly that race and the cleared references returned.
    if not getattr(orch, "startup_cluster_work_done", True):
        return ingress_redirect(request, "/fixtures?msg=starting")
    try:
        import asyncio
        from ..database import run_db
        # dev46 (46a/N2c): the write lock is async and is acquired OUTSIDE
        # run_db — never inside a callable on the single DB worker.
        async with get_write_lock():
            counts = await run_db(find_orphaned_cluster_references,
                                  orch.db, repair=True)
            if engine:
                for c in orch._cfg.circuits:
                    # dev44: RESET before replay. rebuild_from_db does not
                    # clear the live engine's model or river→DB id map (at
                    # startup _init_circuit made them fresh) — replaying on
                    # top of a poisoned map left the dominant center's stale
                    # dead-cluster entry in place, and the very next backfill
                    # re-minted 1,736 orphans through it (observed 8/16
                    # 10:20:43, ninety seconds after the first repair).
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
        from ..database import get_write_lock, run_db
        # Serialise against recompute/reclassify/other rebuilds — all share the
        # write lock so heavy DB writers never run concurrently on the engine's
        # shared connection. (dev46 46a: the lock is held OUTSIDE run_db.)
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


# ── Confirm / save a cluster label ────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/confirm")
async def confirm_cluster(request: Request, cluster_id: int, circuit: str = Depends(_valid_circuit)):
    form         = await request.form()
    name         = (form.get("name") or "").strip()
    fixture_type = (form.get("fixture_type") or "other").strip()
    publish      = 1 if form.get("publish_to_ha") else 0

    if not name:
        name = fixture_type.replace("_", " ").title()

    orch = _orch(request)
    from ..database import run_db, upsert_fixture_from_cluster
    fixture_id = await run_db(                                # dev46 (46a)
        upsert_fixture_from_cluster,
        orch.db, circuit, cluster_id, name, fixture_type, publish
    )
    if publish and fixture_id:
        fp = getattr(orch, "_fixture_publisher", None)
        if fp:
            fp.publish_fixture(fixture_id)
    # Notify the cluster engine so the type-aware match gate takes effect
    # immediately — no restart needed.
    engine = orch.cluster_engine
    if engine:
        engine.notify_fixture_confirmed(circuit, cluster_id, fixture_type)
    return ingress_redirect(request, "/fixtures")


# ── Relink an orphaned fixture to a cluster (Sprint A) ────────────────────────

@router.post("/{circuit}/fixture/{fixture_id}/relink-cluster")
async def relink_orphaned_fixture(
    request: Request,
    fixture_id: str,
    circuit: str = Depends(_valid_circuit),
):
    """Attach a fixture flagged as ``cluster_backfill_needed`` to a chosen
    cluster on the same circuit. Form field: ``cluster_id``.

    Conservative gate (in database.relink_fixture_to_cluster):
      - fixture must exist on this circuit
      - cluster must exist on this circuit
      - target cluster must not already be linked to a different fixture
    On success: clears the backfill flag, updates the cluster, re-publishes
    to HA so the entity reappears, and notifies the cluster engine so its
    type-aware match gate applies immediately.
    """
    form = await request.form()
    try:
        cluster_id = int(form.get("cluster_id") or 0)
    except (TypeError, ValueError):
        return ingress_redirect(request, "/fixtures?msg=error")
    if cluster_id <= 0:
        return ingress_redirect(request, "/fixtures?msg=error")

    orch = _orch(request)
    from ..database import relink_fixture_to_cluster, run_db
    try:
        await run_db(relink_fixture_to_cluster,               # dev46 (46a)
                     orch.db, circuit, fixture_id, cluster_id)
    except ValueError as e:
        log.warning("[%s] relink %s → cluster %d rejected: %s",
                    circuit, fixture_id, cluster_id, e)
        return ingress_redirect(request, "/fixtures?msg=relink_failed")
    except Exception as e:
        log.error("[%s] relink %s → cluster %d failed: %s",
                  circuit, fixture_id, cluster_id, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")

    # Re-publish to HA so the entity reappears for this fixture.
    fp = getattr(orch, "_fixture_publisher", None)
    if fp:
        try:
            fp.publish_fixture(fixture_id)
        except Exception as e:
            log.error("[%s] publish_fixture %s after relink failed: %s",
                      circuit, fixture_id, e)

    # Tell the cluster engine to apply the type-aware match gate to events
    # landing in this cluster going forward.
    engine = getattr(orch, "cluster_engine", None)
    if engine:
        # Look up the fixture_type to feed the notification.
        row = await run_db(                                   # dev46 (46a)
            lambda: orch.db.execute(
                "SELECT fixture_type FROM fixtures WHERE id = ?", (fixture_id,)
            ).fetchone())
        ftype = row["fixture_type"] if row else None
        if ftype:
            try:
                engine.notify_fixture_confirmed(circuit, cluster_id, ftype)
            except Exception as e:
                log.error("[%s] notify_fixture_confirmed after relink "
                          "failed: %s", circuit, e)

    log.info("[%s] relinked fixture %s → cluster %d", circuit, fixture_id, cluster_id)
    return ingress_redirect(request, "/fixtures?msg=relinked")


# ── Forget a signature (Sprint C) ─────────────────────────────────────────────

@router.post("/{circuit}/signature/{fixture_type}/forget")
async def forget_signature(
    request: Request,
    fixture_type: str,
    circuit: str = Depends(_valid_circuit),
):
    """Remove a learned-from-labels signature so future events stop being
    tagged ``matched_fixture_type = <fixture_type>`` by the signature matcher.

    Useful when the user realises they mis-labelled some events and wants
    the matcher to start fresh — they don't have to hunt down each
    individual labelled event. (Re-labelling will repopulate the signature
    on the next save.)
    """
    orch = _orch(request)
    from ..database import delete_fixture_signature, run_db
    try:
        removed = await run_db(delete_fixture_signature,      # dev46 (46a)
                               orch.db, circuit, fixture_type)
    except Exception as e:
        log.error("[%s] forget signature %s failed: %s",
                  circuit, fixture_type, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    if not removed:
        log.info("[%s] forget signature %s: no row existed",
                 circuit, fixture_type)
    log.info("[%s] forgot signature for fixture_type=%s",
             circuit, fixture_type)
    return ingress_redirect(request, "/fixtures?msg=signature_forgotten")


# ── Delete a cluster ──────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/delete")
async def delete_cluster_endpoint(request: Request, cluster_id: int, circuit: str = Depends(_valid_circuit)):
    orch = _orch(request)
    from ..database import delete_cluster, get_fixture_id_for_cluster, run_db

    def _lookup_and_delete():
        # dev46 (46a/N2a): lookup + delete in ONE callable — the id must not
        # be read in one hop and acted on in another.
        fid = get_fixture_id_for_cluster(orch.db, circuit, cluster_id)
        delete_cluster(orch.db, circuit, cluster_id)
        return fid

    fixture_id = await run_db(_lookup_and_delete)
    if fixture_id:
        fp = getattr(orch, "_fixture_publisher", None)
        if fp:
            fp.retract_fixture(fixture_id)
    # Drop the cluster from the type cache so the gate no longer applies
    # to any subsequent river center that re-maps to this slot.
    engine = orch.cluster_engine
    if engine:
        engine.notify_fixture_removed(circuit, cluster_id)
    return ingress_redirect(request, "/fixtures")


# ── Merge clusters ────────────────────────────────────────────────────────────

@router.get("/{circuit}/merge", response_class=HTMLResponse)
async def merge_page(request: Request, circuit: str = Depends(_valid_circuit)):
    """Show a confirmation page for merging the selected clusters."""
    raw_ids = request.query_params.get("ids", "")
    try:
        ids = list(dict.fromkeys(int(i) for i in raw_ids.split(",") if i.strip()))
    except ValueError:
        return ingress_redirect(request, "/fixtures?msg=error")
    if len(ids) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    orch = _orch(request)
    from ..database import get_clusters_with_fixtures, get_all_cluster_stats
    from ..fixtures import FIXTURE_TYPE_LABELS
    from ..database import run_db
    clusters_raw, all_stats = await run_db(                   # dev46 (46a)
        lambda: (get_clusters_with_fixtures(orch.db, circuit),
                 get_all_cluster_stats(orch.db, circuit)))
    clusters_by_id = {
        cl["id"]: {**cl, **all_stats.get(cl["id"], {})}
        for cl in clusters_raw
    }

    selected = [clusters_by_id[i] for i in ids if i in clusters_by_id]
    if len(selected) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    default_survivor = max(selected, key=lambda c: c.get("member_count") or 0)
    total_events = sum(c.get("event_count") or 0 for c in selected)
    total_members = sum(c.get("member_count") or 0 for c in selected)
    confirmed_count = sum(1 for c in selected if c.get("fixture_id"))

    return _tmpl(request).TemplateResponse("fixtures_merge.html", {
        "request":          request,
        "page":             "fixtures",
        "circuit":          circuit,
        "clusters":         selected,
        "default_survivor": default_survivor["id"],
        "total_events":     total_events,
        "total_members":    total_members,
        "confirmed_count":  confirmed_count,
        "fixture_type_labels": FIXTURE_TYPE_LABELS,
    })


@router.post("/{circuit}/merge")
async def merge_clusters_endpoint(
    request: Request, circuit: str = Depends(_valid_circuit)
):
    """Execute a multi-cluster merge after user confirmation."""
    form = await request.form()
    try:
        survivor_id = int(form.get("survivor_id") or 0)
        ids = list(dict.fromkeys(
            int(v) for v in form.getlist("ids") if str(v).strip()
        ))
    except (ValueError, TypeError):
        return ingress_redirect(request, "/fixtures?msg=error")

    if survivor_id not in ids or len(ids) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    orch = _orch(request)
    from ..database import merge_clusters, get_fixture_id_for_cluster

    # Collect non-survivor fixture IDs before merge, for HA retraction only.
    deleted_ids = [i for i in ids if i != survivor_id]

    def _collect_and_merge():
        # dev46 (46a/N2a): the pre-merge id sweep and the merge itself are one
        # transaction's worth of work — one callable on the single DB thread.
        retract = [fid for i in deleted_ids
                   if (fid := get_fixture_id_for_cluster(orch.db, circuit, i))]
        return retract, merge_clusters(orch.db, circuit, survivor_id, ids)

    from ..database import run_db
    try:
        retract_fixture_ids, summary = await run_db(_collect_and_merge)
    except ValueError as e:
        log.warning("[%s] merge validation failed: %s", circuit, e)
        return ingress_redirect(request, "/fixtures?msg=merge_failed")
    except Exception as e:
        log.error("[%s] merge_clusters error: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=merge_failed")

    # Retract deleted HA fixture entities.
    fp = getattr(orch, "_fixture_publisher", None)
    if fp:
        for fid in retract_fixture_ids:
            try:
                fp.retract_fixture(fid)
            except Exception as e:
                log.error("[%s] retract_fixture %s failed: %s", circuit, fid, e)

    # Rebuild engine state to heal river_id_map and type_cache.
    engine = getattr(orch, "cluster_engine", None)
    if engine:
        try:
            from ..database import get_write_lock, run_db
            async with get_write_lock():
                await run_db(engine.rebuild_from_db, circuit)
        except Exception as e:
            log.error("[%s] rebuild_from_db after merge failed: %s", circuit, e)

    log.info(
        "[%s] merged fixture clusters: survivor=%d, deleted=%s, "
        "events_relinked=%d, fixtures_removed=%d",
        circuit, survivor_id, deleted_ids,
        summary["events_relinked"], summary["fixtures_removed"],
    )
    return ingress_redirect(request, "/fixtures?msg=merged")


# ── Type-level migration ──────────────────────────────────────────────────────

@router.post("/{circuit}/migrate")
async def migrate_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Merge same-type clusters into one cluster per fixture type per circuit.

    Conservative: only merges clusters that pass the safety gate (member
    count, confidence, centroid distance).  Does not auto-confirm
    suggested-only clusters.
    """
    orch = _orch(request)
    from ..database import migrate_to_type_level_clusters, run_db

    def _migrate_and_commit(conn):
        # dev46 (46a/N2a): the migration's writes and their commit belong to
        # ONE transaction, so they must live in ONE run_db callable. Splitting
        # them across two hops would leave the transaction open across a
        # queue boundary, letting a foreign statement land inside it — a
        # silent correctness bug on a shared connection, with no exception.
        summary = migrate_to_type_level_clusters(conn, circuit)
        conn.commit()
        return summary

    try:
        summary = await run_db(_migrate_and_commit, orch.db)
        engine = getattr(orch, "cluster_engine", None)
        if engine:
            await run_db(engine.rebuild_from_db, circuit)
        log.info("[%s] type-level migration: %s", circuit, summary)
    except Exception as e:
        log.error("[%s] migrate_circuit failed: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(request, "/fixtures?msg=migrated")


@router.post("/{circuit}/reclassify")
async def reclassify_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Retrain fixture-type signatures from the user's labels and backfill
    ``matched_fixture_type`` over every unlabelled event on this circuit.

    Idempotent and safe: never touches user-labelled rows, writes NULL on
    abstention (clearing stale matches), and never persists 'other'.
    """
    from ..database import (reclassify_all_events_from_signatures,
                            run_isolated_write, get_write_lock)
    from ..config import DB_PATH
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
        from ..database import (reclassify_all_events_from_signatures,
                                cleanup_composite_flags, run_isolated_write,
                                get_write_lock, recompute_cycle_pulse_counts,
                                resuggest_all_clusters,
                                recompute_all_user_label_suggestions,
                                coalesce_low_flow_events, run_db,
                                snapshot_database)
        from ..config import DB_PATH
        # Reject (don't silently queue for many seconds) if another heavy DB
        # write is already running — recompute/reclassify/recluster all share
        # the write lock. Best-effort UX guard; correctness is the lock itself.
        if get_write_lock().locked():
            return ingress_redirect(request, "/fixtures?msg=busy")
        rng = await run_db(                                   # dev46 (46a)
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
        # database.run_isolated_write). This is what fixes the shared-connection
        # races that crashed concurrent recomputes.
        def _job(conn):
            r = recompute_volume_and_active_flow(conn, circuit, fetch)
            cleanup_composite_flags(conn)
            # dev.24: coalesce low-flow sensor-chatter fragments (one sustained low
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
            # dev.22: cycle-pulse backfill MUST precede reclassify so the matcher's
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
    from ..database import get_clusters_with_fixtures, get_all_cluster_stats
    db = _orch(request).db
    all_stats = get_all_cluster_stats(db, circuit)
    clusters = [{**cl, **all_stats.get(cl["id"], {})}
                for cl in get_clusters_with_fixtures(db, circuit)]
    return JSONResponse(clusters)
