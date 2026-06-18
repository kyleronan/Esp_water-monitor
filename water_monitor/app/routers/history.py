"""History router — event log and leak test history."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ..circuit_compat import resolve_circuit
from ..fixtures import FIXTURE_TYPE_LABELS, user_selectable_types
from ..database import patch_event as _patch_event
from ._helpers import run_blocking

_VALID_USER_FIXTURE_TYPES: frozenset = frozenset(user_selectable_types())

log = logging.getLogger(__name__)
router = APIRouter(prefix="/history")

DEFAULT_EVENT_LIMIT = 100


def _orch(r): return r.app.state.orchestrator
def _tmpl(r): return r.app.state.templates


async def _bg_reclassify(circuit: str) -> None:
    """Fire-and-forget k-NN reclassify after a label change — offloaded so the
    label POST returns immediately (reclassify can be slow on a large history).
    Runs on a private connection under the write lock (dev.8 run_isolated_write),
    so it never races live writes. §2.4 — tracked as a job so a FAILURE is surfaced
    to the UI (success is silent; the label change already gave instant feedback).
    The user's own label + any cycle-mates are applied synchronously before this."""
    from ..database import (reclassify_all_events_from_signatures,
                            run_isolated_write, start_job, finish_job)
    from ..config import DB_PATH

    def _work(c):
        job = start_job(c, "reclassify", circuit, "Reclassifying events…")
        try:
            reclassify_all_events_from_signatures(c, circuit)
            finish_job(c, job, "done", "Reclassify complete")
        except Exception:
            finish_job(c, job, "error", "Reclassify failed — see addon log")
            raise

    try:
        await run_isolated_write(DB_PATH, _work)
    except Exception as e:
        log.warning("[%s] background reclassify failed: %s", circuit, e)


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def history_page(request: Request):
    try:
        return await _history_page(request)
    except Exception:
        # Log with full traceback at ERROR; the user-facing page stays
        # generic so we don't leak exception text / stack info into the
        # HTML. Pointing at the addon log is enough actionable detail
        # for the user; the rest belongs in the log only.
        log.error("History page error", exc_info=True)
        return HTMLResponse(
            "<h1>History temporarily unavailable</h1>"
            "<p>Something went wrong rendering this page. "
            "Check the addon log for details, or refresh to retry.</p>",
            status_code=500,
        )


def _collect_circuit_history_sync(
    db,
    circuits,
    date_from: str,
    date_to: str,
    chart_range: str,
    chart_from,
    filter_param: str,
    filter_circuit: str,
    today,
) -> list[dict]:
    """Synchronous bundle of the history page's per-circuit DB work.

    Owns the per-circuit loop so the calling async handler can offload
    it via run_blocking() in one executor hop. Everything inside is
    plain sqlite3 + dict assembly — no awaits.
    """
    from ..database import (get_recent_events, get_leak_test_history,
                            get_daily_summaries, get_home_profile)
    # Read the hide-phantom display preference once (same for all circuits).
    # Display-only: phantom volume is already zeroed at detection, so hiding
    # never changes any total — it only removes rows from the History list.
    _profile = get_home_profile(db)
    hide_phantoms = bool(_profile and _profile["hide_pressure_artifact_events"])
    hide_cross_talk = bool(_profile and _profile["hide_cross_talk_events"])
    out: list[dict] = []
    for circuit_cfg in circuits:
        if filter_circuit and circuit_cfg.circuit != filter_circuit:
            continue
        events = get_recent_events(
            db, circuit_cfg.circuit,
            limit=DEFAULT_EVENT_LIMIT,
            date_from=date_from or None,
            date_to=date_to or None,
        )
        # Dashboard "Degraded supply" View-link uses ?filter=degraded.
        # Post-filter rather than a dedicated SQL path so the rest of
        # the rendering machinery doesn't need to change.
        if filter_param == "degraded":
            events = [e for e in events if dict(e).get("degraded_supply")]
        # Settings "Hide pressure-artifact events" toggle.
        if hide_phantoms:
            events = [e for e in events
                      if not dict(e).get("is_pressure_restoration_phantom")]
        # Settings "Hide cross-talk events" toggle (independent of the phantom one).
        if hide_cross_talk:
            events = [e for e in events if not dict(e).get("is_cross_talk")]
        leak_tests = get_leak_test_history(db, circuit_cfg.circuit, limit=20)
        summaries  = get_daily_summaries(
            db, circuit_cfg.circuit,
            date_from=chart_from,
        )

        # For YoY: also fetch prior-year summaries (shifted 365 days).
        prior_summaries = []
        if chart_range == "yoy" and chart_from:
            from datetime import date as _date, timedelta as _td
            prior_from = (_date.fromisoformat(chart_from)
                          - _td(days=365)).isoformat()
            prior_to   = (today - _td(days=365)).isoformat()
            prior_summaries = get_daily_summaries(
                db, circuit_cfg.circuit,
                date_from=prior_from,
                date_to=prior_to,
            )

        # Hourly-volume fallback: aggregate to daily for chart when no
        # summaries exist yet (first-day install).
        hv_daily: dict = {}
        if not summaries:
            hv_rows = db.execute("""
                SELECT date(hour_ts) AS day, SUM(volume_litres) AS vol
                FROM hourly_volume
                WHERE circuit = ?
                  AND (? IS NULL OR hour_ts >= ?)
                GROUP BY date(hour_ts)
                ORDER BY day ASC
            """, (circuit_cfg.circuit, chart_from, chart_from)).fetchall()
            hv_daily = {r["day"]: r["vol"] for r in hv_rows}

        out.append({
            "circuit":         circuit_cfg.circuit,
            "display_name":    circuit_cfg.label,
            "events":          events,
            "leak_tests":      leak_tests,
            "event_count":     len(events),
            "summaries":       summaries,
            "prior_summaries": prior_summaries,
            "hv_daily":        hv_daily,
        })
    return out


async def _history_page(request: Request):
    orch = _orch(request)
    cfg  = orch._cfg

    date_from   = request.query_params.get("from", "").strip()
    date_to     = request.query_params.get("to",   "").strip()
    chart_range = request.query_params.get("range", "30d")
    # Optional filters surfaced by the Dashboard "Degraded supply" banner
    # link. `filter=degraded` restricts the per-circuit events list to
    # degraded-supply rows; `circuit=...` further scopes to one circuit.
    filter_param  = request.query_params.get("filter",  "").strip().lower()
    filter_circuit = resolve_circuit(request.query_params.get("circuit", "").strip())
    # 30d | 6m | 1y | all | monthly | yearly | yoy
    using_range = bool(date_from or date_to)

    from datetime import date, timedelta as td
    today = date.today()
    chart_from_map = {
        "30d":    (today - td(days=30)).isoformat(),
        "6m":     (today - td(days=183)).isoformat(),
        "1y":     (today - td(days=365)).isoformat(),
        "all":    None,
        "monthly": today.replace(day=1).isoformat(),
        "yearly":  today.replace(month=1, day=1).isoformat(),
        "yoy":    (today - td(days=730)).isoformat(),
    }
    chart_from = chart_from_map.get(chart_range, chart_from_map["30d"])

    # All per-circuit DB work (recent events with full columns, leak
    # test history, daily summaries, optional YoY summaries, hourly
    # volume fallback) runs in a single thread-pool hop instead of
    # blocking the event loop per query. On a 2-circuit deployment
    # with a year of events this is the difference between a ~150 ms
    # dashboard-fight and a clean async hand-off.
    circuit_history = await run_blocking(
        _collect_circuit_history_sync,
        orch.db,
        list(cfg.circuits),
        date_from, date_to, chart_range, chart_from,
        filter_param, filter_circuit,
        today,
    )

    fixture_type_options = [
        {"value": k, "label": FIXTURE_TYPE_LABELS.get(k, k.replace("_", " ").title())}
        for k in user_selectable_types()
    ]

    return _tmpl(request).TemplateResponse("history.html", {
        "request":              request,
        "circuit_history":      circuit_history,
        "page":                 "history",
        "date_from":            date_from,
        "date_to":              date_to,
        "using_range":          using_range,
        "default_limit":        DEFAULT_EVENT_LIMIT,
        "chart_range":          chart_range,
        "fixture_type_options": fixture_type_options,
        "fixture_type_labels":  FIXTURE_TYPE_LABELS,
        "filter_param":         filter_param,
        "filter_circuit":       filter_circuit,
    })


@router.get("/api/event/{event_id}/waveform")
async def event_waveform(event_id: str, request: Request):
    """Return the high-resolution min/max waveform for one event.

    Source: the `event_waveforms` table, populated by feature_extractor's
    `_persist_waveform` after each new event is processed. Historical events
    captured before this migration won't have a waveform row — clients get
    a 404 and should fall back to the existing 32-point signature display.
    """
    import json as _json
    orch = _orch(request)
    row = orch.db.execute(
        """SELECT w.flow_min_json, w.flow_max_json,
                  w.pressure_min_json, w.pressure_max_json,
                  w.duration_seconds,
                  e.start_ts, e.degraded_supply
           FROM event_waveforms w
           JOIN events e ON w.event_id = e.id
           WHERE w.event_id = ?""",
        (event_id,),
    ).fetchone()
    if not row:
        return JSONResponse({"error": "waveform not found"}, status_code=404)
    try:
        return JSONResponse({
            "event_id":         event_id,
            "start_ts":         row["start_ts"],
            "duration_seconds": row["duration_seconds"],
            "degraded_supply":  bool(row["degraded_supply"]),
            "flow_min":         _json.loads(row["flow_min_json"]),
            "flow_max":         _json.loads(row["flow_max_json"]),
            "pressure_min":     _json.loads(row["pressure_min_json"]),
            "pressure_max":     _json.loads(row["pressure_max_json"]),
        })
    except (ValueError, TypeError) as e:
        log.warning("Waveform JSON decode failed for %s: %s", event_id, e)
        return JSONResponse({"error": "waveform corrupted"}, status_code=500)


@router.get("/api/events/{circuit}")
async def events_api(
    circuit: str,
    request: Request,
    limit: int = DEFAULT_EVENT_LIMIT,
    date_from: str = "",
    date_to: str = "",
):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    from ..database import get_recent_events
    events = get_recent_events(
        orch.db, circuit,
        limit=limit,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return JSONResponse(events)


@router.patch("/api/events/{circuit}/{event_id}")
async def patch_event_api(circuit: str, event_id: str, request: Request):
    """Update user-editable fields on a single event.

    Accepted payload keys (all optional):
      user_fixture_type (str | null) — assign or clear a fixture type label.
      excluded_from_training (bool)  — ignore / restore the event.
    """
    payload = await request.json()
    db = _orch(request).db

    # Validate fixture type before touching the DB
    if "user_fixture_type" in payload:
        ftype = payload["user_fixture_type"] or None
        if ftype is not None and ftype not in _VALID_USER_FIXTURE_TYPES:
            return JSONResponse(
                {"error": f"Invalid fixture type: {ftype!r}"},
                status_code=400,
            )

    propagation_meta: dict = {}
    signature_meta: dict = {}
    classification_meta = None

    # Sprint H — manual classification (independent checkboxes). Authoritative
    # over auto-detection; a Phantom mark zeroes volume from totals. Sprint H.1:
    # a manual save (incl. all-false = "normal") always locks; reset:true
    # returns the event to automatic detection. Processed alongside (not
    # instead of) a fixture-type change, so the single Save button persists
    # both in one request — the client only sends `classification` when the
    # user actually changed a checkbox, so labelling alone never locks it.
    if "classification" in payload:
        from ..database import (set_event_classification,
                                clear_event_classification, classify_action)
        action, data = classify_action(payload["classification"])
        if action == "error":
            return JSONResponse({"error": data["msg"]}, status_code=400)
        if action == "reset":
            ok = clear_event_classification(db, event_id, circuit)
        else:
            ok = set_event_classification(
                db, event_id, circuit,
                phantom=data["phantom"],
                supply_pressure=data["supply_pressure"],
                dribble=data["dribble"],
                cross_talk=data["cross_talk"],
            )
        if not ok:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        classification_meta = {"action": action, "classification": data}

    kwargs: dict = {}
    if "user_fixture_type" in payload:
        kwargs["user_fixture_type"] = payload["user_fixture_type"] or None
    if "excluded_from_training" in payload:
        # Ignore/Restore — route to the explicit user_ignored intent (Sprint H);
        # patch_event re-derives effective excluded_from_training.
        kwargs["user_ignored"] = bool(payload["excluded_from_training"])

    # B7 — guard against accidentally EXCLUDING a sparse 'training' anchor: that
    # silently unseeds the class from the k-NN, and in History the event looks
    # ordinary. Warn (single confirm) only on this harmful action — relabel and
    # excluding a 'cycle'/'user' event are unaffected.
    if payload.get("excluded_from_training") and not payload.get("confirm_exclude"):
        srow = db.execute(
            "SELECT fixture_label_source, user_fixture_type FROM events "
            "WHERE id = ? AND circuit = ?", (event_id, circuit)).fetchone()
        if srow and srow["fixture_label_source"] == "training":
            ftype = srow["user_fixture_type"] or "this fixture"
            others = db.execute(
                "SELECT COUNT(*) FROM events WHERE circuit = ? "
                "  AND fixture_label_source = 'training' AND user_fixture_type = ? "
                "  AND id <> ? AND COALESCE(excluded_from_training, 0) = 0",
                (circuit, srow["user_fixture_type"], event_id)).fetchone()[0]
            scope = "the only" if others == 0 else "one of several"
            return JSONResponse({
                "needs_confirm": "exclude_training",
                "message": (f"This event was captured during setup and is {scope} "
                            f"training example for {ftype}. Excluding it may leave "
                            f"that fixture unclassified. Exclude anyway?"),
            })

    if kwargs:
        found = _patch_event(db, event_id, circuit, **kwargs)
        if not found:
            return JSONResponse({"error": "Event not found"}, status_code=404)

    # Sprint B — if the patch touched user_fixture_type, propagate the
    # signal into the cluster's suggested_type (soft hint; never silently
    # links a cluster to a fixture). When the event has no cluster_id we
    # have nowhere to land the signal — log it so the gap is visible.
    if "user_fixture_type" in payload:
        try:
            from ..database import (
                recompute_cluster_suggestion_from_user_labels,
                upsert_fixture_signature,
            )
            row = db.execute(
                "SELECT cluster_id, excluded_from_training "
                "FROM events WHERE id = ? AND circuit = ?",
                (event_id, circuit),
            ).fetchone()
            cid = row["cluster_id"] if row else None
            if cid is not None:
                result = recompute_cluster_suggestion_from_user_labels(
                    db, circuit, int(cid)
                )
                if result is not None:
                    propagation_meta["cluster_id"] = int(cid)
                    propagation_meta["suggested_type"] = result["suggested_type"]
                    propagation_meta["labelled_member_count"] = \
                        result["labelled_member_count"]
                    propagation_meta["total_label_count"] = \
                        result["total_label_count"]
            elif row and not row["excluded_from_training"]:
                # Event eligible for clustering but has no cluster_id yet (the
                # preliminary-match-without-commit gap). The label IS recorded and
                # trains the k-NN classifier — only the legacy cluster bulk-propagation
                # is skipped until the next backfill assigns this event a cluster.
                # DEBUG, not INFO: a known low-impact gap, not an error, and it would
                # otherwise fire on every label of an un-clustered event.
                log.debug(
                    "[%s] event %s labelled with cluster_id NULL — label recorded and "
                    "trains the classifier; cluster propagation follows once it is "
                    "clustered (next backfill)",
                    circuit, event_id,
                )

            # Sprint C — refresh the fixture_type signature for whichever
            # type(s) this patch touched. We refresh the NEW type and,
            # if the user changed an existing label, also the OLD type
            # (otherwise the old signature keeps a stale event member).
            old_type = payload.get("_previous_user_fixture_type")
            new_type = payload.get("user_fixture_type") or None
            for sig_type in {t for t in (new_type, old_type) if t}:
                sig = upsert_fixture_signature(db, circuit, sig_type)
                if sig is not None:
                    signature_meta[sig_type] = {
                        "member_count": sig["member_count"],
                    }

            # 2a — auto-label this appliance event's cycle-mates (source='cycle')
            # so one label seeds several. No-op for non-appliance types. Commit so
            # the cycle labels are durable + visible to the History reload and the
            # background reclassify below.
            from ..database import propagate_cycle_label
            cycle_propagated = propagate_cycle_label(db, circuit, event_id, new_type)
            if cycle_propagated:
                db.commit()
                propagation_meta["cycle_propagated"] = cycle_propagated

            # Re-run the label-trained k-NN over unlabelled events so the new label
            # spreads + stale matched_fixture_type clears. Offloaded to the background
            # so the POST returns immediately (the user's own label + cycle-mates are
            # already applied synchronously above).
            import asyncio
            asyncio.create_task(_bg_reclassify(circuit))
        except Exception as e:
            # Propagation is best-effort. A failure here must not block
            # the label save the user already saw succeed.
            log.warning(
                "[%s] propagate user label for event %s failed: %s",
                circuit, event_id, e,
            )

    return JSONResponse({
        "ok": True,
        "propagation": propagation_meta,
        "signatures": signature_meta,
        "classification": classification_meta,
    })


@router.post("/api/events/{circuit}/undo_auto_cycle")
async def undo_auto_cycle_api(circuit: str, request: Request):
    """Bulk-undo the auto 'cycle' labels around an anchor event — clears every
    `source='cycle'` label within ±45 min of it (the cycle), then refreshes the
    k-NN in the background. Never touches 'user'/'training'/legacy labels."""
    from datetime import timedelta
    from ..database import (clear_auto_labels, _parse_event_ts,
                            _CYCLE_PULSE_WINDOW_SECONDS)
    db = _orch(request).db
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    event_id = (payload or {}).get("event_id")
    if not event_id:
        return JSONResponse({"error": "event_id required"}, status_code=400)
    anchor = db.execute(
        "SELECT start_ts, cycle_group_id FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if anchor is None:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    gid = anchor["cycle_group_id"]
    if gid:
        # dev.24 — precise: clear by the persisted cycle-group key (the rollup
        # grouping), so exactly this cycle's auto labels are undone.
        ids = [r["id"] for r in db.execute(
            "SELECT id FROM events WHERE circuit = ? AND cycle_group_id = ? "
            "  AND fixture_label_source = 'cycle'",
            (circuit, gid),
        ).fetchall()]
    else:
        # Legacy fallback — pre-dev.24 cycle labels have no group id, so undo by
        # the original ±45-min proximity window around the anchor.
        a_ts = _parse_event_ts(anchor["start_ts"])
        if a_ts is None:
            return JSONResponse({"error": "Event has no usable timestamp"},
                                status_code=400)
        lo = (a_ts - timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
        hi = (a_ts + timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
        ids = [r["id"] for r in db.execute(
            "SELECT id FROM events WHERE circuit = ? AND fixture_label_source = 'cycle' "
            "  AND start_ts BETWEEN ? AND ?",
            (circuit, lo, hi),
        ).fetchall()]
    cleared = clear_auto_labels(db, circuit, event_ids=ids) if ids else 0
    if cleared:
        import asyncio
        asyncio.create_task(_bg_reclassify(circuit))
    return JSONResponse({"ok": True, "cleared": cleared})


@router.post("/api/events/{circuit}/{event_id}/reprocess")
async def reprocess_event_api(circuit: str, event_id: str, request: Request):
    """dev.26 — rebuild ONE event from HA history.

    Deletes this event (and any overlapping purely-machine-derived events),
    reversing their volume, then re-imports the event's own span — optionally
    padded by ``buffer_minutes`` — so a garbled/unclosed event (e.g. an irrigation
    run that failed to close and absorbed a whole day) becomes the real runs. The
    window comes from the event's authoritative ``start_ts``/``end_ts``, so the
    user never has to guess which day a long event began. User-labelled / classified
    / ignored events are preserved (``delete_events_in_range`` skips them); the
    re-import deliberately does not move the catch-up checkpoint.
    """
    from datetime import timedelta, timezone
    from ..reprocess import reprocess_window
    from ..database import _parse_event_ts
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    db = orch.db
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        buffer_min = int((payload or {}).get("buffer_minutes", 0) or 0)
    except (TypeError, ValueError):
        buffer_min = 0
    buffer_min = max(0, min(buffer_min, 240))            # clamp 0..4 h

    row = db.execute(
        "SELECT start_ts, end_ts FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    start = _parse_event_ts(row["start_ts"])
    if start is None:
        return JSONResponse({"error": "Event has no usable timestamp"},
                            status_code=400)
    end = _parse_event_ts(row["end_ts"]) or start
    # Normalise to aware UTC so the widen math (compute_widened_window) compares
    # like with like — stored strings are +00:00 but guard a stray naive value.
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    buf = timedelta(minutes=buffer_min)
    from_dt, to_dt = start - buf, end + buf

    try:
        result = await reprocess_window(orch, circuit, from_dt, to_dt)
    except Exception as e:
        log.error("[%s] reprocess_event %s failed: %s", circuit, event_id, e,
                  exc_info=True)
        return JSONResponse({"error": "Reprocess failed — see addon log."},
                            status_code=500)
    if result.get("busy"):
        return JSONResponse(
            {"error": "Another volume operation is running — try again shortly."},
            status_code=409)
    return JSONResponse({"ok": True, **result})
