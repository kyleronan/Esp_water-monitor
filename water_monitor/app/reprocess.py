"""dev.26 — shared reprocess-window orchestration.

A "reprocess" deletes a circuit's purely-machine-derived events overlapping a time
window (reversing their volume) and re-imports that window from HA flow history, so
a garbled stored event — e.g. an irrigation run that failed to close and absorbed a
whole day — is rebuilt as the real runs. Two UIs drive it through the SAME core:

  * the History event modal (window = the clicked event's own span ± a buffer), and
  * the Settings → Dev tools date tool (window = a local calendar range).

Keeping the delete + auto-widen + import logic here (not duplicated in each router)
guarantees both paths behave identically. Reuses ``delete_events_in_range`` (the
overlap-aware, volume-reversing, label-preserving delete),
``historical_importer.import_range`` (HA reconstruction), and the
``run_isolated_write`` / ``get_write_lock`` admin-write serialisation.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from .config import DB_PATH
from .database import delete_events_in_range, get_write_lock, run_isolated_write

log = logging.getLogger(__name__)


def _parse_utc(value: str) -> datetime:
    """Parse a stored ISO timestamp to an aware UTC datetime (assume UTC if naive)."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def compute_widened_window(
    from_dt: datetime,
    to_dt: datetime,
    span_start: Optional[str],
    span_end: Optional[str],
) -> Tuple[datetime, datetime, bool]:
    """Widen ``[from_dt, to_dt]`` to engulf the full span of whatever was deleted.

    ``span_start`` / ``span_end`` are the ISO bounds reported by
    ``delete_events_in_range`` (or ``None`` when nothing was deleted). A deleted
    event that extends *outside* the picked window (the 27.6 h case that started the
    day before) must be re-imported across its whole span — otherwise the part
    outside the window would be lost. Returns ``(imp_from, imp_to, widened)``.
    """
    imp_from, imp_to = from_dt, to_dt
    if span_start:
        s = _parse_utc(span_start)
        if s < imp_from:
            imp_from = s
    if span_end:
        e = _parse_utc(span_end)
        if e > imp_to:
            imp_to = e
    widened = imp_from != from_dt or imp_to != to_dt
    return imp_from, imp_to, widened


async def reprocess_window(
    orch: Any, circuit: str, from_dt: datetime, to_dt: datetime,
) -> Dict[str, Any]:
    """Delete ``circuit``'s machine events overlapping ``[from_dt, to_dt]`` and
    re-import the (auto-widened) span from HA history.

    Returns ``{"deleted", "imported", "widened", "from", "to"}``, or
    ``{"busy": True}`` when another admin write is already running. Deliberately
    does NOT call ``update_import_state`` — re-importing a past range must never
    move the catch-up checkpoint backward.
    """
    importer = getattr(orch, "historical_importer", None)
    if importer is None:
        raise RuntimeError("historical importer unavailable")
    # Fast-fail for UX: another recompute/reclassify/reprocess is mid-flight.
    if get_write_lock().locked():
        return {"busy": True}

    from_iso, to_iso = from_dt.isoformat(), to_dt.isoformat()
    # 1) Delete the window's machine events under the write lock (sync, isolated).
    res = await run_isolated_write(
        DB_PATH, lambda c: delete_events_in_range(c, circuit, from_iso, to_iso))
    # 2) Widen the import to the true deleted span, then reconstruct from HA. The
    #    reconstructed events queue onto the live pipeline; the FeatureExtractor
    #    worker stores + classifies them.
    imp_from, imp_to, widened = compute_widened_window(
        from_dt, to_dt, res["span_start"], res["span_end"])
    imported = await importer.import_range(circuit, imp_from, imp_to)
    log.info(
        "[%s] reprocess %s..%s (widened=%s → %s..%s): deleted %d, re-imported %d",
        circuit, from_iso, to_iso, widened,
        imp_from.isoformat(), imp_to.isoformat(), res["deleted"], imported,
    )
    return {
        "deleted": res["deleted"],
        "imported": imported,
        "widened": widened,
        "from": imp_from.isoformat(),
        "to": imp_to.isoformat(),
    }
