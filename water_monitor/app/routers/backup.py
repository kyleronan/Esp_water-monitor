"""
Backup / restore router — three-tier design.

EXPORT
  GET /backup/export/quick-restore
      JSON — settings + training + last 365 days. Small (~1-5 MB).
             Used for reinstall recovery and setup wizard restore.

  GET /backup/export/history-archive
      SQLite (.db) — events + hourly_volume, all history.
                     Compact binary. Import post-setup to restore long-term history.

  GET /backup/export/full
      ZIP — raw water_monitor.db + settings.json summary.
            Full data archive. Not designed for import.

IMPORT
  POST /backup/import/quick-restore    — restore from quick-restore JSON
  POST /backup/import/history-archive  — merge history from SQLite archive

UI
  GET /backup  — backup/restore page
"""
from __future__ import annotations

import io
import json
import logging
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response

from ._helpers import ingress_redirect
from ..config import DB_PATH

log = logging.getLogger(__name__)
router = APIRouter(prefix="/backup")
MAX_BACKUP_BYTES = 50 * 1024 * 1024  # 50 MB hard limit

def _safe_insert(db, tbl: str, rows: list) -> int:
    """
    Insert rows into tbl, validating column names against the live schema
    to prevent SQL injection via crafted backup files.
    Returns count of rows inserted.
    """
    if not rows:
        return 0
    # Fetch the actual columns that exist in this table
    valid_cols = {r[1] for r in db.execute(f"PRAGMA table_info({tbl})").fetchall()}
    # Filter to only columns that exist in both the payload and the table
    cols = [c for c in rows[0].keys() if c in valid_cols]
    if not cols:
        raise ValueError(f"No valid columns found for table {tbl}")
    ph = ",".join("?" for _ in cols)
    cn = ",".join(cols)
    sql = f"INSERT OR REPLACE INTO {tbl} ({cn}) VALUES ({ph})"
    for row in rows:
        db.execute(sql, [row.get(c) for c in cols])
    return len(rows)


# ── Table groups ─────────────────────────────────────────────────────────────

# Included in the quick-restore JSON (full rows, no date filter)
QUICK_RESTORE_TABLES = [
    "device_config", "circuit_entity_map", "home_profile",
    "circuit_profile", "learning_config", "sensitivity_config",
    "alert_config", "leak_test_schedule", "zone_schedules",
    "data_retention", "training_state", "fixtures",
    "fixture_signatures", "fixture_clusters", "cluster_cooccurrence",
    "leak_test_history", "threshold_history",
    "daily_summary",
]

# events + hourly_volume included with 90-day filter in quick-restore
QUICK_RESTORE_RECENT = ["events", "hourly_volume"]
QUICK_RESTORE_DAYS   = 365

# History archive SQLite tables
HISTORY_ARCHIVE_TABLES = ["events", "hourly_volume",
                          "zone_flow_history", "leak_test_history"]


def _orch(r): return r.app.state.orchestrator
def _tmpl(r): return r.app.state.templates
def _ts():    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _download(content: bytes, filename: str, media_type: str) -> Response:
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _row_counts(db, tables: List[str]) -> Dict[str, int]:
    out = {}
    for tbl in tables:
        try:
            out[tbl] = db.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            out[tbl] = 0
    return out


# ── Export: Quick Restore ─────────────────────────────────────────────────────

@router.get("/export/quick-restore", response_class=Response)
async def export_quick_restore(request: Request):
    db     = _orch(request).db
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=QUICK_RESTORE_DAYS)).isoformat()
    tables = {}

    for tbl in QUICK_RESTORE_TABLES:
        try:
            tables[tbl] = [dict(r)
                           for r in db.execute(f"SELECT * FROM {tbl}").fetchall()]
        except Exception as e:
            log.warning("Quick-restore export %s: %s", tbl, e)
            tables[tbl] = []

    for tbl, col in [("events", "start_ts"), ("hourly_volume", "hour_ts")]:
        try:
            # ORDER BY rowid ASC so that on restore the last-inserted (newest)
            # row appears last in the JSON array.  With INSERT OR REPLACE the
            # last row for each (circuit, start_ts) wins — which is what we want.
            tables[tbl] = [dict(r) for r in db.execute(
                f"SELECT * FROM {tbl} WHERE {col} >= ? ORDER BY rowid ASC",
                (cutoff,)).fetchall()]
        except Exception as e:
            log.warning("Quick-restore export %s: %s", tbl, e)
            tables[tbl] = []

    payload = {
        "backup_type":  "quick_restore",
        "version":      3,
        "exported_at":  datetime.now(timezone.utc).isoformat(),
        "history_days": QUICK_RESTORE_DAYS,
        "tables":       tables,
    }
    return _download(
        json.dumps(payload, indent=2, default=str).encode(),
        f"wm_quick_restore_{_ts()}.json",
        "application/json",
    )


# ── Export: History Archive (SQLite) ─────────────────────────────────────────

@router.get("/export/history-archive", response_class=Response)
async def export_history_archive(request: Request):
    db = _orch(request).db

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        arc = sqlite3.connect(str(tmp_path))
        arc.row_factory = sqlite3.Row

        for tbl in HISTORY_ARCHIVE_TABLES:
            try:
                schema = db.execute(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type='table' AND name=?", (tbl,)).fetchone()
                if not schema or not schema[0]:
                    continue
                arc.execute(schema[0])
                rows = db.execute(f"SELECT * FROM {tbl}").fetchall()
                if rows:
                    cols = rows[0].keys()
                    arc.executemany(
                        f"INSERT INTO {tbl} ({','.join(cols)}) "
                        f"VALUES ({','.join('?' for _ in cols)})",
                        [list(r) for r in rows],
                    )
            except Exception as e:
                log.warning("History archive %s: %s", tbl, e)

        arc.execute("""CREATE TABLE IF NOT EXISTS _archive_meta
                       (key TEXT PRIMARY KEY, value TEXT)""")
        for k, v in {
            "backup_type": "history_archive",
            "version":     "3",
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }.items():
            arc.execute("INSERT OR REPLACE INTO _archive_meta VALUES (?,?)", (k, v))

        arc.commit()
        arc.close()
        content = tmp_path.read_bytes()
    finally:
        tmp_path.unlink(missing_ok=True)

    return _download(content, f"wm_history_archive_{_ts()}.db",
                     "application/octet-stream")


# ── Export: Full ZIP ──────────────────────────────────────────────────────────

@router.get("/export/full", response_class=Response)
async def export_full(request: Request):
    db  = _orch(request).db
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:

        # Consistent SQLite snapshot using the backup API.
        # This works even while the DB is being written to — no torn reads.
        import io as _io
        import sqlite3 as _sqlite3
        snap_path = Path(tempfile.mktemp(suffix=".db"))
        try:
            src_conn  = _sqlite3.connect(str(DB_PATH))
            mem_conn  = _sqlite3.connect(":memory:")
            src_conn.backup(mem_conn)
            src_conn.close()
            disk_conn = _sqlite3.connect(str(snap_path))
            mem_conn.backup(disk_conn)
            mem_conn.close()
            disk_conn.close()
            zf.write(str(snap_path), "water_monitor.db")
        finally:
            snap_path.unlink(missing_ok=True)

        # Quick Restore JSON — included so the ZIP is self-contained for reinstall
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=QUICK_RESTORE_DAYS)).isoformat()
        qr_tables = {}
        for tbl in QUICK_RESTORE_TABLES:
            try:
                qr_tables[tbl] = [dict(r)
                                   for r in db.execute(f"SELECT * FROM {tbl}").fetchall()]
            except Exception as e:
                log.warning("Full export quick-restore table %s: %s", tbl, e)
                qr_tables[tbl] = []
        for tbl, col in [("events", "start_ts"), ("hourly_volume", "hour_ts")]:
            try:
                qr_tables[tbl] = [dict(r) for r in db.execute(
                    f"SELECT * FROM {tbl} WHERE {col} >= ?"
                    f" ORDER BY rowid ASC", (cutoff,)).fetchall()]
            except Exception as e:
                log.warning("Full export quick-restore %s: %s", tbl, e)
                qr_tables[tbl] = []

        qr_payload = {
            "backup_type":  "quick_restore",
            "version":      3,
            "exported_at":  datetime.now(timezone.utc).isoformat(),
            "history_days": QUICK_RESTORE_DAYS,
            "tables":       qr_tables,
        }
        zf.writestr("quick_restore.json",
                    json.dumps(qr_payload, indent=2, default=str))

        # Human-readable settings summary
        settings = {}
        for tbl in ["device_config", "circuit_entity_map", "home_profile",
                    "sensitivity_config", "alert_config", "leak_test_schedule",
                    "training_state", "data_retention"]:
            try:
                settings[tbl] = [dict(r)
                                  for r in db.execute(f"SELECT * FROM {tbl}").fetchall()]
            except Exception:
                pass

        zf.writestr("settings.json", json.dumps({
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "note": ("Human-readable reference only. "
                     "To restore, replace water_monitor.db directly."),
            "tables": settings,
        }, indent=2, default=str))

        zf.writestr("README.txt", (
            "Water Monitor — Full Data Export\n"
            "=================================\n\n"
            "Contents:\n"
            "  water_monitor.db     complete SQLite database\n"
            "  quick_restore.json   Quick Restore backup (use this for reinstall)\n"
            "  settings.json        human-readable settings summary\n\n"
            "To restore after reinstall (easiest):\n"
            "  1. Open the Water Monitor addon setup wizard\n"
            "  2. Choose 'Restore from backup'\n"
            "  3. Upload quick_restore.json\n\n"
            "To restore via raw database (advanced):\n"
            "  1. Stop the Water Monitor addon\n"
            "  2. Copy water_monitor.db to /addon_data/water_monitor/ via SSH or Samba\n"
            "  3. Start the addon — setup wizard is skipped automatically\n"
        ))

    return _download(buf.getvalue(), f"wm_full_export_{_ts()}.zip",
                     "application/zip")


# ── Import: Quick Restore JSON ────────────────────────────────────────────────

@router.post("/import/quick-restore")
async def import_quick_restore(
    request: Request,
    file: UploadFile = File(...),
    import_settings: str = Form(default=""),
    import_history:  str = Form(default=""),
):
    orch = _orch(request)

    try:
        raw = await file.read(MAX_BACKUP_BYTES + 1)
        if len(raw) > MAX_BACKUP_BYTES:
            return JSONResponse({"ok": False,
                                 "error": "File too large (max 50 MB)."},
                                status_code=413)
        payload = json.loads(raw)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Invalid JSON: {e}"},
                            status_code=400)

    tables = payload.get("tables", {})
    if not tables:
        return JSONResponse({"ok": False, "error": "No table data in backup."},
                            status_code=400)

    restore = []
    if import_settings == "1":
        restore += QUICK_RESTORE_TABLES
    if import_history == "1":
        restore += QUICK_RESTORE_RECENT

    if not restore:
        return JSONResponse({"ok": False,
                             "error": "Select at least one group."},
                            status_code=400)

    imported = {}
    db = orch.db

    # Wrap the entire restore in a single transaction.  If any table's DELETE
    # or INSERT fails, all prior DELETEs are rolled back — avoiding a state
    # where some tables are wiped but not restored.
    #
    # Every table in the restore list is cleared unconditionally, even when
    # the backup has an empty array or the table is absent from the backup
    # entirely.  This ensures the DB reflects the exact state of the backup
    # — stale rows from a previous restore cannot bleed through.
    try:
        with db:
            for tbl in restore:
                db.execute(f"DELETE FROM {tbl}")
                rows = tables.get(tbl)
                if rows:
                    imported[tbl] = _safe_insert(db, tbl, rows)
                else:
                    imported[tbl] = 0
    except Exception as e:
        log.error("Import quick-restore failed: %s", e)
        return JSONResponse({"ok": False, "error": f"Restore failed: {e}"},
                            status_code=500)

    # After events are imported, normalize timestamps to UTC then dedup.
    # Order matters: normalize first so rows with the same logical instant
    # but different offset strings (+00:00 vs -06:00) collapse correctly.
    if "events" in restore:
        try:
            from ..database import normalize_events_utc, dedup_events
            normalize_events_utc(db)
            removed = dedup_events(db)
            if removed:
                log.warning(
                    "Quick Restore: removed %d duplicate event(s) from backup",
                    removed)
        except Exception as e:
            log.warning("Quick Restore dedup failed (non-fatal): %s", e)
    try:
        orch.reload_circuit_entities()
    except Exception as e:
        log.warning("Import reload: %s", e)

    total = sum(imported.values())
    log.info(
        "Quick Restore complete — %d rows imported: %s",
        total,
        ", ".join(f"{t}={n}" for t, n in imported.items()),
    )
    return JSONResponse({
        "ok":      True,
        "imported": imported,
        "errors":  [],
        "summary": f"{total} rows restored",
    })


# ── Import: History Archive (SQLite) ──────────────────────────────────────────

@router.post("/import/history-archive")
async def import_history_archive(
    request: Request,
    file: UploadFile = File(...),
):
    """Merge history rows from a SQLite archive. Existing rows are kept."""
    orch = _orch(request)
    raw = await file.read(MAX_BACKUP_BYTES + 1)
    if len(raw) > MAX_BACKUP_BYTES:
        return JSONResponse({"ok": False,
                             "error": "File too large (max 50 MB)."},
                            status_code=413)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp.write(raw)
        tmp_path = Path(tmp.name)

    imported, errors = {}, []

    try:
        arc = sqlite3.connect(str(tmp_path))
        arc.row_factory = sqlite3.Row

        in_archive = {r[0] for r in arc.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        for tbl in HISTORY_ARCHIVE_TABLES:
            if tbl not in in_archive:
                continue
            try:
                rows = arc.execute(f"SELECT * FROM {tbl}").fetchall()
                if not rows:
                    imported[tbl] = 0
                    continue
                # Validate columns against live schema (defence in depth —
                # archive could be from a different schema version)
                valid_cols = {r[1] for r in orch.db.execute(
                    f"PRAGMA table_info({tbl})").fetchall()}
                cols = [c for c in rows[0].keys() if c in valid_cols]
                if not cols:
                    log.warning("Import archive %s: no valid columns", tbl)
                    continue
                ph = ",".join("?" for _ in cols)
                cn = ",".join(cols)
                orch.db.executemany(
                    f"INSERT OR IGNORE INTO {tbl} ({cn}) VALUES ({ph})",
                    [[r[c] for c in cols] for r in rows],
                )
                imported[tbl] = len(rows)
            except Exception as e:
                log.error("Import history-archive %s: %s", tbl, e)
                errors.append(f"{tbl}: {e}")

        arc.close()
    finally:
        tmp_path.unlink(missing_ok=True)

    orch.db.commit()
    total = sum(imported.values())

    return JSONResponse({
        "ok":      len(errors) == 0,
        "imported": imported,
        "errors":  errors,
        "summary": f"{total} rows merged from history archive",
    })


# ── UI page ───────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def backup_page(request: Request):
    orch = _orch(request)
    db   = orch.db

    all_tables = list(dict.fromkeys(
        QUICK_RESTORE_TABLES + QUICK_RESTORE_RECENT + HISTORY_ARCHIVE_TABLES))
    counts = _row_counts(db, all_tables)

    try:
        db_size_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    except Exception:
        db_size_bytes = 0

    # Estimate history archive size (events + hourly_volume row counts × avg row size)
    event_rows  = counts.get("events", 0)
    volume_rows = counts.get("hourly_volume", 0)
    archive_est = event_rows * 200 + volume_rows * 50   # bytes
    # Full ZIP is roughly the SQLite file size (compressed)
    full_est    = int(db_size_bytes * 0.6)

    def fmt(b):
        if b >= 1_048_576: return f"{b/1_048_576:.1f} MB"
        if b >= 1024:       return f"{b/1024:.1f} KB"
        return f"{b} B"

    return _tmpl(request).TemplateResponse("backup.html", {
        "request":          request,
        "page":             "backup",
        "counts":           counts,
        "db_size":          fmt(db_size_bytes),
        "archive_size_est": fmt(archive_est),
        "full_size_est":    fmt(full_est),
        "quick_tables":     QUICK_RESTORE_TABLES + QUICK_RESTORE_RECENT,
        "history_tables":   HISTORY_ARCHIVE_TABLES,
    })
