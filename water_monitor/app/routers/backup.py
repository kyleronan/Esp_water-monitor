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
from typing import Dict, List

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response

from ..auth import require_admin
from ..config import DB_PATH
from ..database import get_data_retention
from ..restore_utils import (
    normalize_restore_row as _normalize_row,
    safe_insert_rows as _safe_insert,
)

log = logging.getLogger(__name__)
# Admin-only router: exports contain the entire database; imports overwrite it.
router = APIRouter(prefix="/backup", dependencies=[Depends(require_admin)])
MAX_BACKUP_BYTES = 50 * 1024 * 1024  # 50 MB hard limit

# dev34 — the /share pickup path. Browser uploads pass through Home
# Assistant's ingress proxy, which rejects large bodies before the add-on
# ever sees them — fine for the ~1 MB history archive, fatal for a years-old
# archive or a full export. Files placed here (Samba / File editor / SSH) are
# read straight from disk, so size stops mattering. Requires `map: share:rw`
# in config.yaml.
SHARE_DIR = Path("/share/water_monitor")
_SHARE_SUFFIXES = {".db", ".zip"}


# ── Table groups ─────────────────────────────────────────────────────────────

# Included in the quick-restore JSON (full rows, no date filter)
QUICK_RESTORE_TABLES = [
    "device_config", "circuit_entity_map", "home_profile",
    "circuit_profile", "learning_config", "sensitivity_config",
    "alert_config", "leak_test_schedule", "zone_schedules",
    "data_retention", "training_state", "fixtures",
    "fixture_signatures", "fixture_clusters", "cluster_cooccurrence",
    "leak_test_history", "threshold_history",
    "daily_summary", "fixture_ha_entity_map", "fixture_daily_summary",
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



# ── Export: study snapshot (dev46 46p) ────────────────────────────────────────

async def _snapshot_db(db_path) -> bytes:
    """Consistent copy of the whole DB, WITHOUT touching the shared connection.

    dev46 (46a/N3). The one-thread invariant applies to the SHARED connection,
    not to the database FILE — so the snapshot opens its own short-lived
    connection and runs on the default pool. Wrapping ``Connection.backup()``
    in ``run_db`` would be wrong twice over: it has no per-step return, so one
    call would hold the single DB worker for the whole copy INCLUDING its
    sleeps, stalling every page render behind it.

    This is the audit's sole "justified separate connection" (Verification #4,
    bucket 3): source duplicate + destination file, neither of which is the
    shared connection.
    """
    import asyncio

    def _work() -> bytes:
        import sqlite3 as _sq
        import tempfile as _tf
        src = _sq.connect(str(db_path))
        try:
            with _tf.TemporaryDirectory() as td:
                dest_path = Path(td) / "snapshot.db"
                dst = _sq.connect(str(dest_path))
                try:
                    # pages/sleep let SQLite yield between steps; WAL means a
                    # reader never blocks the writer.
                    src.backup(dst, pages=512, sleep=0.005)
                finally:
                    dst.close()
                return dest_path.read_bytes()
        finally:
            src.close()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _work)


@router.get("/export/study-snapshot", response_class=Response)
async def export_study_snapshot(request: Request):
    """dev46 (46p) — one click for the "fresh export" every study needs.

    Pulling study data by hand is the step that has actually gated the
    refill-shape, feature-space and idle-decay work, so it is worth a button.
    The payload is the whole database plus a manifest stamping schema version,
    add-on version and export time — a study that cannot say WHICH schema and
    build it was run against is not reproducible.

    Two gates, both because SQLite's backup restarts from scratch whenever
    another connection writes the source:
      * startup (R2) — the boot pass writes at every chunk boundary, and
        "export right after a restart" is exactly the workflow, so an
        ungated export could restart indefinitely;
      * an in-flight rebuild (R3) — same problem, minutes long, and the
        operator gets no explanation for the wait.
    """
    from ..config import DB_PATH
    from ..database import get_write_lock
    from ..db_migrations import _CURRENT_VERSION

    orch = _orch(request)
    if not getattr(orch, "startup_cluster_work_done", True):
        return JSONResponse(
            # dev46 (46k): pages are up by now, so "still starting up" would
            # read as a contradiction the operator can see on screen. Name the
            # thing that is actually still running.
            {"status": "starting",
             "message": "The add-on is still re-deriving event labels after "
                        "the restart — try again in a minute, once that "
                        "background pass has finished."},
            status_code=503)

    lock = get_write_lock()
    rebuilding = lock.locked()
    if not rebuilding:
        try:
            from ..database import get_incomplete_reseed, run_db
            for c in orch._cfg.circuits:
                if await run_db(get_incomplete_reseed, orch.db, c.circuit):
                    rebuilding = True
                    break
        except Exception:               # noqa: BLE001 — gate is best-effort
            pass
    if rebuilding:
        return JSONResponse(
            {"status": "busy",
             "message": "A rebuild is running — try again shortly."},
            status_code=409)

    stamp = _ts()
    snapshot = await _snapshot_db(DB_PATH)
    manifest = {
        "export_type":    "study_snapshot",
        "exported_at":    datetime.now(timezone.utc).isoformat(),
        "schema_version": _CURRENT_VERSION,
        "addon_version":  _addon_version(),
        "db_bytes":       len(snapshot),
        "note": "Whole-database snapshot for offline study work. Read-only "
                "by intent — nothing here is meant to be imported back.",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", json.dumps(manifest, indent=2))
        z.writestr("water_monitor.db", snapshot)
    log.info("study snapshot exported (%d bytes, schema %s)",
             len(snapshot), _CURRENT_VERSION)
    return _download(buf.getvalue(), f"wm_study_{stamp}.zip", "application/zip")


def _addon_version() -> str:
    """Best-effort add-on version for the manifest (same source as the boot
    log line, 46g)."""
    try:
        from ..event_detector import _read_addon_version
        return _read_addon_version() or "unknown"
    except Exception:                   # noqa: BLE001
        return "unknown"


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

    # Include circuit labels so custom display names survive a restore
    from ..database import load_circuit_labels
    circuit_labels = load_circuit_labels(db)

    payload = {
        "backup_type":  "quick_restore",
        "version":      3,
        "exported_at":  datetime.now(timezone.utc).isoformat(),
        "history_days": QUICK_RESTORE_DAYS,
        "circuits": [
            {"circuit_id": cid, "display_name": label}
            for cid, label in circuit_labels.items()
        ],
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
        import sqlite3 as _sqlite3
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as _tf:
            snap_path = Path(_tf.name)
        try:
            src_conn  = _sqlite3.connect(str(DB_PATH))
            mem_conn  = _sqlite3.connect(":memory:")
            disk_conn = _sqlite3.connect(str(snap_path))
            try:
                src_conn.backup(mem_conn)
                mem_conn.backup(disk_conn)
            finally:
                src_conn.close()
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

        from ..database import load_circuit_labels as _load_labels
        _circuit_labels = _load_labels(db)
        qr_payload = {
            "backup_type":  "quick_restore",
            "version":      3,
            "exported_at":  datetime.now(timezone.utc).isoformat(),
            "history_days": QUICK_RESTORE_DAYS,
            "circuits": [
                {"circuit_id": cid, "display_name": lbl}
                for cid, lbl in _circuit_labels.items()
            ],
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


# ── /share pickup + drop-off (dev34) ─────────────────────────────────────────

def _resolve_share_file(filename: str) -> Path:
    """Validate a user-supplied /share filename: bare basename, allowed
    suffix, and resolving inside SHARE_DIR. Raises ValueError otherwise —
    the filename crosses a trust boundary (it names a server-side path)."""
    if not filename or Path(filename).name != filename:
        raise ValueError("Filename must be a bare name, not a path.")
    if Path(filename).suffix.lower() not in _SHARE_SUFFIXES:
        raise ValueError("Only .db and .zip files can be imported.")
    p = (SHARE_DIR / filename).resolve()
    if p.parent != SHARE_DIR.resolve():
        raise ValueError("File is outside the share folder.")
    if not p.is_file():
        raise ValueError(f"Not found: {SHARE_DIR}/{filename}")
    return p


@router.get("/share-archives")
async def list_share_archives(request: Request):
    """Importable files in /share/water_monitor. `available` is False when
    the share mapping is absent (older install of the add-on config)."""
    if not SHARE_DIR.parent.exists():
        return JSONResponse({"available": False, "files": [],
                             "dir": str(SHARE_DIR)})
    SHARE_DIR.mkdir(exist_ok=True)
    files = sorted(
        ({"name": p.name, "size_mb": round(p.stat().st_size / 1048576, 1),
          "mtime": datetime.fromtimestamp(
              p.stat().st_mtime, tz=timezone.utc).isoformat()}
         for p in SHARE_DIR.iterdir()
         if p.is_file() and p.suffix.lower() in _SHARE_SUFFIXES),
        key=lambda f: f["mtime"], reverse=True)
    return JSONResponse({"available": True, "files": files,
                         "dir": str(SHARE_DIR)})


@router.post("/import/share-archive")
async def import_share_archive(
    request: Request,
    filename: str = Form(...),
    labels_only: bool = Form(False),
):
    """Merge history from a file in /share/water_monitor — the no-size-limit
    twin of the upload import, for archives the ingress proxy would reject
    (a years-old history archive, or a full export). Accepts a raw SQLite
    .db or a full-export .zip (the water_monitor.db member is used). Same
    merge semantics: existing rows kept, labels_only honoured, post-merge
    reprocess+reclassify runs."""
    orch = _orch(request)
    try:
        src = _resolve_share_file(filename)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

    tmp_path: Path = src
    extracted = None
    if src.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(src) as zf:
                member = next((n for n in zf.namelist()
                               if Path(n).name == "water_monitor.db"), None)
                if member is None:
                    return JSONResponse(
                        {"ok": False, "error": "No water_monitor.db inside "
                         "this zip — is it a Water Monitor full export?"},
                        status_code=400)
                with tempfile.NamedTemporaryFile(suffix=".db",
                                                 delete=False) as tmp:
                    with zf.open(member) as m:
                        while chunk := m.read(1 << 20):
                            tmp.write(chunk)
                    extracted = tmp_path = Path(tmp.name)
        except zipfile.BadZipFile:
            return JSONResponse({"ok": False, "error": "Not a valid zip file."},
                                status_code=400)
    try:
        log.info("Importing history from %s (labels_only=%s)", src, labels_only)
        # dev46 (46a): the whole merge — including its single `with orch.db:`
        # transaction — runs in ONE run_db callable, so no foreign statement
        # can land inside the open transaction (rule N2a).
        from ..database import run_db
        return await run_db(_merge_archive_from_path, orch, tmp_path,
                            labels_only)
    finally:
        if extracted is not None:
            extracted.unlink(missing_ok=True)


@router.post("/export/full-to-share")
async def export_full_to_share(request: Request):
    """Write the Full Export zip to /share/water_monitor instead of the
    browser — the drop-off half of the /share path, so large backups never
    transit ingress in either direction (and land where HA backups / Samba
    can pick them up)."""
    if not SHARE_DIR.parent.exists():
        return JSONResponse(
            {"ok": False, "error": "/share is not mapped into the add-on — "
             "update to a build with the share mapping and restart."},
            status_code=503)
    SHARE_DIR.mkdir(exist_ok=True)
    resp = await export_full(request)
    name = f"wm_full_export_{_ts()}.zip"
    (SHARE_DIR / name).write_bytes(resp.body)
    size_mb = round(len(resp.body) / 1048576, 1)
    log.info("Full export written to %s (%s MB)", SHARE_DIR / name, size_mb)
    return JSONResponse({"ok": True, "file": f"{SHARE_DIR}/{name}",
                         "size_mb": size_mb})


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

    db = orch.db

    def _restore_sync() -> dict:
        """dev46 (46a): the entire quick-restore — PRAGMA toggles, the single
        bulk transaction, the events normalize/dedup pass and the circuit-label
        restore — in ONE DB-thread callable. Splitting it would leave the bulk
        transaction open across a queue boundary (rule N2a); running it on the
        loop thread would put multi-second DELETE/INSERT batches on the shared
        connection while the DB worker may be mid-statement."""
        imported: dict = {}
        # PRAGMA foreign_keys must be set outside the transaction — SQLite
        # ignores it when a transaction is already open.  Disable for the bulk
        # restore so cross-table FK ordering (e.g. events → fixtures) does not
        # block the DELETE pass, then re-enable immediately after.
        db.execute("PRAGMA foreign_keys = OFF")
        # Wrap the entire restore in a single transaction.  If any table's
        # DELETE or INSERT fails, all prior DELETEs are rolled back — avoiding
        # a state where some tables are wiped but not restored.
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
        finally:
            db.execute("PRAGMA foreign_keys = ON")

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

        # Restore circuit display labels from backup, or seed defaults for old
        # backups.
        try:
            from ..database import load_circuit_labels, upsert_circuit_label
            circuit_entries = payload.get("circuits", [])
            if circuit_entries:
                for entry in circuit_entries:
                    cid   = entry.get("circuit_id", "")
                    label = entry.get("display_name", "")
                    if cid and label:
                        upsert_circuit_label(db, cid, label)
                log.info("Quick Restore: restored %d circuit label(s)",
                         len(circuit_entries))
            else:
                # Old backup without circuit metadata — seed defaults if the
                # table is empty.
                existing = load_circuit_labels(db)
                if not existing:
                    upsert_circuit_label(db, "circuit_1", "Main")
                    upsert_circuit_label(db, "circuit_2", "Irrigation")
                    log.info("Quick Restore: seeded default circuit labels "
                             "(legacy backup)")
        except Exception as e:
            log.warning("Quick Restore: circuit label restore failed "
                        "(non-fatal): %s", e)
        return imported

    from ..database import run_db
    try:
        imported = await run_db(_restore_sync)
    except Exception as e:
        log.error("Import quick-restore failed: %s", e)
        return JSONResponse({"ok": False, "error": f"Restore failed: {e}"},
                            status_code=500)

    try:
        orch.reload_circuit_entities()
    except Exception as e:
        log.warning("Import reload: %s", e)

    # Reload circuit labels into the in-memory config
    try:
        orch.reload_circuit_labels()
    except Exception as e:
        log.warning("Import reload labels: %s", e)

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
    labels_only: bool = Form(False),
):
    """Merge history rows from a SQLite archive. Existing rows are kept.

    ``labels_only`` (dev34) merges ONLY the archive's user-labelled events —
    the training fuel — instead of its full event history. Motivation: a
    fresh start discarded 486 hand-made labels, and the classifier's coverage
    (not its accuracy) collapsed afterwards. Importing the labels back roughly
    triples the pool, particularly for the starved classes.

    Rows arrive with their FEATURES INTACT. Blanking the pressure columns
    would look conservative and is the opposite: `pressure_delta_psi` is a
    LINEAR k-NN dimension, so a NULL becomes a fabricated "0 psi drop" that
    pulls every imported row into one corner of the space. Cross-regime
    distance is already handled — the rule-fit pools are windowed by
    timestamp, the active/edge k-NN tiers hard-require active-flow columns
    that firmware-3.12 rows lack (so those rows serve the legacy tier), and
    the dev32 pressure feature conditions on supply regime by design.
    """
    orch = _orch(request)
    raw = await file.read(MAX_BACKUP_BYTES + 1)
    if len(raw) > MAX_BACKUP_BYTES:
        return JSONResponse({"ok": False,
                             "error": "File too large (max 50 MB)."},
                            status_code=413)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp.write(raw)
        tmp_path = Path(tmp.name)
    try:
        # dev46 (46a): the whole merge — including its single `with orch.db:`
        # transaction — runs in ONE run_db callable, so no foreign statement
        # can land inside the open transaction (rule N2a).
        from ..database import run_db
        return await run_db(_merge_archive_from_path, orch, tmp_path,
                            labels_only)
    finally:
        tmp_path.unlink(missing_ok=True)


def _merge_archive_from_path(orch, db_path: Path,
                             labels_only: bool) -> JSONResponse:
    """The history-archive merge core, shared by the upload endpoint and the
    /share pickup (dev34): merge rows from the SQLite file at ``db_path`` into
    the live DB, then run the post-merge verdict/reclassify pass. The caller
    owns ``db_path``'s lifetime."""
    imported, errors, ignored = {}, [], {}
    arc = None

    try:
        arc = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        arc.row_factory = sqlite3.Row

        in_archive = {r[0] for r in arc.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        try:
            with orch.db:   # single transaction — rolls back all tables on any failure
                for tbl in HISTORY_ARCHIVE_TABLES:
                    if tbl not in in_archive:
                        continue
                    if labels_only and tbl != "events":
                        continue          # labels live on events only
                    if labels_only:
                        # Explicit user labels only, and never an artifact row:
                        # the goal is training fuel, not the archive's whole
                        # pre-board history (which adds noise and volume rows
                        # that would double-count against the live ledger).
                        rows = arc.execute(
                            "SELECT * FROM events "
                            "WHERE user_fixture_type IS NOT NULL "
                            "  AND user_fixture_type <> '' "
                            "  AND COALESCE(excluded_from_training, 0) = 0"
                        ).fetchall()
                    else:
                        rows = arc.execute(f"SELECT * FROM {tbl}").fetchall()
                    if not rows:
                        imported[tbl] = 0
                        continue
                    # Validate columns against live schema (defence in depth —
                    # archive could be from a different schema version)
                    valid_cols = {r[1] for r in orch.db.execute(
                        f"PRAGMA table_info({tbl})").fetchall()}
                    # Cluster linkage is a DB-LOCAL derived cache, never
                    # portable: fixture_clusters ids are small autoincrements,
                    # so an archive row's cluster_id points at a missing
                    # cluster here at best and a DIFFERENT one at worst
                    # (observed live: 272 orphaned + 11 silently joined to
                    # wrong clusters and replayed into cluster state every
                    # boot). Imported rows arrive unlinked; the post-merge
                    # backfill re-derives membership against THIS db's
                    # clusters. Features stay intact — they are measurements,
                    # not references.
                    drop = ({"cluster_id", "match_confidence", "match_level"}
                            if tbl == "events" else set())
                    cols = [c for c in rows[0].keys()
                            if c in valid_cols and c not in drop]
                    if not cols:
                        log.warning("Import archive %s: no valid columns", tbl)
                        continue
                    ph = ",".join("?" for _ in cols)
                    cn = ",".join(cols)
                    # Count before/after to get actual inserted rows — INSERT OR IGNORE
                    # silently skips duplicates so the count delta is the ground truth.
                    before = orch.db.execute(
                        f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    orch.db.executemany(
                        f"INSERT OR IGNORE INTO {tbl} ({cn}) VALUES ({ph})",
                        [
                            [_normalize_row(dict(zip(cols, [r[c] for c in cols])), tbl).get(c)
                             for c in cols]
                            for r in rows
                        ],
                    )
                    after = orch.db.execute(
                        f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    imported[tbl] = after - before
                    # INSERT OR IGNORE drops an entire row on an id collision
                    # (ids are uuid5 over circuit+start_ts, so a collision means
                    # the live DB already has that instant). "Probably zero" is
                    # a proxy — count it, and name the casualties when it isn't.
                    skipped = len(rows) - (after - before)
                    if skipped > 0:
                        ignored[tbl] = skipped
                        log.warning(
                            "Import archive %s: %d of %d row(s) skipped on id "
                            "collision (live rows kept)", tbl, skipped, len(rows))
                    # Heal-on-reimport: rows a PRE-FIX import inserted still
                    # carry the old install's cluster linkage (missing or,
                    # worse, colliding ids). Re-importing the same archive is
                    # otherwise a no-op (INSERT OR IGNORE), so use it as the
                    # repair channel: clear linkage on every archive row that
                    # already exists here. Safe — linkage is a derived cache
                    # the startup backfill/reclassify re-derives against
                    # THIS db's clusters.
                    if tbl == "events":
                        ids = [r["id"] for r in rows if "id" in r.keys()]
                        healed = 0
                        for i in range(0, len(ids), 500):
                            chunk = ids[i:i + 500]
                            healed += orch.db.execute(
                                f"UPDATE events SET cluster_id = NULL, "
                                f"  match_confidence = NULL, match_level = NULL "
                                f"WHERE cluster_id IS NOT NULL AND id IN "
                                f"({','.join('?' * len(chunk))})",
                                chunk).rowcount
                        if healed:
                            imported["cluster_links_cleared"] = healed
                            log.info(
                                "Import archive: cleared stale cluster linkage "
                                "on %d previously-imported row(s) — backfill "
                                "re-derives it locally", healed)
        except Exception as e:
            log.error("Import history-archive failed (transaction rolled back): %s", e)
            errors.append(str(e))

        # Post-merge: re-derive exclusion verdicts (phantom + low-flow dribble)
        # and backfill label-trained fixture types over the freshly-merged rows,
        # so an imported archive doesn't land inert. Runs only if the import
        # committed cleanly; best-effort, so a failure here just means the
        # derived columns lag until the next startup pass (which repeats both).
        if not errors:
            try:
                from ..feature_extractor import reprocess_event_exclusion_verdicts
                from ..database import reclassify_all_events_from_signatures
                vres = reprocess_event_exclusion_verdicts(orch.db)
                matched = cleared = 0
                for crow in orch.db.execute(
                        "SELECT DISTINCT circuit FROM events").fetchall():
                    r = reclassify_all_events_from_signatures(orch.db, crow[0])
                    matched += r["events_matched"]
                    cleared += r["events_cleared"]
                log.info(
                    "Import post-merge: flagged %d dribble(s); %d event(s) "
                    "auto-typed, %d stale match(es) cleared",
                    vres.get("dribbles_flagged", 0), matched, cleared)
            except Exception as e:
                log.warning("Import post-merge processing failed (non-fatal): %s", e)

    finally:
        if arc is not None:
            arc.close()

    total = sum(imported.values())

    summary = f"{total} rows merged from history archive"
    if labels_only:
        summary = f"{total} labelled event(s) merged from history archive"
    if ignored:
        summary += (f" ({sum(ignored.values())} skipped on id collision)")

    return JSONResponse({
        "ok":      len(errors) == 0,
        "imported": imported,
        "ignored": ignored,
        "errors":  errors,
        "summary": summary,
    })


# ── UI page ───────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def backup_page(request: Request):
    orch = _orch(request)
    db   = orch.db

    all_tables = list(dict.fromkeys(
        QUICK_RESTORE_TABLES + QUICK_RESTORE_RECENT + HISTORY_ARCHIVE_TABLES))
    from ..database import run_db
    # dev46 (46a): one COUNT(*) per table — off the loop thread.
    counts = await run_db(_row_counts, db, all_tables)

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
    # Quick restore is JSON — more verbose than binary; config rows ~350 B each,
    # event rows ~500 B each, hourly volume rows ~120 B each.
    settings_rows = sum(counts.get(t, 0) for t in QUICK_RESTORE_TABLES)
    quick_est = settings_rows * 350 + event_rows * 500 + volume_rows * 120

    def fmt(b):
        if b >= 1_048_576: return f"{b/1_048_576:.1f} MB"
        if b >= 1024:       return f"{b/1024:.1f} KB"
        return f"{b} B"

    try:
        retention = get_data_retention(db)
    except Exception:
        retention = {}

    return _tmpl(request).TemplateResponse("backup.html", {
        "request":              request,
        "page":                 "backup",
        "counts":               counts,
        "db_size":              fmt(db_size_bytes),
        "quick_restore_size_est": fmt(quick_est),
        "archive_size_est":     fmt(archive_est),
        "full_size_est":        fmt(full_est),
        "quick_tables":         QUICK_RESTORE_TABLES + QUICK_RESTORE_RECENT,
        "history_tables":       HISTORY_ARCHIVE_TABLES,
        "retention":            retention,
    })
