"""Backup / restore router — three tiers.

Quick-restore JSON: QUICK_RESTORE_TABLES plus the last QUICK_RESTORE_DAYS of
events / hourly_volume (~1-5 MB); reinstall recovery and the setup wizard's
restore. History archive (.db): HISTORY_ARCHIVE_TABLES, all history; imported
post-setup as a merge that keeps existing rows. Full ZIP: the scrubbed raw
database plus a settings summary; an archive, not designed for import. Large
files bypass the ingress body limit through /share/water_monitor, both ways.
"""
from __future__ import annotations

import io
import json
import logging
import re
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response

from ..auth import require_admin
from ..config import DATA_DIR, DB_PATH
from ..database import (
    dedup_events,
    get_data_retention,
    get_incomplete_reseed,
    get_write_lock,
    load_circuit_labels,
    load_circuit_labels as _load_labels,
    normalize_events_utc,
    run_db)
from ..restore_utils import (
    normalize_restore_row as _normalize_row,
    restore_circuit_labels,
    safe_insert_rows as _safe_insert,
)
from ._helpers import _orch, _tmpl

log = logging.getLogger(__name__)
# Admin-only router: exports contain the entire database; imports overwrite it.
router = APIRouter(prefix="/backup", dependencies=[Depends(require_admin)])
MAX_BACKUP_BYTES = 50 * 1024 * 1024  # 50 MB hard limit

# The /share pickup path. Browser uploads pass through Home Assistant's
# ingress proxy, which rejects large bodies before the add-on ever sees them —
# fine for the ~1 MB history archive, fatal for a years-old archive or a full
# export. Files placed here (Samba / File editor / SSH) are read straight from
# disk, so size stops mattering. Requires `map: share:rw` in config.yaml.
SHARE_DIR = Path("/share/water_monitor")
_SHARE_SUFFIXES = {".db", ".zip"}

# ── /share import hardening (2.17) ──────────────────────────────────────────
# /share is writable by every add-on holding `share:rw`, so the zip is
# attacker-supplied even though the endpoint is admin-only. Reading the
# COMPRESSED input off disk sidesteps the ingress size limit; the DECOMPRESSED
# output needs a bound of its own.
MAX_EXTRACTED_BYTES = 2 * 1024 * 1024 * 1024   # 2 GB of decompressed database
# A single member expanding more than this is the classic bomb signature and is
# cheap to reject early. It is NOT the control: Fifield's non-recursive bomb
# builds a huge archive out of members that each sit under any sane ratio, so
# the ABSOLUTE streamed cap above is what actually holds. This only produces a
# better error message, sooner.
MAX_COMPRESSION_RATIO = 100

# Where the zip member is unpacked. NOT the system temp dir: on Home Assistant
# OS a container's /tmp can be tmpfs, i.e. RAM, so an unbounded (or merely
# large) extraction there wedges the container — and this container is the one
# that drives the main water valve. /data is the add-on's real disk.
def _extract_dir() -> Path:
    d = Path(DATA_DIR) / "import_tmp"
    d.mkdir(parents=True, exist_ok=True)
    # Moving off /tmp means nothing reclaims an orphan on reboot any more, and
    # an import killed mid-stream (restart, OOM) leaves a whole database
    # behind. Sweep anything older than a day: no import runs that long, and
    # /data is the same disk the live DB needs.
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    for stale in d.iterdir():
        try:
            if stale.is_file() and stale.stat().st_mtime < cutoff:
                stale.unlink()
        except OSError:                 # best-effort housekeeping only
            pass
    return d


# A /share filename is attacker-chosen and becomes part of a SQLite path. `?`
# and `#` are legal filename characters, so `x?mode=rwc&.db` passes a
# bare-basename + suffix check and, opened as a URI, becomes a READ-WRITE open
# of a DIFFERENT file (`x`): SQLite stops at the first `?` and honours the
# attacker's `mode`. _merge_archive_from_path drops URI mode (braces); this
# allowlist is the belt, and keeps `%`, newlines and quoting out of the path.
_SHARE_NAME_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")

# What the full export must NOT carry off the add-on's disk: /share is readable
# by every add-on that maps it, so exporting there publishes the file.
#   csrf_server_secret — the HMAC key behind every CSRF token; regenerated on
#     first use after a restore, so exclude rather than encrypt (encrypting
#     would only move the key-custody problem).
#   seen_users.display_name — HA account names from the first-sight upsert;
#     nothing restores from them (RBAC keys on user_id), so pure disclosure.
EXPORT_EXCLUDED_TABLES = ("csrf_server_secret", "csrf_tokens")
# operator_users.display_name is deliberately KEPT: it is the label an admin
# attached to a grant, and a restored install that lost it shows a bare user id
# on the Access page. admin_ids_cache is re-derived from HA on every role sync,
# so its names carry nothing a restore needs.
EXPORT_SCRUBBED_COLUMNS = (("seen_users", "display_name"),
                           ("admin_ids_cache", "display_name"))

# QUICK_RESTORE_TABLES is not just "settings" — see the invariant note on the
# list itself. Named here so the guarding test and the restore UI use the same
# words.
RETARGETING_RESTORE_TABLES = ("device_config", "circuit_entity_map",
                              "leak_test_schedule")


# ── Table groups ─────────────────────────────────────────────────────────────

# Included in the quick-restore JSON (full rows, no date filter).
#
# INVARIANT — this list is PRIVILEGED, not merely "settings": device_config /
# circuit_entity_map decide WHICH HA entities the add-on reads and drives
# (valve switches included — a restore can re-point valve control), and
# leak_test_schedule decides WHEN it closes the main valve by itself. Importing
# a quick-restore file is therefore a control-plane change. Defensible (admin-
# only, behind an explicit checkbox, and a restore that could not re-point
# entities would be useless after a rebuild) but it must stay a DECISION:
# adding a table with that reach means updating RETARGETING_RESTORE_TABLES
# and its test, deliberately.
QUICK_RESTORE_TABLES = [
    "device_config", "circuit_entity_map", "home_profile",
    "circuit_profile", "learning_config", "sensitivity_config",
    "alert_config", "leak_test_schedule", "zone_schedules",
    "data_retention", "training_state", "fixtures",
    "fixture_signatures", "fixture_clusters", "cluster_cooccurrence",
    "leak_test_history", "threshold_history",
    "daily_summary", "fixture_daily_summary",
]

# events + hourly_volume included with 90-day filter in quick-restore
QUICK_RESTORE_RECENT = ["events", "hourly_volume"]
QUICK_RESTORE_DAYS   = 365

# History archive SQLite tables
HISTORY_ARCHIVE_TABLES = ["events", "hourly_volume",
                          "zone_flow_history", "leak_test_history"]


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



# ── Export: study snapshot ────────────────────────────────────────────────────

def scrub_export_copy(conn: sqlite3.Connection) -> Dict[str, int]:
    """Strip the add-on's own secrets from a SNAPSHOT (never the live DB).

    Why these tables and columns: see EXPORT_EXCLUDED_TABLES. The caller MUST
    ``VACUUM`` afterwards: a DELETE only moves pages onto the freelist, and a
    freelist page in a shipped .db is trivially recoverable — without the
    rewrite the scrub is cosmetic.
    """
    removed: Dict[str, int] = {}
    for tbl in EXPORT_EXCLUDED_TABLES:
        try:
            n = conn.execute(f"DELETE FROM {tbl}").rowcount
        except sqlite3.Error:
            continue                      # table absent in this schema version
        if n and n > 0:
            removed[tbl] = n
    for tbl, col in EXPORT_SCRUBBED_COLUMNS:
        try:
            n = conn.execute(
                f"UPDATE {tbl} SET {col} = NULL WHERE {col} IS NOT NULL"
            ).rowcount
        except sqlite3.Error:
            continue
        if n and n > 0:
            removed[f"{tbl}.{col}"] = n
    return removed


def _sanitized_snapshot(dest: Path) -> Dict[str, int]:
    """Write a scrubbed, consistent copy of the live DB to ``dest``.

    ``VACUUM INTO`` gives a consistent copy in one statement without touching
    the shared connection; the backup API is the fallback for a SQLite older
    than 3.27. Either way the scrub, and the rewrite that makes it real, run
    on the COPY — the live database is never written to by an export.
    """
    src = sqlite3.connect(str(DB_PATH))
    try:
        try:
            src.execute("VACUUM INTO ?", (str(dest),))
        except sqlite3.DatabaseError:
            dst = sqlite3.connect(str(dest))
            try:
                src.backup(dst)
            finally:
                dst.close()
    finally:
        src.close()

    conn = sqlite3.connect(str(dest))
    try:
        removed = scrub_export_copy(conn)
        conn.commit()
        conn.execute("VACUUM")            # see scrub_export_copy's contract
    finally:
        conn.close()
    if removed:
        log.info("Export scrub: %s",
                 ", ".join(f"{k}={v}" for k, v in removed.items()))
    return removed


async def _snapshot_db(db_path) -> bytes:
    """Consistent copy of the whole DB, WITHOUT touching the shared connection.

    The one-thread invariant applies to the SHARED connection, not to the
    database FILE — so the snapshot opens its own short-lived connection and
    runs on the default pool. Wrapping ``Connection.backup()`` in ``run_db``
    would be wrong: it has no per-step return, so one call would hold the
    single DB worker for the whole copy INCLUDING its sleeps, stalling every
    page render behind it.
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
                # A study snapshot gets copied to a laptop and passed around.
                # It is read-only by intent and has no use for the CSRF key or
                # for HA account names, so it leaves without them. Same helper,
                # same VACUUM contract.
                scrub = _sq.connect(str(dest_path))
                try:
                    scrub_export_copy(scrub)
                    scrub.commit()
                    scrub.execute("VACUUM")
                finally:
                    scrub.close()
                return dest_path.read_bytes()
        finally:
            src.close()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _work)


@router.get("/export/study-snapshot", response_class=Response)
async def export_study_snapshot(request: Request):
    """One click for the "fresh export" every study needs: the whole database
    plus a manifest stamping schema version, add-on version and export time —
    a study that cannot say which build it ran against is not reproducible.

    Two gates, both because SQLite's backup restarts from scratch whenever
    another connection writes the source: the boot pass (writes at every chunk
    boundary, and "export right after a restart" is exactly the workflow, so an
    ungated copy could restart indefinitely) and an in-flight rebuild (minutes
    long, and the operator would get no explanation for the wait).
    """
    from ..db_migrations import _CURRENT_VERSION

    orch = _orch(request)
    if not getattr(orch, "startup_cluster_work_done", True):
        return JSONResponse(
            # Pages are up by now, so "still starting up" would read as a
            # contradiction the operator can see on screen. Name the thing that
            # is actually still running.
            {"status": "starting",
             "message": "The add-on is still re-deriving event labels after "
                        "the restart — try again in a minute, once that "
                        "background pass has finished."},
            status_code=503)

    lock = get_write_lock()
    rebuilding = lock.locked()
    if not rebuilding:
        try:
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
    log line)."""
    try:
        from ..event_detector_core import _read_addon_version
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

        # Consistent SQLite snapshot, scrubbed of the add-on's own secrets —
        # this zip can be written straight into /share, which every add-on with
        # `share:rw` can read. VACUUM INTO needs a destination that does not
        # exist yet, so a private directory rather than mkstemp.
        with tempfile.TemporaryDirectory() as _td:
            snap_path = Path(_td) / "water_monitor.db"
            _sanitized_snapshot(snap_path)
            zf.write(str(snap_path), "water_monitor.db")

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


# ── /share pickup + drop-off ─────────────────────────────────────────────────

def _resolve_share_file(filename: str) -> Path:
    """Validate a user-supplied /share filename: bare basename, allowed
    suffix, and resolving inside SHARE_DIR. Raises ValueError otherwise —
    the filename crosses a trust boundary (it names a server-side path)."""
    if not filename or Path(filename).name != filename:
        raise ValueError("Filename must be a bare name, not a path.")
    # Allowlist, not a denylist. Anything outside [A-Za-z0-9._-] is rejected
    # before the name can be concatenated into a path or a SQLite URI. See
    # _SHARE_NAME_RE for the `x?mode=rwc&.db` case this stops.
    if not _SHARE_NAME_RE.fullmatch(filename):
        raise ValueError(
            "Filename may only contain letters, digits, dot, dash and "
            "underscore. Rename the file in /share and try again.")
    if Path(filename).suffix.lower() not in _SHARE_SUFFIXES:
        raise ValueError("Only .db and .zip files can be imported.")
    p = (SHARE_DIR / filename).resolve()
    if p.parent != SHARE_DIR.resolve():
        raise ValueError("File is outside the share folder.")
    if not p.is_file():
        raise ValueError(f"Not found: {SHARE_DIR}/{filename}")
    return p


class _ArchiveRejected(ValueError):
    """A supplied archive/database is not usable — answer 400, not 500."""


def _precheck_member(info: zipfile.ZipInfo) -> None:
    """Cheap metadata rejects, before a single byte is decompressed.

    Both figures here come from the archive's central directory, i.e. from
    whoever wrote the file. They buy a fast, well-worded refusal and nothing
    more; the enforcement is the running byte counter in _extract_db_member.
    Kept as a separate function so a test can neuter it and prove the counter
    stands on its own.
    """
    if info.file_size > MAX_EXTRACTED_BYTES:
        raise _ArchiveRejected(
            f"The database inside this zip declares "
            f"{info.file_size / 1048576:.0f} MB, over the "
            f"{MAX_EXTRACTED_BYTES // 1048576} MB import limit.")
    if (info.compress_size > 0
            and info.file_size / info.compress_size > MAX_COMPRESSION_RATIO):
        raise _ArchiveRejected(
            "This zip expands more than "
            f"{MAX_COMPRESSION_RATIO}x — refusing to unpack it.")


def _extract_db_member(src: Path) -> Path:
    """Stream `water_monitor.db` out of a /share zip under a hard byte cap.

    The cap is a running counter on decompressed bytes as ``ZipExtFile.read``
    hands them back; ``ZipInfo.file_size`` is author-chosen metadata, so it is
    only a pre-filter (``_precheck_member``). CPython's ``ZipExtFile`` happens
    to clamp output to the declared size and then fail the CRC, so an
    UNDERSTATED size cannot overrun there — an implementation detail, not a
    format guarantee; the counter is what this code depends on.

    Zip-slip is deliberately not checked for — no member name reaches the
    filesystem: the name is only compared against the literal
    "water_monitor.db", and the output path is a ``mkstemp`` name we generate.
    """
    with zipfile.ZipFile(src) as zf:
        info = next((i for i in zf.infolist()
                     if Path(i.filename).name == "water_monitor.db"), None)
        if info is None:
            raise _ArchiveRejected(
                "No water_monitor.db inside this zip — is it a Water Monitor "
                "full export?")
        _precheck_member(info)

        fd, tmp_name = tempfile.mkstemp(dir=str(_extract_dir()), suffix=".db")
        tmp_path = Path(tmp_name)
        written = 0
        try:
            with open(fd, "wb") as out, zf.open(info) as member:
                while chunk := member.read(1 << 20):
                    written += len(chunk)
                    if written > MAX_EXTRACTED_BYTES:
                        raise _ArchiveRejected(
                            "The database inside this zip is larger than the "
                            f"{MAX_EXTRACTED_BYTES // 1048576} MB import "
                            "limit (it kept expanding past its declared "
                            "size) — refusing to unpack it.")
                    out.write(chunk)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    return tmp_path


def _validate_sqlite_file(path: Path) -> None:
    """Reject a non-database before anything opens it as one.

    Without this a text file (or a truncated download) surfaces as a
    ``DatabaseError`` from somewhere deep in the merge — a 500 and a stack
    trace where the honest answer is "that is not a Water Monitor archive".
    """
    try:
        with open(path, "rb") as fh:
            magic = fh.read(16)
    except OSError as exc:
        raise _ArchiveRejected(f"Could not read the archive: {exc}") from exc
    if magic != b"SQLite format 3\x00":
        raise _ArchiveRejected(
            "That file is not a SQLite database (wrong file header).")
    probe = sqlite3.connect(str(path))
    try:
        probe.execute("PRAGMA query_only = ON")
        row = probe.execute("PRAGMA quick_check(1)").fetchone()
    except sqlite3.DatabaseError as exc:
        raise _ArchiveRejected(
            f"That database could not be opened: {exc}") from exc
    finally:
        probe.close()
    if not row or str(row[0]).lower() != "ok":
        raise _ArchiveRejected(
            "That database failed SQLite's integrity check — it looks "
            "truncated or corrupt.")


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
    twin of the upload import, for archives the ingress proxy would reject.
    Accepts a raw SQLite
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
            extracted = tmp_path = _extract_db_member(src)
        except zipfile.BadZipFile:
            return JSONResponse({"ok": False, "error": "Not a valid zip file."},
                                status_code=400)
        except _ArchiveRejected as e:
            log.warning("Rejected /share zip %s: %s", src, e)
            return JSONResponse({"ok": False, "error": str(e)},
                                status_code=400)
    try:
        _validate_sqlite_file(tmp_path)
    except _ArchiveRejected as e:
        log.warning("Rejected /share archive %s: %s", src, e)
        if extracted is not None:
            extracted.unlink(missing_ok=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    try:
        log.info("Importing history from %s (labels_only=%s)", src, labels_only)
        # The whole merge — including its single `with orch.db:` transaction —
        # runs in ONE run_db callable, so no foreign statement can land inside
        # the open transaction.
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
        """The entire quick-restore — PRAGMA toggles, the single bulk
        transaction, the events normalize/dedup pass and the circuit-label
        restore — in ONE DB-thread callable. Splitting it would leave the bulk
        transaction open across a queue boundary; running it on the loop thread
        would put multi-second DELETE/INSERT batches on the shared connection
        while the DB worker may be mid-statement."""
        imported: dict = {}
        # PRAGMA foreign_keys must be set outside the transaction — SQLite
        # ignores it when a transaction is already open.  Disable for the bulk
        # restore so cross-table FK ordering (e.g. events → fixtures) does not
        # block the DELETE pass, then re-enable immediately after.
        db.execute("PRAGMA foreign_keys = OFF")
        # One transaction: any DELETE/INSERT failure rolls back every prior
        # DELETE, so no table is left wiped-but-not-restored. Every listed table
        # is cleared unconditionally — even when the backup has an empty array
        # or omits it — so stale rows from a previous restore cannot bleed through.
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
                normalize_events_utc(db)
                removed = dedup_events(db)
                if removed:
                    log.warning(
                        "Quick Restore: removed %d duplicate event(s) from backup",
                        removed)
            except Exception as e:
                log.warning("Quick Restore dedup failed (non-fatal): %s", e)

        # Restore circuit display labels from backup, or seed defaults for old
        # backups. Shared with the setup wizard's restore — restore_utils
        # exists so the two paths cannot drift apart.
        try:
            restore_circuit_labels(db, payload)
        except Exception as e:
            log.warning("Quick Restore: circuit label restore failed "
                        "(non-fatal): %s", e)
        return imported

    try:
        imported = await run_db(_restore_sync)
    except Exception as e:
        log.error("Import quick-restore failed: %s", e)
        return JSONResponse({"ok": False, "error": f"Restore failed: {e}"},
                            status_code=500)

    try:
        await orch.reload_circuit_entities_async()
    except Exception as e:
        log.warning("Import reload: %s", e)

    # Reload circuit labels into the in-memory config
    try:
        await orch.reload_circuit_labels_async()
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

    ``labels_only`` merges ONLY the archive's user-labelled events — training
    fuel, not history. A fresh start once discarded 486 hand labels and the
    classifier's coverage (not its accuracy) collapsed; importing them back
    roughly triples the pool, most of all for the starved classes.

    Rows arrive with FEATURES INTACT. Blanking pressure columns looks
    conservative and is the opposite: `pressure_delta_psi` is a LINEAR k-NN
    dimension, so NULL becomes a fabricated "0 psi drop" that pulls every
    imported row into one corner. Cross-regime distance is already handled:
    rule-fit pools are windowed by timestamp, the active/edge k-NN tiers
    hard-require active-flow columns firmware-3.12 rows lack (those serve the
    legacy tier), and the pressure feature conditions on supply regime.
    """
    orch = _orch(request)
    raw = await file.read(MAX_BACKUP_BYTES + 1)
    if len(raw) > MAX_BACKUP_BYTES:
        return JSONResponse({"ok": False,
                             "error": "File too large (max 50 MB)."},
                            status_code=413)

    # /data, not the system temp dir: a container's /tmp can be tmpfs on HA OS,
    # and this container drives the valve. Bounded here by MAX_BACKUP_BYTES
    # already, but the destination should not differ between the two paths.
    fd, tmp_name = tempfile.mkstemp(dir=str(_extract_dir()), suffix=".db")
    tmp_path = Path(tmp_name)
    with open(fd, "wb") as tmp:
        tmp.write(raw)
    try:
        _validate_sqlite_file(tmp_path)
    except _ArchiveRejected as e:
        tmp_path.unlink(missing_ok=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    try:
        # The whole merge — including its single `with orch.db:` transaction —
        # runs in ONE run_db callable, so no foreign statement can land inside
        # the open transaction.
        return await run_db(_merge_archive_from_path, orch, tmp_path,
                            labels_only)
    finally:
        tmp_path.unlink(missing_ok=True)


def _merge_archive_from_path(orch, db_path: Path,
                             labels_only: bool) -> JSONResponse:
    """The history-archive merge core, shared by the upload endpoint and the
    /share pickup: merge rows from the SQLite file at ``db_path`` into
    the live DB, then run the post-merge verdict/reclassify pass. The caller
    owns ``db_path``'s lifetime."""
    imported, errors, ignored = {}, [], {}
    arc = None

    try:
        # NOT a URI open: `connect(uri=True)` re-parses `?`/`#`/`%` in the
        # filename as URI syntax, so `x?mode=rwc&.db` would open a DIFFERENT
        # file READ-WRITE (see _SHARE_NAME_RE). A plain path plus `query_only`
        # gives the read-only guarantee with no parser between us and the name,
        # and does not depend on the caller having validated it.
        arc = sqlite3.connect(str(db_path))
        arc.execute("PRAGMA query_only = ON")
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
                    # so an archive cluster_id is a missing cluster here at
                    # best and a DIFFERENT one at worst (observed: 272 orphaned
                    # + 11 joined to wrong clusters, replayed every boot).
                    # Rows arrive unlinked; the post-merge backfill re-derives
                    # membership here. Features are measurements, not
                    # references — they stay.
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
                    # Heal-on-reimport: rows inserted by an older build still
                    # carry the source install's (missing or colliding) cluster
                    # linkage. Re-importing is otherwise a no-op (INSERT OR
                    # IGNORE), so it doubles as the repair channel: clear the
                    # linkage on every archive row already here. Safe — the
                    # startup backfill/reclassify re-derives it locally.
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
                from ..reclassify import reclassify_all_events_from_signatures
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
    # One COUNT(*) per table — off the loop thread.
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
