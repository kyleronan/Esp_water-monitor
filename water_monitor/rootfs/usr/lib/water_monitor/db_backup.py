#!/usr/bin/env python3
"""Supervisor-backup hooks for the live SQLite database (plan unit 2.27).

WHY THIS EXISTS
    `config.yaml` declares `backup: "hot"` (the default), so when Home
    Assistant takes an add-on backup the Supervisor tars `/data` while this
    add-on keeps running. The database is in WAL mode. A tar of a live WAL
    database is three files copied at three different instants:

        water_monitor.db        the main file
        water_monitor.db-wal    committed pages not yet folded into it
        water_monitor.db-shm    the shared index into the WAL

    Copy only the main file and you have silently lost every transaction that
    was still in the WAL. Copy all three at different instants and you can
    catch an auto-checkpoint halfway through rewriting main-file pages, which
    is real corruption, not just lost tail.

    SQLite's own answer is that a live database must be copied with a SQLite
    primitive -- the online backup API or `VACUUM INTO` -- never with a file
    copy. That is what `pre` below does.

WHAT THIS DOES, AND WHAT IT DOES NOT DO
    `pre` writes a point-in-time consistent copy of the database to
    `/data/water_monitor.db.supervisor-backup` using `VACUUM INTO`, verifies it
    with `PRAGMA quick_check`, and then best-effort checkpoints the live WAL.
    The Supervisor's tar therefore carries BOTH the live file set (which is
    still only best-effort) AND one copy that a SQLite primitive produced and
    that was verified before it was handed over.

    It does NOT make the tar atomic. Writes continue during the archive window
    -- see the "WHAT THIS DOES NOT PROTECT AGAINST" block at the bottom.

WHY NOT HOLD THE APP'S WRITE LOCK
    `database.get_write_lock()` is an in-process `asyncio.Lock`. This runs as a
    separate process, so it cannot take that lock at all; reaching it would
    mean an HTTP call to the running app, and every non-`/health` path is
    behind the ingress-IP guard in main.py, which 127.0.0.1 fails. Punching a
    loopback hole in that guard is the exact class of change that made
    X-Remote-User-Id forgeable once already.

    Even if it could be taken, it would not deliver what it promises:
    `run_isolated_write`'s own docstring records that the lock serialises ADMIN
    write jobs against each other, while the live feature extractor and the
    pruner write inline on the shared `orch.db` connection WITHOUT taking it.
    A held write lock is therefore not a quiesced database.

    And holding any lock across the Supervisor's archive window -- an interval
    this process does not control and cannot observe -- risks wedging the one
    DB worker for good if `backup_post` never runs. A wedged DB worker means no
    event detection, and leak detection is the product. An imperfect backup is
    the smaller failure by a wide margin.

    `VACUUM INTO` takes only a READ transaction, and in WAL mode a reader never
    blocks a writer. Nothing here can wedge anything, and every lock this
    process holds dies with the process.

WHY NOT `backup: "cold"`
    The Supervisor stops the container, so the house goes unmonitored for the
    length of every backup. Rejected on those grounds alone.

WHY NOT `backup_exclude` THE LIVE DATABASE
    Considered, and rejected. Excluding `water_monitor.db*` would leave exactly
    one database in the archive -- the clean snapshot -- and halve the archive.
    But then a `pre` that failed for ANY reason (disk pressure, a locked file,
    a bad interpreter) would produce an archive containing NO database at all,
    silently. An empty backup is far worse than a torn one, so the live file
    set stays in the archive as the floor and the snapshot sits on top of it.

EXIT CODE
    Always 0. The Supervisor's handling of a non-zero `backup_pre` is not
    documented, and no failure of this script is worth failing or skipping the
    user's backup over. Everything is reported on stdout, which lands in the
    add-on log.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

DB_NAME = "water_monitor.db"
# Named, not timestamped, so `post` and `recover` can find it without globbing
# and a second `pre` cannot accumulate copies.
SNAPSHOT_SUFFIX = ".supervisor-backup"
PARTIAL_SUFFIX = ".supervisor-backup.partial"
# Written by SQLite beside the main file; must be removed when a snapshot is
# promoted, or SQLite would apply a WAL belonging to a DIFFERENT database.
SIDECAR_SUFFIXES = ("-wal", "-shm")

# `pre` writes a whole second copy of the database into /data. Refuse unless
# there is comfortably room for it. There is no free-space check anywhere
# before the nightly full-DB backup (plan unit 8.9); this guard is scoped to
# THIS script's own write so that adding a snapshot cannot be the thing that
# fills the disk.
MIN_FREE_RATIO = 1.2

# Long enough to ride out a busy writer, short enough that `pre` cannot stall a
# backup for minutes.
BUSY_TIMEOUT_MS = 30_000


def _say(msg: str) -> None:
    print("[wm-backup] " + msg, flush=True)


def data_dir() -> Path:
    """Same env override as app/config.py, so tests can point it at tmp_path."""
    return Path(os.environ.get("DATA_DIR", "/data"))


def db_path(base: Path | None = None) -> Path:
    return (base or data_dir()) / DB_NAME


def snapshot_path(db: Path) -> Path:
    return db.with_name(db.name + SNAPSHOT_SUFFIX)


def partial_path(db: Path) -> Path:
    return db.with_name(db.name + PARTIAL_SUFFIX)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=BUSY_TIMEOUT_MS / 1000.0)
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    return conn


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError as e:                      # pragma: no cover - never fatal
        _say(f"could not remove {path.name}: {e}")


def quick_check(path: Path) -> bool:
    """True when ``path`` opens and passes ``PRAGMA quick_check``.

    A corrupt file can raise rather than return a row, so both outcomes are
    folded into the same False.
    """
    if not path.exists():
        return False
    try:
        conn = sqlite3.connect(str(path))
        try:
            row = conn.execute("PRAGMA quick_check(1)").fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError as e:
        _say(f"{path.name} failed to open for quick_check: {e}")
        return False
    return bool(row) and str(row[0]).lower() == "ok"


def have_room_for_snapshot(db: Path) -> bool:
    """Whether /data can take a second copy of the database with margin."""
    try:
        need = db.stat().st_size * MIN_FREE_RATIO
        free = shutil.disk_usage(str(db.parent)).free
    except OSError as e:                      # pragma: no cover - never fatal
        _say(f"could not measure free space ({e}); attempting the snapshot")
        return True
    if free < need:
        _say(f"skipping snapshot: {free} bytes free, need ~{int(need)}")
        return False
    return True


def write_snapshot(db: Path) -> bool:
    """Write a verified, consistent copy of ``db`` to its snapshot path.

    ``VACUUM INTO`` is the primitive of choice over ``Connection.backup``: it
    is one statement, it holds only a read transaction, and its output is a
    freshly written database in rollback-journal mode -- a SINGLE
    self-contained file with no WAL sidecar of its own, which is exactly what
    should go into an archive. ``backup()`` is the fallback for a SQLite older
    than 3.27.

    Written to a `.partial` name and renamed into place, so a process killed
    mid-copy can never leave a truncated file under the name `recover` trusts.
    """
    partial = partial_path(db)
    dest = snapshot_path(db)
    _unlink(partial)
    try:
        src = _connect(db)
        try:
            try:
                src.execute("VACUUM INTO ?", (str(partial),))
            except sqlite3.DatabaseError as e:
                _say(f"VACUUM INTO unavailable ({e}); using the backup API")
                _unlink(partial)
                dst = sqlite3.connect(str(partial))
                try:
                    src.backup(dst)
                finally:
                    dst.close()
        finally:
            src.close()
    except (sqlite3.DatabaseError, OSError) as e:
        _say(f"snapshot FAILED ({e}); the archive will carry the live files "
             f"only. This is a degraded backup, not a broken add-on.")
        _unlink(partial)
        return False

    # Verify before handing it over. A snapshot that is itself bad is worse
    # than no snapshot, because `recover` would promote it over a live
    # database that merely looked suspect.
    if not quick_check(partial):
        _say("snapshot failed quick_check and was discarded")
        _unlink(partial)
        return False

    try:
        os.replace(str(partial), str(dest))
    except OSError as e:                      # pragma: no cover - never fatal
        _say(f"could not put the snapshot in place: {e}")
        _unlink(partial)
        return False
    _say(f"snapshot written and verified: {dest.name} "
         f"({dest.stat().st_size} bytes)")
    return True


def checkpoint_live(db: Path) -> bool:
    """Fold the WAL back into the main file. PARTIAL MITIGATION ONLY.

    It makes the live main file self-sufficient AT THIS INSTANT, so the copy
    the tar takes of it is as complete as a file copy can be. It is NOT atomic
    against the writes that continue for the whole archive window afterwards.
    """
    try:
        conn = _connect(db)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
    except sqlite3.DatabaseError as e:
        _say(f"WAL checkpoint skipped ({e}) -- non-fatal")
        return False
    return True


def cmd_pre(db: Path) -> None:
    """`backup_pre`: leave a verified snapshot beside the live database."""
    if not db.exists():
        _say("no database yet; nothing to snapshot")
        return
    wrote = False
    if have_room_for_snapshot(db):
        wrote = write_snapshot(db)
    if not wrote:
        old = snapshot_path(db)
        if old.exists():
            age_h = (time.time() - old.stat().st_mtime) / 3600.0
            _say(f"WARNING: the archive will carry a snapshot that is "
                 f"{age_h:.1f} h old")
    checkpoint_live(db)


def cmd_post(db: Path) -> None:
    """`backup_post`: reclaim the doubled footprint the moment the tar is done.

    The snapshot exists only for the length of one backup, so steady-state disk
    use in /data is unchanged. The archive keeps its copy.
    """
    for p in (partial_path(db), snapshot_path(db)):
        if p.exists():
            _say(f"removing {p.name}")
        _unlink(p)


def cmd_recover(db: Path) -> None:
    """Boot step: make the snapshot in a RESTORED archive actually count.

    Without this the snapshot is decoration -- a restore puts both files back
    and the app opens the live one regardless of its state.

    Deliberately a no-op, at zero cost, whenever no snapshot is present. That
    is the normal boot: `post` removed it. A snapshot only survives into a boot
    after a restore, or after a backup whose `post` never ran. So the
    (whole-file) `quick_check` below runs at most once after a restore, never
    on an ordinary restart.
    """
    snap = snapshot_path(db)
    _unlink(partial_path(db))
    if not snap.exists():
        return

    if db.exists():
        if quick_check(db):
            _say("restored database is healthy; discarding the spare snapshot")
            _unlink(snap)
            return
        # Keep the bad file. It may hold rows the snapshot predates, and
        # deleting a user's only other copy of their history is not this
        # script's call to make.
        aside = db.with_name(db.name + ".corrupt-"
                             + time.strftime("%Y%m%d-%H%M%S"))
        _say(f"restored database FAILED quick_check; setting it aside as "
             f"{aside.name}")
        try:
            os.replace(str(db), str(aside))
        except OSError as e:
            _say(f"could not set it aside ({e}); leaving the database alone")
            return
    else:
        _say("no database present; promoting the backup snapshot")

    # The restored -wal/-shm belong to the file we just moved aside. Applying
    # them to the snapshot would corrupt it, so they go first.
    for suffix in SIDECAR_SUFFIXES:
        _unlink(db.with_name(db.name + suffix))
    try:
        os.replace(str(snap), str(db))
    except OSError as e:                      # pragma: no cover - never fatal
        _say(f"could not promote the snapshot: {e}")
        return
    _say("snapshot promoted to the live database")


_COMMANDS = {"pre": cmd_pre, "post": cmd_post, "recover": cmd_recover}


def main(argv: list[str]) -> int:
    """Always returns 0 -- see the module docstring."""
    action = argv[1] if len(argv) > 1 else ""
    fn = _COMMANDS.get(action)
    if fn is None:
        _say(f"usage: db_backup.py {{{'|'.join(_COMMANDS)}}} (got {action!r})")
        return 0
    try:
        fn(db_path())
    except Exception as e:                    # noqa: BLE001 - see docstring
        _say(f"{action} failed ({type(e).__name__}: {e}) -- continuing")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))


# WHAT THIS DOES NOT PROTECT AGAINST -- stated plainly, because a fix that
# claims atomicity it does not have is worse than a partial one that says so.
#
#  * The tar is still not atomic. `pre` runs to completion and THEN the
#    Supervisor archives /data; every write in that window lands in the WAL
#    after the snapshot was taken. Those writes are in the archived live file
#    set (maybe) and are NOT in the snapshot. The snapshot is a point-in-time
#    copy from the start of the backup, so a restore that falls back to it
#    loses the events detected during the archive window -- seconds to minutes
#    of history, never a corrupt database.
#
#  * An auto-checkpoint firing DURING the tar can still rewrite main-file pages
#    while tar is reading them, which is the genuine corruption path for the
#    live file set. Suppressing that needs `PRAGMA wal_autocheckpoint=0` on the
#    RUNNING APP's connections for the window -- a per-connection setting, so
#    it cannot be done from this process. That is precisely why the snapshot
#    exists: the archive does not depend on the live file set being sound.
#
#  * `checkpoint_live` is the option-(b) mitigation and nothing more. It is not
#    atomic against concurrent writes and never was.
#
#  * If /data is too full for a second copy, `pre` says so and the archive
#    degrades to the live file set alone.
