"""Per-event waveform capture — the firmware wire format and its reassembler.

The MIDDLE layer of three: it depends on ``event_detector_core`` (for the
shared logger only) and knows nothing about ``EventDetector``, which is what
keeps the dependency graph acyclic — ``event_detector`` -> ``event_waveform``
-> ``event_detector_core``, never back the other way.

Every ``_WF_*`` constant here is FIRMWARE WIRE-FORMAT VOCABULARY: the values are
fixed by what the ESP publishes, so this module is the one place they may be
defined. ``test_unit73_event_detector_split`` pins
``_WF_FL_RESOLUTION_REDUCED`` against feature_extractor's copy, and
``test_dead_symbols_dev59`` pins its LINE NUMBER — do not shift it.
"""
from __future__ import annotations

import base64
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .event_detector_core import log


# ------------------------------------------------------------------------------- #
# Per-event waveform capture (firmware 3.9.0+) — chunked HA-event accumulator    #
# ------------------------------------------------------------------------------- #
# Each event is delivered as a stream of esphome.water_monitor_waveform_chunk
# events. A chunk fires every time a firmware-side 1500-sample buffer fills
# (~30 s @ ~50 Hz) and also on event end. The accumulator stitches chunks
# keyed on (boot_id, event_id) and emits a WaveformRecord when the final
# chunk arrives and all expected seqs are present. Native cadence is
# preserved; arrays are variable-length — feature_extractor.py is fine with
# variable lengths already.

# Wire-format / safety constants.
_WF_START_SAMPLES: int = 150            # first slice of full_flow used as start_flow (3 s @ ~50 Hz)
_WF_MAX_RECORDS: int = 30               # max assembled records kept per circuit
_WF_FLAG_VALID_MASK: int = 0x7F         # bits 0-6 only; bit 7 must be 0
_WF_INFLIGHT_TTL_S: float = 7200.0      # 2 h — absolute upper bound on supported event length
# Once the FINAL chunk has arrived (so `total` is known), a set still missing
# chunks is a transport GAP: give up after this grace rather than holding memory
# for the full 2 h TTL, and count it. Generous enough for a delayed chunk.
_WF_FINAL_GAP_TIMEOUT_S: float = 120.0
_WF_FL_RESOLUTION_REDUCED: int = 0x04   # firmware dropped/decimated samples (a hole)
_WF_MAX_CHUNK_SAMPLES: int = 1500       # firmware buffer cap; bound decoded-payload length defensively
_WF_MAX_TOTAL_CHUNKS: int = 600         # bound for the 'total' field (~5 hour event at 30s cadence)
# Receiver-side reassembly bounds. Chunks arrive from a device over the HA
# event bus, so the sender is untrusted input and unbounded reassembly state is
# a denial-of-service primitive (CVE-2018-5391 "FragmentSmack"). Two dimensions
# must be bounded INDEPENDENTLY — bounding either alone still leaves a way to
# grow memory without limit:
#   * how large ONE in-flight set may become  -> _WF_MAX_TOTAL_CHUNKS, enforced
#     against `seq` on every chunk (see on_waveform_chunk), not just against
#     the wire `total` field that only arrives on the FINAL chunk.
#   * how MANY in-flight sets may exist       -> _WF_MAX_INFLIGHT_SETS below.
# The TTL sweep (_evict_stale) bounds TIME but neither of these: it only runs
# when a chunk arrives, and a sender that keeps sending never trips it.
#
# High/low water marks follow the Linux ipfrag_high_thresh / ipfrag_low_thresh
# shape: at the high mark, evict oldest-first down to the low mark, so the
# table cannot be pinned at capacity with one eviction per arriving chunk.
# Normal operation holds ONE in-flight set per circuit (occasionally two
# across a boot_id change), so 16 is far above any legitimate working set.
_WF_MAX_INFLIGHT_SETS: int = 16         # high-water mark on len(self._inflight)
_WF_INFLIGHT_LOW_WATER: int = 12        # evict oldest-first down to this


@dataclass
class WaveformMetadata:
    """Per-event waveform metadata, populated from the final chunk."""
    event_id: int           # id  — monotonic per-boot event counter
    boot_id: int            # b   — ESP session id
    start_ms: int           # event_s — event-wide start millis()
    end_ms: int             # event_e — event-wide end millis()
    start_points: int       # sn — len(start_flow) (derived: min(_WF_START_SAMPLES, full))
    full_points: int        # fn — len(full_flow)  (sum of chunk samples)
    flow_scale: int         # int16 → L/min divisor (constant 100 today)
    pressure_scale: int     # int16 → PSI divisor (constant 100 today)
    peak_flow: float        # pk — peak flow (×100 in wire, /100 here)
    pressure_delta: float   # dp — pressure delta (×100 in wire, /100 here)
    propagation_delay_ms: int   # pd — onset propagation delay (ms; -1 = not detected)
    quality: int            # q  — 0 ok, 1 incomplete, firmware never publishes 2-6
    flags: int              # fl — bitfield (see plan)
    # Onset position. ``onset_seq`` and ``onset_idx`` are the source chunk
    # and within-chunk sample index as reported by firmware; ``onset_index``
    # is the linear index into the concatenated full_flow/full_pressure and
    # is resolved by the accumulator once chunk lengths are known.
    #
    # Default to -1 (not 0) so a record constructed without onset fields —
    # malformed parse, future schema change, hand-built test fixture — cannot
    # look like "onset at sample 0" and have feature_extractor treat the
    # pre-roll as the post-onset ramp. _assemble also validates that
    # (onset_seq, onset_idx) is in bounds for the received chunks.
    onset_seq: int = -1
    onset_idx: int = -1
    onset_index: int = -1
    pressure_onset_seq: int = -1
    pressure_onset_idx: int = -1
    pressure_onset_index: int = -1
    # Legacy fields kept at zero for compat — pre/post/tail no longer meaningful.
    pre_ms: int = 0
    post_ms: int = 0
    tail_ms: int = 0
    version: int = 1


@dataclass
class WaveformRecord:
    """Fully assembled and decoded per-event waveform — ready for feature extraction."""
    circuit: str
    boot_id: int
    event_id: int
    metadata: WaveformMetadata
    # Variable-length lists — feature_extractor.py guards each access with `if x:`.
    start_flow: List[float]       # L/min — first _WF_START_SAMPLES of full_flow
    start_pressure: List[float]   # PSI
    full_flow: List[float]        # L/min — concatenated across all chunks
    full_pressure: List[float]    # PSI
    received_at: float            # time.monotonic() when the final chunk assembled


@dataclass
class _InflightChunkSet:
    """Per-event scratchpad: chunks seen, final metadata once it arrives, TTL stamp."""
    chunks: Dict[int, Tuple[List[float], List[float]]] = field(default_factory=dict)
    total: Optional[int] = None
    final_metadata: Optional[WaveformMetadata] = None
    first_received_at: float = 0.0
    # Track which (seq) we've already DEBUG-logged a duplicate for, so the
    # log doesn't spam if a chunk arrives 3+ times.
    duplicate_logged: set = field(default_factory=set)


def _parse_wire_bool(value: Any) -> bool:
    """Parse a wire-format boolean tolerantly.

    HA may pass back the literal string from the template, or coerce to a
    native bool. Accept "true"/"false" (any case) and native True/False;
    raise ValueError on anything else so the caller can DEBUG-log + reject.
    """
    if value is True:
        return True
    if value is False:
        return False
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise ValueError(f"invalid wire bool: {value!r}")


def _normalize_int_field(d: dict, key: str) -> Optional[int]:
    """Get d[key] and normalize to int. Returns None on missing/invalid.

    Accepts str or native int; rejects bool (Python bool is int subclass).
    Logs DEBUG with the rejection reason; the caller just checks for None.
    """
    raw = d.get(key)
    if raw is None:
        log.debug("waveform: chunk missing field %r", key)
        return None
    if isinstance(raw, bool):
        log.debug("waveform: chunk field %r has unexpected bool value", key)
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.debug("waveform: chunk field %r not castable to int: %.40r", key, raw)
        return None


def _parse_chunk_scalars(data: dict) -> Optional[dict]:
    """Parse and range-check the per-chunk scalar fields. Returns a dict of
    normalized ints (b, id, seq, s, e, fs, ps) or None on failure."""
    out: dict = {}
    for key in ("b", "id", "seq", "s", "e", "fs", "ps"):
        v = _normalize_int_field(data, key)
        if v is None:
            return None
        out[key] = v
    if out["b"] < 0 or out["id"] < 0 or out["seq"] < 0:
        log.debug("waveform: chunk negative b/id/seq")
        return None
    if out["fs"] <= 0 or out["ps"] <= 0:
        log.debug("waveform: chunk fs/ps must be positive")
        return None
    return out


def _parse_final_metadata(
    data: dict, circuit: str, full_len: int, start_len: int,
) -> Optional[Tuple[WaveformMetadata, int]]:
    """Build a WaveformMetadata from the final chunk's extra fields.

    `full_len` is the total sample count after concatenation; `start_len` is
    the prefix slice length (min of _WF_START_SAMPLES and full_len).

    Returns ``(metadata, total)`` so the caller does not re-parse ``total``
    (a defensive re-parse risks silently substituting 0 and assembling an
    incomplete record).
    """
    b = _normalize_int_field(data, "b")
    eid = _normalize_int_field(data, "id")
    evs = _normalize_int_field(data, "event_s")
    eve = _normalize_int_field(data, "event_e")
    total = _normalize_int_field(data, "total")
    onset_seq = _normalize_int_field(data, "onset_seq")
    onset_idx = _normalize_int_field(data, "onset_idx")
    press_onset_seq = _normalize_int_field(data, "press_onset_seq")
    press_onset_idx = _normalize_int_field(data, "press_onset_idx")
    pk = _normalize_int_field(data, "pk")
    dp = _normalize_int_field(data, "dp")
    pd = _normalize_int_field(data, "pd")
    q = _normalize_int_field(data, "q")
    fl = _normalize_int_field(data, "fl")
    fs = _normalize_int_field(data, "fs")
    ps = _normalize_int_field(data, "ps")
    if None in (b, eid, evs, eve, total, onset_seq, onset_idx,
                pk, dp, pd, q, fl, fs, ps,
                press_onset_seq, press_onset_idx):
        return None
    if total <= 0 or total > _WF_MAX_TOTAL_CHUNKS:
        log.debug("waveform[%s]: chunk total=%d out of range", circuit, total)
        return None
    if not (0 <= q <= 6):
        log.debug("waveform[%s]: final q=%d out of range", circuit, q)
        return None
    if fl & ~_WF_FLAG_VALID_MASK:
        log.debug("waveform[%s]: final fl=%d has invalid high bits", circuit, fl)
        return None
    # Onset position is reported here in (seq, idx) form; the linear index
    # into the concatenated full array is resolved by the accumulator once
    # all chunk lengths are known (`_assemble`).
    meta = WaveformMetadata(
        event_id=eid,
        boot_id=b,
        start_ms=evs,
        end_ms=eve,
        start_points=start_len,
        full_points=full_len,
        flow_scale=fs,
        pressure_scale=ps,
        peak_flow=pk / 100.0,
        pressure_delta=dp / 100.0,
        propagation_delay_ms=pd,
        quality=q,
        flags=fl,
        onset_seq=onset_seq,
        onset_idx=onset_idx,
        pressure_onset_seq=press_onset_seq,
        pressure_onset_idx=press_onset_idx if press_onset_seq >= 0 else -1,
    )
    return meta, total


def _decode_waveform(
    b64_payload: str,
    scale: int,
    expected_pts: Optional[int] = None,
) -> Optional[List[float]]:
    """Decode a base64-encoded little-endian int16 waveform payload.

    Returns the decoded floats, or None when malformed. If ``expected_pts``
    is given, the byte length must match exactly; if None, any whole int16
    count is accepted (used by the chunk path, where chunk length varies).
    """
    if not b64_payload:
        log.debug("waveform: empty base64 payload")
        return None
    try:
        raw = base64.b64decode(b64_payload, validate=True)
    except Exception:
        log.debug("waveform: base64 decode error for payload %.40r", b64_payload)
        return None
    if len(raw) % 2 != 0:
        log.debug("waveform: payload length %d not a whole int16 count", len(raw))
        return None
    if expected_pts is not None and len(raw) != expected_pts * 2:
        log.debug(
            "waveform: payload length %d != expected %d bytes (%d pts × 2)",
            len(raw), expected_pts * 2, expected_pts,
        )
        return None
    pts = len(raw) // 2
    if pts > _WF_MAX_CHUNK_SAMPLES:
        log.debug(
            "waveform: payload %d samples exceeds max chunk size %d",
            pts, _WF_MAX_CHUNK_SAMPLES,
        )
        return None
    values: List[float] = []
    for i in range(pts):
        (v,) = struct.unpack_from("<h", raw, i * 2)
        values.append(v / scale)
    return values


def _normalize_node_name(name: str) -> str:
    """Normalize an ESPHome node name for identity comparison.

    App.get_name() may use hyphens; HA entity prefixes use underscores.
    Both sides of the comparison must be normalized the same way.
    """
    return name.strip().lower().replace("-", "_")


class WaveformChunkAccumulator:
    """
    Per-circuit accumulator for chunked waveform delivery (firmware 3.9.0+).

    The firmware streams `esphome.water_monitor_waveform_chunk` events keyed
    by (boot_id, event_id, seq). Each non-final chunk carries a slice of the
    flow + pressure waveform; the final chunk carries the last slice (which
    may be empty) plus event-wide metadata + the expected `total` chunk count.

    Lifecycle per event:
      1. Chunks arrive in any order; held in an in-flight set per (boot, id).
      2. Final chunk sets `total` and the WaveformMetadata.
      3. When all seqs 0..total-1 are present, the accumulator concatenates
         them and emits a WaveformRecord. The in-flight entry is removed.
      4. Missing seqs at final time → DEBUG log, no record (a delayed chunk
         can still complete the set later until TTL eviction).

    **Durability:** chunk accumulation is in-memory only. An add-on restart
    during an active event drops in-flight chunks for that event; the final
    chunk arriving after restart will be missing predecessors and won't
    assemble. EventDetector continues normal event detection without
    waveform enrichment in that specific case — no persistence layer.

    Exposes get_record / latest_record / pop_record so EventDetector's
    public accessors are a thin pass-through (see EventDetector below).
    """

    def __init__(self, circuit: str, expected_node: str,
                 on_record_assembled: Optional[Callable[[WaveformRecord], None]] = None,
                 on_degraded: Optional[Callable[[str, str], None]] = None) -> None:
        self._circuit = circuit
        # Normalize once so every comparison is cheap.
        self._expected_node = _normalize_node_name(expected_node)
        self._inflight: Dict[Tuple[int, int], _InflightChunkSet] = {}
        self._records: List[WaveformRecord] = []
        # Transport-health counters (since boot/restart): total assembled,
        # assembled-but-firmware-flagged-degraded (quality / resolution-reduced),
        # and transport GAPS (final arrived but chunks lost → discarded).
        self._n_assembled = 0
        self._n_degraded = 0
        self._n_gaps = 0
        # Reassembly-bound counters. Rejections/evictions are COUNTED rather
        # than logged per chunk: the bounds exist to survive a flood, and a log
        # line per rejected chunk moves the denial-of-service from the heap to
        # the log. Surfaced via transport_stats(); the first occurrence of each
        # is logged at WARNING so the condition is visible without tailing
        # DEBUG.
        self._n_rejected_seq = 0        # chunk dropped: seq >= _WF_MAX_TOTAL_CHUNKS
        self._n_overflow_evicted = 0    # in-flight set dropped: table at capacity
        # The transport_version gate is the STRICTEST test in this class (exact
        # string "1") and the least observable: a firmware that bumps it drops
        # 100% of chunks with no visible difference from "the ESP is quiet".
        # Counted here and surfaced via transport_stats(); the first occurrence
        # also escalates through `on_degraded` so /health/detail names it
        # instead of merely reporting zero waveforms.
        self._n_rejected_version = 0
        self._on_degraded = on_degraded
        self._degraded_reported: set = set()
        if not self._expected_node:
            # See device_discovery._derive_prefix: an empty prefix does not
            # reject anything, it turns the node-identity guard below
            # (`if self._expected_node and ...`) into a no-op. Surfaced as
            # transport_stats()["node_check_enabled"] = False.
            log.warning(
                "waveform[%s]: no expected node name (esp_device_prefix is "
                "empty) — the waveform node-identity check is DISABLED; "
                "chunks from ANY ESPHome node will be accepted for this "
                "circuit. Re-run device discovery to repopulate the prefix.",
                self._circuit,
            )
        # Optional sink fired once a record finishes assembling (late-waveform
        # upgrade). Kept DB-free; invoked in a try/except in _assemble so a
        # sink bug can never corrupt assembly. None in most tests.
        self._on_record_assembled = on_record_assembled

    def _report_degraded(self, key: str, message: str) -> None:
        """Escalate a transport condition ONCE per process, best-effort.

        Wired by EventDetector to Orchestrator.mark_subsystem_degraded, which
        writes _supervise's record shape into ``worker_health`` — the surface
        /health/detail already reads. Fire-once: the conditions reported are
        all-or-nothing (a version bump drops every chunk), so repeating adds
        nothing and a flood must never become a notification amplifier.
        """
        if key in self._degraded_reported:
            return
        self._degraded_reported.add(key)
        if self._on_degraded is None:
            return
        try:
            self._on_degraded(key, message)
        except Exception as e:      # pragma: no cover - never fatal
            log.debug("waveform[%s]: degraded-report sink raised: %s",
                      self._circuit, e)

    # ------------------------------------------------------------------
    # Public callback
    # ------------------------------------------------------------------

    def on_waveform_chunk(self, data: dict) -> None:
        """Process a single esphome.water_monitor_waveform_chunk event."""
        now = time.monotonic()
        self._evict_stale(now)

        # Identity & schema guards.
        if data.get("schema") != "esp_water_monitor_waveform_chunk":
            log.debug("waveform[%s]: chunk rejected — unexpected schema %r",
                      self._circuit, data.get("schema"))
            return
        if str(data.get("transport_version", "")) != "1":
            self._n_rejected_version += 1
            got = data.get("transport_version")
            if self._n_rejected_version == 1:
                log.warning(
                    "waveform[%s]: chunk rejected — unsupported "
                    "transport_version %r (this add-on speaks \"1\"). EVERY "
                    "chunk carrying this version is dropped, so waveform "
                    "enrichment is off until the add-on is updated. Counted "
                    "as transport_stats.rejected_transport_version.",
                    self._circuit, got)
                self._report_degraded(
                    "waveform_transport_version",
                    "firmware sends waveform transport_version %r; this "
                    "add-on accepts \"1\" only — 100%% of waveform chunks on "
                    "circuit %s are being dropped" % (got, self._circuit))
            else:
                log.debug("waveform[%s]: chunk rejected — unsupported transport_version %r",
                          self._circuit, got)
            return
        node = _normalize_node_name(data.get("node", ""))
        if self._expected_node and node != self._expected_node:
            log.debug("waveform[%s]: chunk rejected — node %r != expected %r",
                      self._circuit, node, self._expected_node)
            return
        if data.get("circuit") != self._circuit:
            return  # silently skip other circuits

        # Scalars.
        sc = _parse_chunk_scalars(data)
        if sc is None:
            return
        try:
            is_final = _parse_wire_bool(data.get("final", "false"))
        except ValueError:
            log.debug("waveform[%s]: chunk rejected — invalid 'final' value %r",
                      self._circuit, data.get("final"))
            return

        # Decode flow/press payloads. On a final chunk an empty payload is
        # explicitly allowed (firmware flushed the last sample in a prior
        # chunk); on a non-final chunk it's a wire-format error.
        flow_b64 = data.get("flow", "")
        press_b64 = data.get("press", "")
        if is_final and flow_b64 == "" and press_b64 == "":
            flow: List[float] = []
            press: List[float] = []
        else:
            decoded_flow = _decode_waveform(flow_b64, sc["fs"])
            decoded_press = _decode_waveform(press_b64, sc["ps"])
            if decoded_flow is None or decoded_press is None:
                log.debug("waveform[%s]: chunk seq=%d rejected — payload decode failed",
                          self._circuit, sc["seq"])
                return
            if len(decoded_flow) != len(decoded_press):
                log.debug(
                    "waveform[%s]: chunk seq=%d rejected — flow/press length mismatch (%d vs %d)",
                    self._circuit, sc["seq"], len(decoded_flow), len(decoded_press),
                )
                return
            flow, press = decoded_flow, decoded_press

        key = (sc["b"], sc["id"])
        seq = sc["seq"]

        # Bound 1 — absolute cap on `seq`, enforced BEFORE `cs.total` is known.
        # `total` only arrives on the FINAL chunk, so the `seq >= cs.total`
        # guard below is dead until then and a sender streaming non-final chunks
        # with ever-increasing seq grows ONE set without limit (each entry up to
        # _WF_MAX_CHUNK_SAMPLES floats x2 arrays) for the full 2 h TTL. Because
        # cs.chunks is keyed by seq, capping seq also caps len(cs.chunks) at
        # _WF_MAX_TOTAL_CHUNKS, the maximum the wire format declares. Checked
        # before the in-flight set is looked up/created so a junk chunk cannot
        # even allocate one.
        if seq >= _WF_MAX_TOTAL_CHUNKS:
            self._n_rejected_seq += 1
            if self._n_rejected_seq == 1:
                log.warning(
                    "waveform[%s]: chunk seq=%d rejected — exceeds hard cap %d "
                    "(boot=%d id=%d); further occurrences counted only "
                    "(transport_stats.rejected_seq)",
                    self._circuit, seq, _WF_MAX_TOTAL_CHUNKS, sc["b"], sc["id"],
                )
            else:
                log.debug(
                    "waveform[%s]: chunk seq=%d rejected — exceeds hard cap %d "
                    "(boot=%d id=%d)",
                    self._circuit, seq, _WF_MAX_TOTAL_CHUNKS, sc["b"], sc["id"],
                )
            return

        cs = self._inflight.get(key)
        if cs is None:
            # Bound 2 — admission control on the NUMBER of in-flight sets.
            # Every distinct (boot_id, event_id) past the node/circuit guards
            # allocates one. Make room BEFORE allocating so the table can never
            # exceed the high water mark.
            self._enforce_inflight_capacity()
            cs = _InflightChunkSet(first_received_at=now)
            self._inflight[key] = cs

        # Reject seq values that exceed any already-known total (catches
        # corrupted/duplicate-event_id misroutes).
        if cs.total is not None and seq >= cs.total:
            log.debug(
                "waveform[%s]: chunk seq=%d rejected — exceeds known total=%d (boot=%d id=%d)",
                self._circuit, seq, cs.total, sc["b"], sc["id"],
            )
            return

        # Duplicate handling.
        if seq in cs.chunks:
            prev_flow, _prev_press = cs.chunks[seq]
            if len(prev_flow) == len(flow):
                if seq not in cs.duplicate_logged:
                    log.debug(
                        "waveform[%s]: duplicate chunk seq=%d for boot=%d id=%d — same length, replacing",
                        self._circuit, seq, sc["b"], sc["id"],
                    )
                    cs.duplicate_logged.add(seq)
                cs.chunks[seq] = (flow, press)
            else:
                log.debug(
                    "waveform[%s]: duplicate chunk seq=%d for boot=%d id=%d — length mismatch (%d vs %d), rejecting",
                    self._circuit, seq, sc["b"], sc["id"], len(prev_flow), len(flow),
                )
                return
        else:
            cs.chunks[seq] = (flow, press)

        # On final, capture metadata + total and try to assemble.
        if is_final:
            # Compute provisional full length to feed metadata builder.
            chunks_in_order = sorted(cs.chunks.items())
            full_len = sum(len(f) for _, (f, _p) in chunks_in_order)
            start_len = min(_WF_START_SAMPLES, full_len)
            parsed = _parse_final_metadata(data, self._circuit, full_len, start_len)
            if parsed is None:
                # Don't pop the in-flight entry — TTL will GC it. The DEBUG
                # log already explained why.
                return
            meta, total = parsed
            # Use the already-validated total from _parse_final_metadata
            # rather than re-parsing the wire field (a re-parse failure
            # would silently substitute 0 and assemble an empty record).
            cs.total = total
            cs.final_metadata = meta

        # Try to assemble if we have a known total and all seqs are present.
        if cs.total is not None and cs.final_metadata is not None:
            missing = [s for s in range(cs.total) if s not in cs.chunks]
            if missing:
                log.debug(
                    "waveform[%s]: cannot assemble event %d yet — missing %d/%d chunk(s): seqs %s",
                    self._circuit, sc["id"], len(missing), cs.total, missing,
                )
                return
            self._assemble(key, cs)

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------

    def _assemble(self, key: Tuple[int, int], cs: _InflightChunkSet) -> None:
        meta = cs.final_metadata
        assert meta is not None
        chunks_in_order = sorted(cs.chunks.items())

        full_flow: List[float] = []
        full_press: List[float] = []
        # Resolve the (onset_seq, onset_idx) pair to a linear index into the
        # concatenated full_flow / full_pressure arrays by summing the lengths
        # of all chunks strictly before onset_seq. This is robust to a future
        # firmware that extends the pre-roll across multiple chunks or fires
        # an event without flow pre-roll.
        prefix_len: Dict[int, int] = {}
        chunk_lengths: Dict[int, int] = {}
        running = 0
        for seq, (flow, press) in chunks_in_order:
            prefix_len[seq] = running
            chunk_lengths[seq] = len(flow)
            full_flow.extend(flow)
            full_press.extend(press)
            running += len(flow)

        def _linear(seq: int, idx: int) -> int:
            """Resolve (seq, idx) to a linear index into full_flow/full_press.

            Returns -1 when:
              - seq < 0 (firmware reported "no onset")
              - seq isn't among the received chunks (corrupt metadata)
              - idx isn't a valid position inside that chunk

            Downstream feature_extractor treats -1 as "use legacy onset
            detection from the waveform itself". Do NOT clamp a negative idx
            to 0: that makes a missing-onset record look like onset lived at
            sample 0, wrong for any event with a pre-roll.
            """
            if seq < 0:
                return -1
            if seq not in chunk_lengths:
                return -1
            if not (0 <= idx < chunk_lengths[seq]):
                return -1
            return prefix_len[seq] + idx

        meta.onset_index = _linear(meta.onset_seq, meta.onset_idx)
        meta.pressure_onset_index = _linear(
            meta.pressure_onset_seq, meta.pressure_onset_idx)

        start_flow = full_flow[:_WF_START_SAMPLES]
        start_press = full_press[:_WF_START_SAMPLES]

        # Update lengths in metadata after concat in case _parse_final_metadata
        # under-counted (shouldn't happen but defensive).
        meta.full_points = len(full_flow)
        meta.start_points = len(start_flow)

        # feature_extractor derives per-sample dt from (pre_ms + post_ms) /
        # start_points. The firmware streams at native ~50 Hz (20 ms per
        # sample); populating pre_ms / post_ms makes dt resolve to 0.020 s and
        # keeps the onset position recoverable. An onset inside the start window
        # is expressed through pre_ms/post_ms; otherwise these describe the
        # window's full extent and downstream falls back to legacy detection.
        _SAMPLE_MS = 20
        if meta.start_points > 0:
            if 0 <= meta.onset_index < meta.start_points:
                onset_in_start = meta.onset_index
                meta.pre_ms = onset_in_start * _SAMPLE_MS
                meta.post_ms = (meta.start_points - onset_in_start) * _SAMPLE_MS
            else:
                # Onset is either unknown (onset_index == -1 after the
                # bounds-check above) or lies outside the start window.
                # Describe the window's full extent only; feature_extractor
                # falls back to detecting onset from the waveform itself.
                meta.pre_ms = 0
                meta.post_ms = meta.start_points * _SAMPLE_MS
        # tail_ms = 0: chunked records do not capture a post-event recovery
        # tail. The full waveform spans onset → event end only. Setting this
        # to anything else feeds the wrong "tail" into feature_extractor's
        # recovery_overshoot_psi / steady_state_fraction full-window math.
        # When tail_ms is 0, those blocks fall through and the legacy
        # _pressure_shape_features value survives (which is correct — it
        # measures over the full pressure_readings window).
        meta.tail_ms = 0

        record = WaveformRecord(
            circuit=self._circuit,
            boot_id=meta.boot_id,
            event_id=meta.event_id,
            metadata=meta,
            start_flow=start_flow,
            start_pressure=start_press,
            full_flow=full_flow,
            full_pressure=full_press,
            received_at=time.monotonic(),
        )
        self._records.append(record)
        if len(self._records) > _WF_MAX_RECORDS:
            self._records = self._records[-_WF_MAX_RECORDS:]
        # Flag if the firmware self-reported a degraded capture (these still
        # assemble, but their SIGNATURE is gated in _enrich_from_waveform).
        self._n_assembled += 1
        if meta.quality != 0 or (meta.flags & _WF_FL_RESOLUTION_REDUCED):
            self._n_degraded += 1

        # Done — remove the in-flight entry so late stray chunks for this
        # event_id don't keep growing memory.
        self._inflight.pop(key, None)

        log.debug(
            "waveform[%s]: assembled event %d from %d chunk(s) "
            "(boot=%d full=%d start=%d q=%d fl=0x%02x pk=%.2f dp=%.2f pd=%dms)",
            self._circuit, meta.event_id, cs.total, meta.boot_id,
            meta.full_points, meta.start_points,
            meta.quality, meta.flags,
            meta.peak_flow, meta.pressure_delta, meta.propagation_delay_ms,
        )

        # Notify the optional sink (late-waveform upgrade). Wrapped so a sink
        # error can never corrupt assembly or escape the WS callback.
        if self._on_record_assembled is not None:
            try:
                self._on_record_assembled(record)
            except Exception as e:
                log.warning("waveform[%s]: on_record_assembled sink raised "
                            "(non-fatal): %s", self._circuit, e)

    # ------------------------------------------------------------------
    # Capacity eviction (bound on the NUMBER of in-flight sets)
    # ------------------------------------------------------------------

    def _enforce_inflight_capacity(self) -> None:
        """Evict oldest-first so a new set can be admitted within the cap.

        Called only from the allocation path in on_waveform_chunk. Modelled on
        the Linux IP-fragment reassembly thresholds: at _WF_MAX_INFLIGHT_SETS,
        drop the OLDEST sets (by first_received_at, the same stamp the TTL sweep
        uses) down to _WF_INFLIGHT_LOW_WATER - 1 so there is room for the
        caller's new set. Batching to a low-water mark rather than evicting one
        per arrival stops a sender pinning the table at capacity and forcing an
        eviction for every chunk it sends.

        Oldest-first: the newest sets are the ones an in-progress event is still
        adding to, and the oldest is the most likely to be abandoned. This
        bounds RETAINED memory even when the stream stops entirely, which
        _evict_stale cannot do (it only runs when a chunk arrives).
        """
        if len(self._inflight) < _WF_MAX_INFLIGHT_SETS:
            return
        n_drop = len(self._inflight) - _WF_INFLIGHT_LOW_WATER + 1
        by_age = sorted(self._inflight.items(),
                        key=lambda kv: kv[1].first_received_at)
        for k, _cs in by_age[:n_drop]:
            cs = self._inflight.pop(k, None)
            if cs is None:
                continue
            self._n_overflow_evicted += 1
            # A set whose final chunk had already arrived is a waveform we are
            # now losing — count it the same way the TTL sweep does.
            if cs.final_metadata is not None:
                self._n_gaps += 1
        if self._n_overflow_evicted == n_drop:
            log.warning(
                "waveform[%s]: in-flight table hit cap %d — evicted %d oldest "
                "set(s) down to %d; further occurrences counted only "
                "(transport_stats.overflow_evicted)",
                self._circuit, _WF_MAX_INFLIGHT_SETS, n_drop,
                _WF_INFLIGHT_LOW_WATER - 1,
            )
        else:
            log.debug(
                "waveform[%s]: in-flight table hit cap %d — evicted %d oldest set(s)",
                self._circuit, _WF_MAX_INFLIGHT_SETS, n_drop,
            )

    # ------------------------------------------------------------------
    # TTL eviction
    # ------------------------------------------------------------------

    def _evict_stale(self, now: float) -> None:
        # A set whose FINAL chunk has arrived (total known) but is still incomplete
        # is a transport GAP — evict it after a short grace and COUNT it. A set still
        # awaiting its final chunk uses the long TTL (the event may still be running).
        stale: List[Tuple[Tuple[int, int], bool]] = []
        for k, cs in self._inflight.items():
            final_incomplete = cs.final_metadata is not None
            ttl = _WF_FINAL_GAP_TIMEOUT_S if final_incomplete else _WF_INFLIGHT_TTL_S
            if (now - cs.first_received_at) > ttl:
                stale.append((k, final_incomplete))
        for k, final_incomplete in stale:
            cs = self._inflight.pop(k, None)
            if cs is None:
                continue
            if final_incomplete:
                self._n_gaps += 1
                missing = ([s for s in range(cs.total) if s not in cs.chunks]
                           if cs.total else [])
                log.info(
                    "waveform[%s]: GAP — event %d lost %d/%s chunk(s) in transport "
                    "(missing seqs %s); waveform discarded",
                    self._circuit, k[1], len(missing), cs.total, missing,
                )
            else:
                log.debug(
                    "waveform[%s]: in-flight chunk set evicted (boot=%d id=%d, %d chunk(s), total=%s)",
                    self._circuit, k[0], k[1], len(cs.chunks), cs.total,
                )

    def transport_stats(self) -> Dict[str, Any]:
        """Waveform-transport health since boot: how many waveforms assembled,
        how many were firmware-flagged degraded, and how many were lost to
        transport gaps (a final chunk arrived but predecessors never did).

        Also reports the two conditions that are otherwise SILENT BY
        CONSTRUCTION — a rejected transport_version, and a node-identity check
        switched off because the derived ESP prefix was empty — both otherwise
        indistinguishable from "the device is quiet". Read it as a report,
        never as a gate."""
        return {"assembled": self._n_assembled,
                "degraded": self._n_degraded,
                "gaps": self._n_gaps,
                # Chunks dropped by the exact-match transport_version gate.
                # Non-zero means a firmware/add-on transport mismatch and NO
                # waveform will ever assemble until one side is updated.
                "rejected_transport_version": self._n_rejected_version,
                # False = `_expected_node` is empty, so the identity guard in
                # on_waveform_chunk is inert and chunks from any node are
                # accepted. Comes from device_config.esp_device_prefix, i.e.
                # from device_discovery._derive_prefix.
                "node_check_enabled": bool(self._expected_node),
                "expected_node": self._expected_node,
                # Reassembly-bound trips (see _WF_MAX_INFLIGHT_SETS): chunks
                # dropped for an out-of-range seq, and in-flight sets dropped
                # because the table was at capacity. Non-zero means either a
                # misbehaving/garbled sender or an attempt to grow receiver
                # memory without bound.
                "rejected_seq": self._n_rejected_seq,
                "overflow_evicted": self._n_overflow_evicted}

    # ------------------------------------------------------------------
    # Lookup interface
    # ------------------------------------------------------------------

    def get_record(self, boot_id: int, event_id: int) -> Optional[WaveformRecord]:
        for rec in reversed(self._records):
            if rec.boot_id == boot_id and rec.event_id == event_id:
                return rec
        return None

    def latest_record(self) -> Optional[WaveformRecord]:
        return self._records[-1] if self._records else None

    def recent_records(self) -> List[WaveformRecord]:
        """All buffered records, newest last. The caller filters by recency/
        overlap — a burst of short ESP captures (e.g. washer fill pauses
        splitting one add-on event into several firmware events) means the
        newest record is often NOT the one that matches."""
        return list(self._records)

    def pop_record(self, boot_id: int, event_id: int) -> Optional[WaveformRecord]:
        for i in range(len(self._records) - 1, -1, -1):
            if self._records[i].boot_id == boot_id and self._records[i].event_id == event_id:
                return self._records.pop(i)
        return None
