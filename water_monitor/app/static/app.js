// Shared UI actions for Water Monitor add-on
const BASE = window.INGRESS_PATH || "";

async function post(url, body = {}) {
  const resp = await fetch(BASE + url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  let data = null;
  try { data = await resp.json(); } catch {}
  return { ok: resp.ok, status: resp.status, data };
}

// ── Toast notifications ────────────────────────────────────────────
function toast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.setAttribute('role', 'status');
  el.innerHTML =
    `<span class="toast-body">${message}</span>` +
    `<button class="toast-close" aria-label="Dismiss notification">×</button>`;
  const dismiss = () => {
    if (el._dismissed) return;
    el._dismissed = true;
    const reduced = !window.matchMedia('(prefers-reduced-motion: no-preference)').matches;
    if (!reduced) {
      el.classList.add('toast-out');
      setTimeout(() => el.remove(), 200);
    } else {
      el.remove();
    }
  };
  el.querySelector('.toast-close').addEventListener('click', dismiss);
  container.appendChild(el);
  if (type === 'success' || type === 'info') {
    setTimeout(dismiss, 4000);
  }
}

// ── Live state updater ─────────────────────────────────────────────
// Always-on poll — reflects valve changes from HA, the ESP firmware
// (faults, leak tests, bypass switch), or any other source.
// Runs every 5s. Also updated immediately after valve commands.

function applyLiveState(circuit, state) {
  // Valve indicator dot + label
  const dot   = document.getElementById(`valve-dot-${circuit}`);
  const label = document.getElementById(`valve-label-${circuit}`);
  if (dot && state.valve_state) {
    const vs = state.valve_state.toLowerCase();
    dot.className = dot.className.replace(/\bvalve-\S+/g, '').trim() + ` valve-${vs}`;
    if (label) label.textContent = vs.toUpperCase();

    // Keep the valve control button in sync
    const ctrl = document.getElementById(`valve-controls-${circuit}`);
    if (ctrl) {
      const faultActive = state.fault_active;
      const faultReason = state.fault_reason || '';
      ctrl.dataset.fault       = faultActive ? 'true' : 'false';
      ctrl.dataset.faultReason = faultReason;

      if (vs === 'open') {
        ctrl.innerHTML = `<button class="btn btn-danger" onclick="valveClose('${circuit}')">Close Valve</button>`;
      } else if (vs === 'closed') {
        if (faultActive) {
          ctrl.innerHTML = `<button class="btn btn-secondary btn-fault-warn" onclick="valveOpenWithFaultWarning('${circuit}', this)">Open Valve</button>`;
        } else {
          ctrl.innerHTML = `<button class="btn btn-primary" onclick="valveOpenModal('${circuit}', this)">Open Valve</button>`;
        }
      } else {
        ctrl.innerHTML = `<button class="btn btn-secondary" disabled>${state.valve_state}</button>`;
      }
    }
  }
  // Pressure + flow readings.
  // The API already applies unit conversion server-side — display as-is.
  const p = document.getElementById(`pressure-${circuit}`);
  const f = document.getElementById(`flow-${circuit}`);
  if (p && state.pressure) {
    const v = parseFloat(state.pressure);
    p.textContent = isNaN(v) ? state.pressure : v.toFixed(window.UNITS.pressure_decimals);
  }
  if (f && state.flow_rate !== undefined) {
    const v = parseFloat(state.flow_rate);
    f.textContent = isNaN(v) ? '0.00' : v.toFixed(window.UNITS.flow_decimals);
  }

  // Primary status line — derives from live state priority order
  const statusEl = document.getElementById(`status-line-${circuit}`);
  if (statusEl) {
    const training = state.training || {};
    const vs = (state.valve_state || '').toLowerCase();
    let statusClass, iconText, statusText;
    if (state.fault_active) {
      statusClass = 'dash-status-fault';
      iconText    = '●';
      statusText  = 'Fault · Valve closed · Manual reset required';
    } else if (state.trickle_active) {
      statusClass = 'dash-status-alert';
      iconText    = '●';
      statusText  = 'Attention needed · Trickle flow detected · Valve closed';
    } else if (vs !== 'open') {
      statusClass = 'dash-status-closed';
      iconText    = '○';
      statusText  = 'Valve closed · No active alerts';
    } else if (training.state === 'calibrating') {
      statusClass = 'dash-status-learning';
      iconText    = '●';
      statusText  = 'Learning · Valve open · Monitoring active';
    } else {
      statusClass = 'dash-status-normal';
      iconText    = '●';
      statusText  = 'Normal · Valve open · No active alerts';
    }
    statusEl.className = `dash-status-line ${statusClass}`;
    const icon = statusEl.querySelector('.dash-status-icon');
    const text = statusEl.querySelector('.dash-status-text');
    if (icon) icon.textContent = iconText;
    if (text) text.textContent = statusText;
  }

  // Leak test ETC — update data attrs if newly available, then restart countdown
  const etc = document.getElementById(`etc-${circuit}`);
  if (etc && state.leak_test_active && state.leak_test_started_at) {
    etc.dataset.start    = state.leak_test_started_at;
    etc.dataset.duration = state.leak_test_duration_secs || '';
    // Start the countdown if it isn't already ticking
    if (!etc._countdownRunning) startCountdownFor(etc);
  }
}

async function fetchLiveState() {
  try {
    const resp = await fetch(`${BASE}/api/dashboard/live`,
                             { signal: AbortSignal.timeout(5000) });
    if (!resp.ok) return;
    const data = await resp.json();
    for (const [circuit, state] of Object.entries(data)) {
      applyLiveState(circuit, state);
    }
  } catch (_) {}
}

// Start polling on page load — always runs
window.addEventListener('DOMContentLoaded', () => {
  fetchLiveState();                    // immediate first update
  setInterval(fetchLiveState, 5000);  // then every 5 seconds
});

// Modal event wiring (only runs if the modal scaffold is present in base.html)
window.addEventListener('DOMContentLoaded', () => {
  const overlay = document.getElementById('valve-modal');
  if (!overlay) return;

  document.getElementById('valve-modal-cancel').addEventListener('click', _closeValveModal);

  document.getElementById('valve-modal-confirm').addEventListener('click', () => {
    if (_valveModalOnConfirm) {
      const fn = _valveModalOnConfirm;
      _closeValveModal();
      fn();
    }
  });

  overlay.addEventListener('click', e => {
    if (e.target === overlay) _closeValveModal();
  });

  document.addEventListener('keydown', e => {
    if (overlay.style.display === 'none') return;
    if (e.key === 'Escape') {
      e.preventDefault();
      _closeValveModal();
      return;
    }
    if (e.key === 'Tab') {
      const focusable = [
        document.getElementById('valve-modal-cancel'),
        document.getElementById('valve-modal-confirm'),
      ];
      const first = focusable[0];
      const last  = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault(); last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault(); first.focus();
      }
    }
  });
});

// ── Valve modal ────────────────────────────────────────────────────
let _valveModalTrigger   = null;
let _valveModalOnConfirm = null;

function _openValveModal(titleText, bodyText, faultText) {
  document.getElementById('valve-modal-title').textContent = titleText;
  document.getElementById('valve-modal-body').textContent  = bodyText;
  const faultEl = document.getElementById('valve-modal-fault');
  if (faultText) {
    faultEl.textContent   = faultText;
    faultEl.style.display = '';
  } else {
    faultEl.style.display = 'none';
  }
  document.getElementById('valve-modal').style.display = '';
  document.getElementById('valve-modal-cancel').focus();
}

function _closeValveModal() {
  document.getElementById('valve-modal').style.display = 'none';
  _valveModalOnConfirm = null;
  if (_valveModalTrigger) {
    _valveModalTrigger.focus();
    _valveModalTrigger = null;
  }
}

function _getDisplayName(circuit) {
  const ctrl = document.getElementById(`valve-controls-${circuit}`);
  return (ctrl && ctrl.dataset.displayName) ? ctrl.dataset.displayName : circuit;
}

function _setValveBtnsLoading(circuit, loadingText) {
  const ctrl = document.getElementById(`valve-controls-${circuit}`);
  if (!ctrl) return;
  ctrl.querySelectorAll('button').forEach(b => {
    b.disabled = true;
    if (b.textContent.trim() === 'Open Valve' || b.textContent.trim() === 'Close Valve')
      b.textContent = loadingText;
  });
}

function _restoreValveBtns(circuit) {
  const ctrl = document.getElementById(`valve-controls-${circuit}`);
  if (!ctrl) return;
  ctrl.querySelectorAll('button').forEach(b => {
    b.disabled = false;
    if (b.textContent.trim() === 'Opening…') b.textContent = 'Open Valve';
    if (b.textContent.trim() === 'Closing…') b.textContent = 'Close Valve';
  });
}

// ── Valve control ──────────────────────────────────────────────────
function valveOpenModal(circuit, triggerEl) {
  _valveModalTrigger   = triggerEl || null;
  const displayName    = _getDisplayName(circuit);
  _valveModalOnConfirm = () => valveOpen(circuit);
  _openValveModal(
    `Open ${displayName} valve?`,
    'This may restore water flow. Confirm the leak condition has been resolved before opening.',
    null
  );
}

function valveOpenWithFaultWarning(circuit, triggerEl) {
  _valveModalTrigger  = triggerEl || null;
  const displayName   = _getDisplayName(circuit);
  const ctrl          = document.getElementById(`valve-controls-${circuit}`);
  const faultReason   = ctrl ? (ctrl.dataset.faultReason || '') : '';
  const faultLine     = faultReason
    ? `⚠ Fault reason: "${faultReason}". Opening before resolving the fault may be unsafe. Reset the fault first, or confirm to override.`
    : '⚠ Safety fault is active. Opening before resolving it may be unsafe. Reset the fault first, or confirm to override.';
  _valveModalOnConfirm = () => valveOpen(circuit);
  _openValveModal(
    `Open ${displayName} valve?`,
    'This may restore water flow. Confirm the leak condition has been resolved before opening.',
    faultLine
  );
}

async function valveOpen(circuit) {
  const displayName = _getDisplayName(circuit);
  _setValveBtnsLoading(circuit, 'Opening…');
  const r = await post(`/device/valve/${circuit}/open`);
  if (r.data && r.data.status === "error") {
    toast(`Could not open ${displayName} valve: ` + (r.data.message || "unknown error"), 'error');
    _restoreValveBtns(circuit);
    return;
  }
  toast(`Open command sent for ${displayName} valve.`, 'success');
  let attempts = 0;
  const iv = setInterval(async () => {
    await fetchLiveState();
    const dot = document.getElementById(`valve-dot-${circuit}`);
    if (!dot || dot.classList.contains('valve-open') || ++attempts >= 15)
      clearInterval(iv);
  }, 2000);
}

async function valveClose(circuit) {
  const displayName = _getDisplayName(circuit);
  toast(`Close command sent for ${displayName} valve.`, 'info');
  _setValveBtnsLoading(circuit, 'Closing…');
  const r = await post(`/device/valve/${circuit}/close`);
  if (r.data && r.data.status === "error") {
    toast(`Could not close ${displayName} valve: ` + (r.data.message || "unknown error"), 'error');
    _restoreValveBtns(circuit);
    return;
  }
  toast(`${displayName} valve closed.`, 'success');
  let attempts = 0;
  const iv = setInterval(async () => {
    await fetchLiveState();
    const dot = document.getElementById(`valve-dot-${circuit}`);
    if (!dot || dot.classList.contains('valve-closed') || ++attempts >= 15)
      clearInterval(iv);
  }, 2000);
}

// ── Fault resets ───────────────────────────────────────────────────
async function resetFault(circuit) {
  if (!confirm("Reset safety fault?")) return;
  await post(`/device/fault/${circuit}/reset`);
  setTimeout(() => location.reload(), 1000);
}

async function resetTrickle(circuit) {
  await post(`/device/trickle/${circuit}/reset`);
  setTimeout(() => location.reload(), 1000);
}

// ── Leak test ──────────────────────────────────────────────────────
async function runLeakTest(circuit) {
  if (!confirm(
    `Run micro leak test on ${circuit}?\n\n` +
    "The valve will close briefly to settle, then the test will run. " +
    "The valve reopens automatically when complete."
  )) return;
  const r = await post(`/device/leaktest/${circuit}/run`);
  const msg    = r.data && r.data.message ? r.data.message : "Leak test started.";
  const status = r.data && r.data.status  ? r.data.status  : "started";
  toast(msg, status === 'error' ? 'error' : 'info');
  if (status === "started") {
    // Reload once HA confirms the test is actually active (avoids stale render)
    _pollThenReload(circuit, s => s.leak_test_active || s.leak_test_running, 20);
  }
}

async function abortLeakTest(circuit) {
  if (!confirm(
    `Abort the leak test on ${circuit}?\n\n` +
    "The test will stop and the valve will reopen."
  )) return;
  const r = await post(`/device/leaktest/${circuit}/abort`);
  const msg = r.data && r.data.message ? r.data.message : "Leak test aborted.";
  toast(msg, r.ok ? 'info' : 'error');
  // Reload once HA confirms the test is no longer active
  _pollThenReload(circuit, s => !s.leak_test_active && !s.leak_test_running, 20);
}

// Poll /api/dashboard/live for one circuit until predicate(state) is true,
// then reload. Gives up after maxAttempts × 2s.
function _pollThenReload(circuit, predicate, maxAttempts) {
  let attempts = 0;
  const iv = setInterval(async () => {
    attempts++;
    try {
      const resp = await fetch(`${BASE}/api/dashboard/live`);
      if (resp.ok) {
        const data = await resp.json();
        const state = data[circuit];
        if (state && predicate(state)) {
          clearInterval(iv);
          location.reload();
          return;
        }
      }
    } catch (_) {}
    if (attempts >= maxAttempts) {
      clearInterval(iv);
      location.reload();
    }
  }, 2000);
}

// ── Leak test countdown ────────────────────────────────────────────
// data-start   : ISO timestamp when the leak test switch was turned ON
// data-duration: total seconds (60s settle + test duration in seconds)
const LEAK_TEST_SETTLE_SECS = 60;

function startCountdownFor(el) {
  const startStr  = el.dataset.start;
  const durStr    = el.dataset.duration;
  if (!startStr || !durStr) {
    el.textContent = 'Running…';
    return;
  }
  const startMs  = new Date(startStr).getTime();
  const totalMs  = parseFloat(durStr) * 1000;
  const settleMs = LEAK_TEST_SETTLE_SECS * 1000;
  const endMs    = startMs + totalMs;
  if (isNaN(startMs) || isNaN(totalMs)) {
    el.textContent = 'Running…';
    return;
  }

  // Mark this element as already ticking so applyLiveState doesn't restart it
  if (el._countdownRunning) return;
  el._countdownRunning = true;

  function tick() {
    if (!el._countdownRunning) return;   // stopped externally
    const now       = Date.now();
    const elapsedMs = now - startMs;
    const remaining = (endMs - now) / 1000;

    if (remaining <= 0) {
      el.textContent = 'Completing…';
      el._countdownRunning = false;
      setTimeout(() => location.reload(), 5000);
      return;
    }
    if (elapsedMs < settleMs) {
      const settleLeft = Math.ceil((settleMs - elapsedMs) / 1000);
      el.textContent = `Settling… ${settleLeft}s`;
    } else {
      const mins = Math.floor(remaining / 60);
      const secs = Math.floor(remaining % 60);
      el.textContent = mins > 0
        ? `${mins}m ${secs.toString().padStart(2, '0')}s remaining`
        : `${secs}s remaining`;
    }
    setTimeout(tick, 1000);
  }
  tick();
}

function startLeakTestCountdowns() {
  document.querySelectorAll('.leak-running-indicator[data-circuit]').forEach(el => {
    startCountdownFor(el);
  });
}

document.addEventListener('DOMContentLoaded', startLeakTestCountdowns);

// ── Connection status indicator ────────────────────────────────────
window.addEventListener("load", () => {
  const el = document.getElementById("conn-status");
  if (!el) return;
  const labelEl = el.querySelector(".conn-label");
  const setStatus = (state, label) => {
    // state: 'ok' | 'reconnecting' | 'offline'
    el.classList.remove("conn-status-ok", "conn-status-offline", "conn-status-reconnecting");
    el.classList.add("conn-status-" + state);
    if (labelEl) labelEl.textContent = label;
  };
  setInterval(async () => {
    try {
      const r = await fetch(BASE + "/health", { signal: AbortSignal.timeout(5000) });
      if (r.ok) setStatus("ok", "Connected");
      else      setStatus("reconnecting", "Reconnecting");
    } catch {
      setStatus("offline", "Offline");
    }
  }, 30000);
});
