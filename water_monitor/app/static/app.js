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
          const escapedReason = faultReason.replace(/'/g, "\\'");
          ctrl.innerHTML = `<button class="btn btn-secondary btn-fault-warn" onclick="valveOpenWithFaultWarning('${circuit}', '${escapedReason}')">Open Valve</button>`;
        } else {
          ctrl.innerHTML = `<button class="btn btn-primary" onclick="valveOpen('${circuit}')">Open Valve</button>`;
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

// ── Valve control ──────────────────────────────────────────────────
async function valveOpenWithFaultWarning(circuit, faultReason) {
  const reasonLine = faultReason
    ? `\n\nFault reason: "${faultReason}"`
    : '';
  const confirmed = confirm(
    `⚠ Safety fault is active on this circuit.${reasonLine}\n\n` +
    `Opening the valve before resolving the fault may be unsafe.\n\n` +
    `Reset the fault first, then open the valve — or confirm below to override.`
  );
  if (!confirmed) return;
  await valveOpen(circuit);
}

async function valveOpen(circuit) {
  if (!confirm(`Open ${circuit} valve?`)) return;
  const r = await post(`/device/valve/${circuit}/open`);
  if (r.data && r.data.status === "error") {
    toast("Could not open valve: " + (r.data.message || "unknown error"), 'error');
    return;
  }
  // Poll faster until the state settles
  let attempts = 0;
  const iv = setInterval(async () => {
    await fetchLiveState();
    const dot = document.getElementById(`valve-dot-${circuit}`);
    if (!dot || dot.classList.contains('valve-open') || ++attempts >= 15)
      clearInterval(iv);
  }, 2000);
}

async function valveClose(circuit) {
  if (!confirm(`Close ${circuit} valve?`)) return;
  const r = await post(`/device/valve/${circuit}/close`);
  if (r.data && r.data.status === "error") {
    toast("Could not close valve: " + (r.data.message || "unknown error"), 'error');
    return;
  }
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
