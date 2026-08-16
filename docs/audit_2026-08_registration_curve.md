# Audit §12.1 extract — oval-gear meter registration curve (pressure-witness inversion)

> **Provenance note (dev41).** The full 2026-08 raw-sensor audit report lives outside this
> repository (hand-written from the audit harness's checkpoints; re-runnable via
> `water_monitor/tools/audit/run_audit.py`). This extract exists so that in-repo citations of
> the "pressure-witness inversion" terminate at the analysis itself rather than at
> descriptions of a missing document. Content carried verbatim from the §12.1 numbers as
> shipped in `flow_integral.py` (dev38, commit 73e6881); if the extract and the full report
> ever disagree, the full report wins and this file must be corrected.

## Method

The pressure channel is an independent witness to the oval-gear flow meter — it shares none
of the meter's pulse mechanism, so it cannot inherit the meter's errors. The audit inverted
the pressure channel against metered volume across the **pre-pump eras** (n = **1,086**
events) to recover the meter's registration curve per flow band.

## Result — metered ÷ true ratio by band

| Band (L/min) | metered ÷ true | 95% CI | Reads |
|---|---|---|---|
| ≥ 8.0 | 0.999 | 0.971 – 1.030 | reference band |
| 4.0 – 8.0 | 0.941 | 0.908 – 0.986 | ~6% low |
| 2.5 – 4.0 | 0.904 | 0.826 – 0.958 | ~10% low |
| 1.5 – 2.5 | 0.732 | 0.669 – 0.821 | ~27% low |
| 1.0 – 1.5 | 0.59 | 0.316 – 1.767 | weak; n = 20 |

## Limits (as stated by the audit)

- The curve is **relative to the meter's own ≥ 8 L/min band** — a common-mode scale error is
  invisible to it. It is **not** an absolute anchor; absolute validation requires a low-flow
  reference test (throttled-valve bucket test) or a utility-register cumulative cross-check.
- Sub-1 L/min flow gets **no** correction: non-registration cannot be recovered by a ratio,
  and extrapolating 0.59 downward would be invention. Those draws stay governed by the
  below-meter-floor verdict.
- Registration error is flow-dependent: the independent bucket-test anchor at normal faucet
  flow does **not** cover the 1.5–4 L/min band where the under-registration finding applies.

## Where this is used

- `registration_curve` table (dev41, migration 20260807) — curve v1, status `unvalidated`.
- `events.registration_est_litres` — annotate-only estimate; **never** feeds
  `volume_litres`, `volume_litres_effective`, or any total.
- Anchor data for any future refit: `meter_anchor_points` / `utility_register_readings`.
