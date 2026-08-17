/* Waveform-chart decision logic — dev46 (46m).
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * These three functions used to live inline in history.html, mixed into a
 * ~2000-line template among DOM wiring and fetches. That is where the dev45
 * bug hid for weeks: the hi-res upgrade guard read
 *
 *     if (fMax.length <= flowPts.length) return;   // "no more detail"
 *
 * which predated the 32 -> 256-point signature widening. Afterwards the
 * envelope almost always had FEWER points than the signature, so the guard
 * fired nearly every time and the per-channel time axis — shipped in dev38 —
 * was unreachable in production for weeks. Nobody noticed because template JS
 * had no tests and the failure was silent: a chart still drew, just on the
 * wrong x-axis.
 *
 * The lesson is not "add a test for that line". It is that a DECISION worth
 * getting right should not be a clause buried in a callback. Each function
 * here is pure: inputs in, verdict out, no DOM, no fetch, no globals. That
 * makes them reviewable at a glance and testable from outside the browser.
 *
 * Loaded as a plain script (no bundler in this project) and exported on
 * window.WFAxis. Kept dependency-free on purpose — see the test file for why
 * a browser-based test is not yet feasible here.
 */
(function (root) {
  "use strict";

  /**
   * Should the hi-res envelope replace the already-drawn signature?
   *
   * The envelope's value is its REAL TIMESTAMPS, not its point count — that
   * is exactly what the dev45 guard got backwards. So: upgrade whenever the
   * envelope can draw an honest time axis, and fall back to the point-count
   * comparison only when it cannot.
   *
   * @param {number} envelopeLen  hi-res sample count
   * @param {number} signatureLen points already rendered from the signature
   * @param {boolean} hasTimes    envelope carries per-sample timestamps
   * @returns {boolean}
   */
  function shouldUpgradeToEnvelope(envelopeLen, signatureLen, hasTimes) {
    if (!(envelopeLen >= 2)) return false;        // nothing to draw
    if (hasTimes) return true;                    // honest axis always wins
    return envelopeLen > signatureLen;            // else: only if finer
  }

  /**
   * Does the payload carry a usable per-sample time axis?
   * One timestamp per sample, or it is not an axis.
   */
  function hasHonestTimes(times, sampleLen) {
    return Array.isArray(times) && times.length === sampleLen && sampleLen >= 2;
  }

  /**
   * Per-channel time axis for the SIGNATURE render, from the spans recorded
   * at capture (dev46 46i).
   *
   * Flow and pressure sample on independent callbacks and downsample
   * independently, so their captured spans genuinely differ. Without them
   * both channels are stretched to the same width on a shared index, which
   * mis-aligns a pressure dip against the flow that caused it.
   *
   * Returns null when the flow span is unusable — legacy rows then keep the
   * proportional overlay rather than being given a fabricated axis.
   *
   * @returns {{tf: number[], tp: (number[]|null)}|null}
   */
  function signatureTimeAxis(flowLen, pressLen, flowSpanS, pressSpanS) {
    var fOk = Number.isFinite(flowSpanS) && flowSpanS > 0 && flowLen > 1;
    if (!fOk) return null;
    var tf = [];
    for (var i = 0; i < flowLen; i++) tf.push((i / (flowLen - 1)) * flowSpanS);
    var tp = null;
    if (Number.isFinite(pressSpanS) && pressSpanS > 0 && pressLen > 1) {
      tp = [];
      for (var j = 0; j < pressLen; j++) {
        tp.push((j / (pressLen - 1)) * pressSpanS);
      }
    }
    return { tf: tf, tp: tp };
  }

  root.WFAxis = {
    shouldUpgradeToEnvelope: shouldUpgradeToEnvelope,
    hasHonestTimes: hasHonestTimes,
    signatureTimeAxis: signatureTimeAxis,
  };
})(typeof window !== "undefined" ? window : this);
