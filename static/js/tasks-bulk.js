// -----------------------------------------------------------
// BOS Lakshya – Bulk tools & small UX utilities
// Vanilla JS, Bootstrap 5 friendly, accessible & resilient
// -----------------------------------------------------------

(function () {
  "use strict";

  const qs  = (sel, root = document) => root.querySelector(sel);
  const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------- 0) Small helpers ----------
  const throttle = (fn, ms = 200) => {
    let t = 0;
    return (...args) => {
      const now = Date.now();
      if (now - t >= ms) { t = now; fn(...args); }
    };
  };

  const setSelectedVisual = (cb) => {
    const row = cb.closest("tr");
    if (!row) return;
    row.classList.toggle("is-selected", cb.checked);
  };

  // ---------- 1) Global [data-confirm] guard ----------
  document.addEventListener("click", function (e) {
    const el = e.target.closest("[data-confirm]");
    if (!el) return;

    const msg = el.getAttribute("data-confirm");
    if (!msg) return;

    // If this is the primary bulk submit inside a [data-bulk] form, let that form confirm.
    const isBulkSubmit = el.matches("[data-bulk-submit]") && el.form && el.form.matches("[data-bulk]");
    if (isBulkSubmit) return;

    if (!window.confirm(msg)) {
      e.preventDefault();
      e.stopPropagation();
    }
  });

  // ---------- 2) FILTER panel chevron toggle (optional) ----------
  document.addEventListener("DOMContentLoaded", function () {
    const panel = qs("#filterPanel");
    const icon  = qs("[data-filter-toggle-icon]");
    if (!panel || !icon) return;

    const toMinus = () => { icon.classList.remove("fa-plus");  icon.classList.add("fa-minus"); };
    const toPlus  = () => { icon.classList.remove("fa-minus"); icon.classList.add("fa-plus");  };

    if (panel.classList.contains("show")) toMinus(); else toPlus();
    panel.addEventListener("shown.bs.collapse", toMinus);
    panel.addEventListener("hidden.bs.collapse", toPlus);
  });

  // ---------- 3) Bulk-form initializer ----------
  function initBulkForm(form) {
    if (form._bulkInit) return;        // idempotent
    form._bulkInit = true;

    const selectAll       = qs("[data-select-all]", form);
    const submitBtn       = qs("[data-bulk-submit]", form);
    const selectedCountEl = qs("[data-selected-count]", form) || qs("[data-selected-count]");

    // Only visible & enabled row checkboxes within this form.
    const getRowChecks = () =>
      qsa("[data-row-check]", form).filter(cb => {
        const row = cb.closest("tr");
        return !cb.disabled && (!row || !row.classList.contains("d-none"));
      });

    function updateState() {
      const checks   = getRowChecks();
      const selected = checks.filter(cb => cb.checked).length;
      const total    = checks.length;

      // Button state + counter
      if (submitBtn) submitBtn.disabled = selected === 0;
      if (selectedCountEl) selectedCountEl.textContent = String(selected);

      // Select-all state
      if (selectAll) {
        selectAll.indeterminate = selected > 0 && selected < total;
        selectAll.checked       = total > 0 && selected === total;
      }

      // Selected row highlight
      checks.forEach(setSelectedVisual);
    }

    // Toggle all
    if (selectAll) {
      selectAll.addEventListener("change", () => {
        getRowChecks().forEach(cb => { cb.checked = selectAll.checked; });
        updateState();
      });
    }

    // Per-row change updates state + highlight
    form.addEventListener("change", (e) => {
      if (e.target && e.target.matches("[data-row-check]")) {
        setSelectedVisual(e.target);
        updateState();
      }
    });

    // Shift-click range selection
    (function enableShiftRange() {
      let last = null;
      form.addEventListener("click", (e) => {
        const cb = e.target && e.target.matches("[data-row-check]") ? e.target : null;
        if (!cb) return;

        const checks = getRowChecks();
        if (e.shiftKey && last && last !== cb) {
          const start = checks.indexOf(last);
          const end   = checks.indexOf(cb);
          if (start !== -1 && end !== -1) {
            const [lo, hi] = start < end ? [start, end] : [end, start];
            const value = cb.checked;
            for (let i = lo; i <= hi; i++) {
              checks[i].checked = value;
              setSelectedVisual(checks[i]);
            }
          }
        }
        last = cb;
        updateState();
      });
    })();

    // Keyboard helpers: A = toggle all (when focused inside table), Esc = clear all
    form.addEventListener("keydown", (e) => {
      const tag = (e.target.tagName || "").toLowerCase();
      const inInput = ["input","textarea","select"].includes(tag);
      if (inInput) return;

      if (e.key.toLowerCase() === "a") {
        e.preventDefault();
        const all = getRowChecks();
        const anyUnchecked = all.some(cb => !cb.checked);
        all.forEach(cb => cb.checked = anyUnchecked);
        updateState();
      } else if (e.key === "Escape") {
        const all = getRowChecks();
        if (all.some(cb => cb.checked)) {
          all.forEach(cb => cb.checked = false);
          updateState();
        }
      }
    });

    // Confirm on submit + avoid double submit
    form.addEventListener("submit", (e) => {
      const anySelected = getRowChecks().some(cb => cb.checked);
      if (!anySelected) {
        e.preventDefault();
        return;
      }

      const msg = submitBtn && submitBtn.getAttribute("data-confirm");
      if (msg && !window.confirm(msg)) {
        e.preventDefault();
        return;
      }

      if (submitBtn) submitBtn.disabled = true;
    });

    // Initial paint
    updateState();

    // If rows are added/removed dynamically, keep state correct
    const mo = new MutationObserver(throttle(updateState, 120));
    mo.observe(form, { childList: true, subtree: true, attributes: true, attributeFilter: ["class", "disabled"] });
  }

  // ---------- 4) Boot (support multiple bulk forms per page) ----------
  function boot() { qsa("form[data-bulk]").forEach(initBulkForm); }
  document.addEventListener("DOMContentLoaded", boot);

  // In case content is swapped via HTMX/Turbo/partial reloads:
  document.addEventListener("htmx:afterSwap", boot);
  document.addEventListener("turbo:render", boot);
})();
