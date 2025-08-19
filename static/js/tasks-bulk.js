// E:\CLIENT PROJECT\employee management system bos\employee_management_system\static\js\tasks-bulk.js
// Unobtrusive bulk selection + counters + confirmations (Bootstrap 5, vanilla JS)

(function () {
  "use strict";

  const qs  = (sel, root = document) => root.querySelector(sel);
  const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------------------------------------------------------------------------
  // 1) Global [data-confirm] guard (works for links/buttons, incl. external form targets)
  // ---------------------------------------------------------------------------
  document.addEventListener("click", function (e) {
    const el = e.target.closest("[data-confirm]");
    if (!el) return;

    const msg = el.getAttribute("data-confirm");
    if (!msg) return;

    // If this is the primary bulk submit inside a [data-bulk] form, let the form's submit handler confirm.
    const isBulkSubmit = el.matches("[data-bulk-submit]") && el.form && el.form.matches("[data-bulk]");
    if (isBulkSubmit) return;

    if (!window.confirm(msg)) {
      e.preventDefault();
      e.stopPropagation();
    }
  });

  // ---------------------------------------------------------------------------
  // 2) FILTER panel chevron (+ / -) toggle (only if the page provides it)
  // ---------------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", function () {
    const panel = qs("#filterPanel");
    const icon  = qs("[data-filter-toggle-icon]");
    if (!panel || !icon) return;

    const toMinus = () => { icon.classList.remove("fa-plus");  icon.classList.add("fa-minus"); };
    const toPlus  = () => { icon.classList.remove("fa-minus"); icon.classList.add("fa-plus");  };

    // Initial
    if (panel.classList.contains("show")) toMinus(); else toPlus();

    panel.addEventListener("shown.bs.collapse", toMinus);
    panel.addEventListener("hidden.bs.collapse", toPlus);
  });

  // ---------------------------------------------------------------------------
  // 3) Bulk-form initializer (supports multiple forms per page)
  // ---------------------------------------------------------------------------
  function initBulkForm(form) {
    const selectAll        = qs("[data-select-all]", form);
    const submitBtn        = qs("[data-bulk-submit]", form);
    let   selectedCountEl  = qs("[data-selected-count]", form) || qs("[data-selected-count]");

    // Only consider visible & enabled row checkboxes within this form.
    const getRowChecks = () =>
      qsa("[data-row-check]", form).filter(cb => {
        const row = cb.closest("tr");
        return !cb.disabled && (!row || !row.classList.contains("d-none"));
      });

    function updateState() {
      const checks   = getRowChecks();
      const selected = checks.filter(cb => cb.checked).length;
      const total    = checks.length;

      if (submitBtn) submitBtn.disabled = selected === 0;
      if (selectedCountEl) selectedCountEl.textContent = String(selected);

      if (selectAll) {
        selectAll.indeterminate = selected > 0 && selected < total;
        selectAll.checked       = total > 0 && selected === total;
      }
    }

    // Select-all toggles all row checks in THIS form
    if (selectAll) {
      selectAll.addEventListener("change", () => {
        getRowChecks().forEach(cb => { cb.checked = selectAll.checked; });
        updateState();
      });
    }

    // Per-row change updates state
    form.addEventListener("change", (e) => {
      if (e.target && e.target.matches("[data-row-check]")) {
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
            for (let i = lo; i <= hi; i++) checks[i].checked = value;
          }
        }
        last = cb;
        updateState();
      });
    })();

    // Confirm on submit + prevent double submit
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

      // Avoid double submits
      if (submitBtn) submitBtn.disabled = true;
    });

    // Initial paint
    updateState();
  }

  // Boot
  document.addEventListener("DOMContentLoaded", function () {
    qsa("form[data-bulk]").forEach(initBulkForm);
  });
})();
