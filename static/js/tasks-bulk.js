// static/js/tasks-bulk.js
// Unobtrusive bulk-selection + confirm handlers for Tasks pages (Bootstrap 5, vanilla JS)

(function () {
  "use strict";

  function qs(sel, root = document) { return root.querySelector(sel); }
  function qsa(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }

  // Attach a generic [data-confirm] click guard (works for buttons/links, incl. form="" targets)
  document.addEventListener("click", function (e) {
    const el = e.target.closest("[data-confirm]");
    if (!el) return;

    const msg = el.getAttribute("data-confirm");
    if (!msg) return;

    // If element is inside a [data-bulk] form and is the submit button,
    // we'll handle confirmation on 'submit'. For others, confirm here.
    const isBulkSubmit =
      el.matches("[data-bulk-submit]") &&
      el.form &&
      el.form.matches("[data-bulk]");

    if (isBulkSubmit) return; // defer to 'submit' handler

    if (!window.confirm(msg)) {
      e.preventDefault();
      e.stopPropagation();
    }
  });

  // Initialize each bulk form independently
  function initBulkForm(form) {
    const selectAll = qs("[data-select-all]", form);
    const submitBtn = qs("[data-bulk-submit]", form);
    const selectedCountEl = qs("[data-selected-count]", form);

    const getRowChecks = () => qsa("[data-row-check]", form).filter(cb => !cb.closest("tr")?.classList.contains("d-none"));

    function updateState() {
      const checks = getRowChecks();
      const enabled = checks.filter(cb => !cb.disabled);
      const selected = enabled.filter(cb => cb.checked).length;
      const total = enabled.length;

      if (submitBtn) submitBtn.disabled = selected === 0;
      if (selectedCountEl) selectedCountEl.textContent = String(selected);

      if (selectAll) {
        selectAll.indeterminate = selected > 0 && selected < total;
        selectAll.checked = total > 0 && selected === total;
      }
    }

    // Select-all toggles only checkboxes in this form
    if (selectAll) {
      selectAll.addEventListener("change", () => {
        const checks = getRowChecks();
        checks.forEach(cb => { if (!cb.disabled) cb.checked = selectAll.checked; });
        updateState();
      });
    }

    // Row checkbox changed
    form.addEventListener("change", (e) => {
      if (e.target && e.target.matches("[data-row-check]")) {
        updateState();
      }
    });

    // Optional: shift-click range selection
    (function enableShiftRange() {
      let lastClicked = null;
      form.addEventListener("click", (e) => {
        const target = e.target;
        if (!target || !target.matches("[data-row-check]")) return;

        const checks = getRowChecks();
        if (e.shiftKey && lastClicked && lastClicked !== target) {
          const start = checks.indexOf(lastClicked);
          const end = checks.indexOf(target);
          if (start !== -1 && end !== -1) {
            const [lo, hi] = start < end ? [start, end] : [end, start];
            const value = target.checked;
            for (let i = lo; i <= hi; i++) {
              if (!checks[i].disabled) checks[i].checked = value;
            }
          }
        }
        lastClicked = target;
        updateState();
      });
    })();

    // Confirm on submit + prevent double submit
    form.addEventListener("submit", (e) => {
      // Guard: require at least one selection for bulk actions
      const selected = getRowChecks().some(cb => cb.checked && !cb.disabled);
      if (!selected) {
        e.preventDefault();
        return;
      }

      const trigger = submitBtn; // primary bulk submit
      const msg = trigger && trigger.getAttribute("data-confirm");
      if (msg && !window.confirm(msg)) {
        e.preventDefault();
        return;
      }

      // Disable to avoid double-submits
      if (submitBtn) submitBtn.disabled = true;
    });

    // Initial state
    updateState();
  }

  document.addEventListener("DOMContentLoaded", function () {
    qsa("form[data-bulk]").forEach(initBulkForm);
  });
})();
