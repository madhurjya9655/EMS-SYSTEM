/* ============================================================
   FILE: static/js/navigation.js
   BOS Lakshya ERP — Module Tab Navigation & Breadcrumb
   ============================================================ */

(function () {
  'use strict';

  // ── Module tab active state ───────────────────────────────
  // Marks the correct module tab as active based on current URL
  (function initModuleTabs() {
    const tabs = document.querySelectorAll('.module-tab[data-url]');
    if (!tabs.length) return;

    const currentPath = window.location.pathname;

    tabs.forEach(function (tab) {
      const tabUrl = tab.getAttribute('data-url');
      if (tabUrl && currentPath.startsWith(tabUrl)) {
        tab.classList.add('active');
      }
    });
  })();

  // ── Tooltip fallback for sidebar (non-CSS environments) ──
  (function initTooltips() {
    // Bootstrap tooltips (optional enhancement)
    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
      var tooltipEls = document.querySelectorAll('[title]');
      tooltipEls.forEach(function (el) {
        // Only init for sidebar icons
        if (el.classList.contains('sidebar-icon-btn')) {
          new bootstrap.Tooltip(el, {
            placement: 'right',
            trigger: 'hover',
          });
        }
      });
    }
  })();

  // ── Progress bars animate on page load ───────────────────
  document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('.progress-bar[data-pct]').forEach(function (bar) {
      var pct = parseFloat(bar.getAttribute('data-pct')) || 0;
      bar.style.width = '0%';
      setTimeout(function () {
        bar.style.transition = 'width .8s cubic-bezier(.4,0,.2,1)';
        bar.style.width = pct + '%';
      }, 200);
    });
  });

  // ── Back button helper ────────────────────────────────────
  var backBtns = document.querySelectorAll('[data-go-back]');
  backBtns.forEach(function (btn) {
    btn.addEventListener('click', function (e) {
      e.preventDefault();
      window.history.back();
    });
  });

})();