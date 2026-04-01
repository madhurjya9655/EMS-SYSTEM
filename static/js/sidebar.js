/* ============================================================
   FILE: static/js/sidebar.js
   BOS Lakshya ERP — Sidebar & Topbar Interactions
   ============================================================ */

(function () {
  'use strict';

  const app        = document.getElementById('erpApp');
  const sidebar    = document.getElementById('erpSidebar');
  const overlay    = document.getElementById('sidebarOverlay');
  const toggleBtn  = document.getElementById('sidebarToggle');

  // ── Mobile sidebar toggle ────────────────────────────────
  function openMobileSidebar() {
    if (app) app.classList.add('mobile-sidebar-open');
    if (overlay) overlay.setAttribute('aria-hidden', 'false');
  }

  function closeMobileSidebar() {
    if (app) app.classList.remove('mobile-sidebar-open');
    if (overlay) overlay.setAttribute('aria-hidden', 'true');
  }

  if (toggleBtn) {
    toggleBtn.addEventListener('click', function () {
      if (app && app.classList.contains('mobile-sidebar-open')) {
        closeMobileSidebar();
      } else {
        openMobileSidebar();
      }
    });
  }

  if (overlay) {
    overlay.addEventListener('click', closeMobileSidebar);
  }

  // Close sidebar on Escape
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      closeMobileSidebar();
      closeProfileDropdown();
    }
  });

  // ── Profile dropdown ──────────────────────────────────────
  const profileBtn  = document.getElementById('profileBtn');
  const profileWrap = document.getElementById('profileDropdownWrap') ||
                      profileBtn && profileBtn.closest('.profile-dropdown-wrap');

  function closeProfileDropdown() {
    if (profileWrap) {
      profileWrap.classList.remove('open');
      if (profileBtn) profileBtn.setAttribute('aria-expanded', 'false');
    }
  }

  if (profileBtn && profileWrap) {
    profileBtn.addEventListener('click', function (e) {
      e.stopPropagation();
      const isOpen = profileWrap.classList.toggle('open');
      profileBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    document.addEventListener('click', function (e) {
      if (!profileWrap.contains(e.target)) {
        closeProfileDropdown();
      }
    });
  }

  // ── Active sidebar link ───────────────────────────────────
  // Mark active icon based on current path
  (function markActiveLink() {
    const currentPath = window.location.pathname;
    const links = document.querySelectorAll('.sidebar-icon-btn');
    links.forEach(function (link) {
      if (link.getAttribute('href') && currentPath.startsWith(link.getAttribute('href')) && link.getAttribute('href') !== '/') {
        link.classList.add('active');
      }
    });
  })();

  // ── Auto-dismiss Bootstrap alerts ────────────────────────
  var alerts = document.querySelectorAll('.alert.fade.show');
  alerts.forEach(function (alertEl) {
    setTimeout(function () {
      if (alertEl && alertEl.classList.contains('show')) {
        var bsAlert = bootstrap && bootstrap.Alert ? bootstrap.Alert.getOrCreateInstance(alertEl) : null;
        if (bsAlert) bsAlert.close();
        else alertEl.style.display = 'none';
      }
    }, 5000);
  });

})();