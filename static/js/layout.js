(function () {
  'use strict';

  var app     = document.getElementById('erpApp');
  var overlay = document.getElementById('sidebarOverlay');

  if (!app) return;

  /* ── CSRF fetch patch ── */
  (function () {
    function getCookie(n) {
      var m = document.cookie.match('(^|;)\\s*' + n + '\\s*=\\s*([^;]+)');
      return m ? decodeURIComponent(m.pop()) : null;
    }
    var of = window.fetch;
    window.fetch = function (i, o) {
      try {
        var u = typeof i === 'string' ? i : (i && i.url ? i.url : '');
        var same = !/^https?:\/\//i.test(u) || u.startsWith(location.origin);
        var m = (o && o.method) || 'GET';
        if (same && !/^(GET|HEAD|OPTIONS)$/i.test(m)) {
          o = o || {};
          o.headers = new Headers(o.headers || {});
          var t = getCookie('csrftoken');
          if (t) o.headers.set('X-CSRFToken', t);
        }
      } catch (_) {}
      return of(i, o);
    };
  })();

  /* ── Sub-panel management ── */
  var currentPanel = null;

  function openPanel(panelId) {
    document.querySelectorAll('.sub-panel-section').forEach(function (s) {
      s.classList.remove('active');
    });
    var sec = document.getElementById('panel-' + panelId);
    if (sec) sec.classList.add('active');
    currentPanel = panelId;
    app.classList.add('panel-open');
    if (overlay) overlay.setAttribute('aria-hidden', 'false');
    document.querySelectorAll('.rail-btn[data-panel]').forEach(function (b) {
      b.classList.toggle('active', b.getAttribute('data-panel') === panelId);
    });
  }

  function closePanel() {
    currentPanel = null;
    app.classList.remove('panel-open');
    if (overlay) overlay.setAttribute('aria-hidden', 'true');
    document.querySelectorAll('.rail-btn[data-panel]').forEach(function (b) {
      b.classList.toggle('active', b.dataset.wasActive === '1');
    });
  }

  function togglePanel(panelId) {
    if (app.classList.contains('panel-open') && currentPanel === panelId) {
      closePanel();
    } else {
      openPanel(panelId);
    }
  }

  document.querySelectorAll('.rail-btn[data-panel]').forEach(function (b) {
    b.dataset.wasActive = b.classList.contains('active') ? '1' : '0';
    b.addEventListener('click', function () {
      togglePanel(b.getAttribute('data-panel'));
    });
  });

  /* Close via X button */
  document.addEventListener('click', function (e) {
    if (e.target.closest('.sub-panel-close')) closePanel();
  });

  /* ── Mobile sidebar ── */
  function openMobileSidebar() {
    app.classList.add('mobile-open');
    if (overlay) {
      overlay.style.display = 'block';
      overlay.setAttribute('aria-hidden', 'false');
    }
  }

  function closeMobileSidebar() {
    app.classList.remove('mobile-open');
    if (overlay) {
      overlay.style.display = '';
      overlay.setAttribute('aria-hidden', 'true');
    }
  }

  /* Overlay click: close everything */
  if (overlay) {
    overlay.addEventListener('click', function () {
      closeMobileSidebar();
      closePanel();
    });
  }

  /* Hamburger toggle */
  var mobileToggle = document.getElementById('mobileToggle');
  if (mobileToggle) {
    mobileToggle.addEventListener('click', function () {
      if (app.classList.contains('mobile-open')) {
        closeMobileSidebar();
      } else {
        openMobileSidebar();
      }
    });
  }

  /* Escape key */
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      closePanel();
      closeMobileSidebar();
      closeProfile();
    }
  });

  /* Close mobile sidebar on resize to desktop */
  window.addEventListener('resize', function () {
    if (window.innerWidth > 992) {
      closeMobileSidebar();
    }
  });

  /* Auto-open panel for active sub-link */
  (function () {
    var activeLink = document.querySelector('.sub-panel-section .sub-link.active');
    if (activeLink) {
      var sec = activeLink.closest('.sub-panel-section');
      if (sec) openPanel(sec.id.replace('panel-', ''));
    }
  })();

  /* ── Profile dropdown ── */
  var profileBtn  = document.getElementById('profileBtn');
  var profileWrap = document.getElementById('profileWrap');

  function closeProfile() {
    if (!profileWrap) return;
    profileWrap.classList.remove('open');
    if (profileBtn) profileBtn.setAttribute('aria-expanded', 'false');
  }

  if (profileBtn && profileWrap) {
    profileBtn.addEventListener('click', function (e) {
      e.stopPropagation();
      var open = profileWrap.classList.toggle('open');
      profileBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
    document.addEventListener('click', function (e) {
      if (!profileWrap.contains(e.target)) closeProfile();
    });
  }

  /* ── Auto-dismiss alerts ── */
  document.querySelectorAll('.alert.fade.show').forEach(function (el) {
    setTimeout(function () {
      if (window.bootstrap && bootstrap.Alert) {
        bootstrap.Alert.getOrCreateInstance(el).close();
      } else {
        el.style.display = 'none';
      }
    }, 5000);
  });

})();