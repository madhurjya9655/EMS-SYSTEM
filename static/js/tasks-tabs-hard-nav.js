/*
 * Force the three task-type tabs to use a normal full-page navigation.
 *
 * Why:
 * Some existing tab code may load a complete Django page inside the current
 * page. That can duplicate the base layout/sidebar and cause a large blank
 * area, shifted content, or broken widths until the browser is refreshed.
 *
 * This script makes these tabs use a normal browser navigation:
 * - Checklist Tasks
 * - Delegation Tasks
 * - Help Ticket Tasks
 *
 * Scope:
 * Navigation only.
 *
 * This file does NOT change:
 * - checklist logic
 * - delegation logic
 * - help ticket logic
 * - delete behavior
 * - recurrence behavior
 * - filters
 * - permissions
 * - CSV download
 * - database logic
 */

(function () {
  "use strict";

  var TASK_TAB_LABELS = {
    "checklist tasks": true,
    "delegation tasks": true,
    "help ticket tasks": true
  };

  /**
   * Return normalized visible text from an element.
   */
  function normalizedText(node) {
    if (!node) {
      return "";
    }

    return (node.textContent || "")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  }

  /**
   * Check whether the clicked anchor belongs to one of the task-type tabs.
   */
  function isTaskTypeTab(anchor) {
    if (!anchor || anchor.tagName !== "A") {
      return false;
    }

    var label = normalizedText(anchor);

    if (TASK_TAB_LABELS[label]) {
      return true;
    }

    var href = anchor.getAttribute("href") || "";

    return (
      href.indexOf("/tasks/checklist/") !== -1 ||
      href.indexOf("/tasks/delegation/") !== -1 ||
      href.indexOf("/tasks/help-ticket/") !== -1 ||
      href.indexOf("/tasks/help_ticket/") !== -1
    );
  }

  /**
   * Use capture mode so this runs before older tab/AJAX click handlers.
   */
  document.addEventListener(
    "click",
    function (event) {
      if (event.defaultPrevented) {
        return;
      }

      if (event.button !== 0) {
        return;
      }

      if (
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        event.altKey
      ) {
        return;
      }

      var anchor = event.target.closest
        ? event.target.closest("a")
        : null;

      if (!isTaskTypeTab(anchor)) {
        return;
      }

      var href = anchor.href;

      if (!href || href.charAt(0) === "#") {
        return;
      }

      event.preventDefault();
      event.stopImmediatePropagation();

      window.location.assign(href);
    },
    true
  );
})();