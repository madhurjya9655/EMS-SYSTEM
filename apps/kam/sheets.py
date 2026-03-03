# FILE: apps/kam/sheets.py
# PURPOSE: Stable Google Sheet sync entrypoint; aligned with KAM dashboard data flow
# UPDATED: 2026-03-03

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from django.utils import timezone

from apps.common.google_auth import GoogleCredentialError
from . import sheets_adapter

logger = logging.getLogger(__name__)

SHEET_ID_ENV = "KAM_SALES_SHEET_ID"


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def run_sync_now(*, worksheet_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Backwards-compatible sync entrypoint.

    - If worksheet_name is provided, it overrides KAM_TAB_SALES for this process.
      (Kept for compatibility with callers that pass an explicit tab name.)
    - Otherwise tabs are resolved from env vars (KAM_TAB_SALES, KAM_TAB_SHEET1, etc.)
    - Returns a dict suitable for UI banners and logging.
    """
    sheet_id = _require_env(SHEET_ID_ENV)

    # Override the sales tab env var when an explicit name is supplied.
    # NOTE: This is process-global. Prefer configuring KAM_TAB_SALES in env
    # rather than passing worksheet_name when running in a multi-threaded server.
    if worksheet_name:
        os.environ["KAM_TAB_SALES"] = worksheet_name   # new canonical key
        os.environ["KAM_SALES_TAB"] = worksheet_name   # legacy key, still recognised

    try:
        stats = sheets_adapter.run_sync_now()
    except GoogleCredentialError as exc:
        raise GoogleCredentialError(str(exc)) from exc

    result: Dict[str, Any] = {
        "sheet_id":          sheet_id,
        "tabs_used":         sheets_adapter.resolve_tabs_for_logging(),
        "sections_enabled":  sheets_adapter.resolve_sections(),
        "timestamp":         timezone.now().isoformat(),
        "summary":           stats.as_message(),
        # detailed counts (useful for UI / debug)
        "customers_upserted": stats.customers_upserted,
        "sales_upserted":     stats.sales_upserted,
        "leads_upserted":     stats.leads_upserted,
        "overdues_upserted":  stats.overdues_upserted,
        "skipped":            stats.skipped,
        "unknown_kam":        stats.unknown_kam,
        "notes":              stats.notes,
    }
    logger.info("KAM sync complete: %s", result)
    return result


def step_sync(intent, *args, **kwargs) -> Dict[str, Any]:
    """
    Backwards-compatible step sync entrypoint.

    Views call sheets.step_sync(intent). Delegates to
    sheets_adapter.step_sync(intent) which manages the paging cursor
    stored on the SyncIntent model.

    Signature kept permissive (*args/**kwargs) so older callers don't break.
    """
    return sheets_adapter.step_sync(intent)