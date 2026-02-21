# FILE: apps/kam/sheets.py

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from django.utils import timezone

# IMPORTANT:
# This module is a stable entrypoint for the rest of the app (views/urls/etc.)
# while delegating the real work to sheets_adapter, which supports multiple tabs
# and flexible schemas.

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

    - If worksheet_name is provided, it's treated as an override for SALES tab only.
      (We set env vars for compatibility with existing code paths.)
    - Otherwise, tabs are resolved from env (supports AUTO mode + fallbacks).
    - Returns a dict suitable for UI banners/logging.
    """
    sheet_id = _require_env(SHEET_ID_ENV)

    # NOTE: This is process-global. Kept only for backwards compatibility.
    # If you expect high concurrency and want to avoid this, prefer configuring env vars
    # (KAM_TAB_SALES / KAM_TAB_SALES_FALLBACKS) instead of passing worksheet_name.
    if worksheet_name:
        os.environ["KAM_TAB_SALES"] = worksheet_name  # new canonical key
        os.environ["KAM_SALES_TAB"] = worksheet_name  # old key still recognized

    try:
        stats = sheets_adapter.run_sync_now()
    except GoogleCredentialError as e:
        # keep the same error type for callers that expect it
        raise GoogleCredentialError(str(e)) from e

    result: Dict[str, Any] = {
        "sheet_id": sheet_id,
        "tabs_used": sheets_adapter.resolve_tabs_for_logging(),
        "sections_enabled": sheets_adapter.resolve_sections(),
        "timestamp": timezone.now().isoformat(),
        "summary": stats.as_message(),
        # detailed counts (useful for UI/debug)
        "customers_upserted": stats.customers_upserted,
        "sales_upserted": stats.sales_upserted,
        "leads_upserted": stats.leads_upserted,
        "overdues_upserted": stats.overdues_upserted,
        "skipped": stats.skipped,
        "unknown_kam": stats.unknown_kam,
        "notes": stats.notes,
    }
    logger.info("KAM sync complete: %s", result)
    return result


def step_sync(intent, *args, **kwargs) -> Dict[str, Any]:
    """
    Backwards-compatible step sync entrypoint.

    Your views expect sheets.step_sync(...) to exist.
    Delegate to sheets_adapter.step_sync(intent) which manages paging cursors
    stored on the SyncIntent model.

    Signature kept permissive (*args/**kwargs) so older callers won't break.
    """
    return sheets_adapter.step_sync(intent)