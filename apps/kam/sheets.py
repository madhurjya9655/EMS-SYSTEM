# FILE: apps/kam/sheets.py
# PURPOSE: Stable Google Sheet sync entrypoint; aligned with KAM dashboard data flow
# FIXED: Hard GoogleCredentialError import that crashed entire KAM app on startup

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from django.utils import timezone

# FIX: Soft import — if apps.common.google_auth doesn't exist, define locally.
# This prevents the entire KAM app from failing to load on import.
try:
    from apps.common.google_auth import GoogleCredentialError
except ImportError:
    class GoogleCredentialError(Exception):  # type: ignore[no-redef]
        pass

from . import sheets_adapter

logger = logging.getLogger(__name__)

SHEET_ID_ENV = "KAM_SALES_SHEET_ID"


def _require_env(name: str) -> str:
    val = (os.getenv(name) or "").strip()
    if not val:
        raise RuntimeError(
            f"Missing required environment variable: {name}\n"
            f"Set this in your Render environment variables or .env file."
        )
    return val


def run_sync_now(*, worksheet_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Sync entrypoint called by:
    - views.sync_now (manual manager trigger)
    - tasks.sync_google_sheet_to_db (Celery beat — every 30 min)
    - Management command: python manage.py sync_kam_sheets

    Returns a dict with summary info for UI banners.
    Never raises — callers handle GoogleCredentialError and RuntimeError.
    """
    sheet_id = _require_env(SHEET_ID_ENV)

    # Allow explicit tab name override (backwards compat)
    if worksheet_name:
        os.environ["KAM_TAB_SALES"] = worksheet_name
        os.environ["KAM_SALES_TAB"] = worksheet_name  # legacy key

    try:
        stats = sheets_adapter.run_sync_now()
    except GoogleCredentialError:
        raise
    except RuntimeError:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in run_sync_now")
        raise RuntimeError(f"Sync failed unexpectedly: {exc}") from exc

    result: Dict[str, Any] = {
        "sheet_id":           sheet_id,
        "tabs_used":          sheets_adapter.resolve_tabs_for_logging(),
        "sections_enabled":   sheets_adapter.resolve_sections(),
        "timestamp":          timezone.now().isoformat(),
        "summary":            stats.as_message(),
        "customers_upserted": stats.customers_upserted,
        "sales_upserted":     stats.sales_upserted,
        "leads_upserted":     stats.leads_upserted,
        "overdues_upserted":  stats.overdues_upserted,
        "skipped":            stats.skipped,
        "unknown_kam":        stats.unknown_kam,
        "notes":              stats.notes,
    }
    logger.info("KAM sync complete: %s", stats.as_message())
    return result


def step_sync(intent, *args, **kwargs) -> Dict[str, Any]:
    """
    Stepped sync for progressive UI (one section at a time).
    Called by views.sync_step with a SyncIntent instance.
    """
    return sheets_adapter.step_sync(intent)