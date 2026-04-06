# FILE: apps/kam/tasks.py
# PURPOSE: Celery beat tasks for automatic Google Sheet → PostgreSQL sync
# SETUP:
#   1. pip install celery redis
#   2. In settings.py add CELERY_BEAT_SCHEDULE (see below)
#   3. Run: celery -A bos_lakshya beat -l info
#          celery -A bos_lakshya worker -l info
#
# settings.py additions required:
#   CELERY_BEAT_SCHEDULE = {
#       "sync-google-sheet-to-db": {
#           "task": "apps.kam.tasks.sync_google_sheet_to_db",
#           "schedule": 1800,   # every 30 minutes
#       },
#   }

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    from celery import shared_task
    _CELERY_AVAILABLE = True
except ImportError:
    _CELERY_AVAILABLE = False

    # Stub decorator so the rest of the module doesn't crash
    def shared_task(fn=None, **kwargs):  # type: ignore
        if fn is not None:
            return fn
        def _wrap(f):
            return f
        return _wrap


@shared_task(
    name="apps.kam.tasks.sync_google_sheet_to_db",
    bind=True,
    max_retries=3,
    default_retry_delay=300,   # 5 minutes before retry
    soft_time_limit=600,       # 10 minute soft limit
    time_limit=720,            # 12 minute hard limit
    acks_late=True,            # Don't ack until task completes
)
def sync_google_sheet_to_db(self):
    """
    Periodic task: Syncs Google Sheet → PostgreSQL.
    Schedule: every 30 minutes via Celery beat.

    Error handling:
    - GoogleCredentialError → retries 3x then stops (credential issue needs human fix)
    - RuntimeError (missing env var) → retries 3x then stops
    - Any other exception → retries with backoff
    """
    try:
        from apps.kam.sheets import run_sync_now
        from apps.common.google_auth import GoogleCredentialError  # type: ignore
    except ImportError:
        from apps.kam.sheets import run_sync_now
        class GoogleCredentialError(Exception): pass  # type: ignore

    logger.info("KAM periodic sync starting")

    try:
        result = run_sync_now()
        logger.info("KAM periodic sync complete: %s", result.get("summary"))
        return {
            "status":  "ok",
            "summary": result.get("summary"),
            "counts": {
                "customers": result.get("customers_upserted", 0),
                "sales":     result.get("sales_upserted", 0),
                "leads":     result.get("leads_upserted", 0),
                "overdues":  result.get("overdues_upserted", 0),
                "skipped":   result.get("skipped", 0),
            },
        }

    except GoogleCredentialError as exc:
        logger.error("KAM sync: credential error (no retry useful): %s", exc)
        # Don't retry credential errors — they need manual fix
        return {"status": "credential_error", "error": str(exc)}

    except RuntimeError as exc:
        logger.error("KAM sync: config error: %s", exc)
        # Retry in case it's a transient env var issue
        raise self.retry(exc=exc)

    except Exception as exc:
        logger.exception("KAM sync: unexpected error")
        raise self.retry(exc=exc)


@shared_task(
    name="apps.kam.tasks.sync_single_section",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
)
def sync_single_section(self, section_key: str):
    """
    Sync a single section on demand (e.g. after Sheet update webhook).
    section_key: one of customers|sales_f|sheet1|frontend|enquiry_f|overdues
    """
    from apps.kam import sheets_adapter

    try:
        from apps.common.google_auth import GoogleCredentialError  # type: ignore
    except ImportError:
        class GoogleCredentialError(Exception): pass  # type: ignore

    logger.info("KAM single-section sync: %s", section_key)

    valid_sections = {"customers", "sales_f", "sheet1", "frontend", "enquiry_f", "overdues"}
    if section_key not in valid_sections:
        logger.error("Invalid section_key: %s", section_key)
        return {"status": "error", "error": f"Invalid section: {section_key}"}

    try:
        sheet_id    = sheets_adapter._require_env("KAM_SALES_SHEET_ID")
        service     = sheets_adapter.build_sheets_service()
        tab_mapping = sheets_adapter._load_kam_names_tab(service, sheet_id)
        db_lookup   = sheets_adapter._build_user_lookup()
        env_usermap = sheets_adapter._load_env_usermap()
        local_cache = {}

        fn = sheets_adapter._STEP_FN_MAP.get(section_key)
        if not fn:
            return {"status": "error", "error": f"No sync function for {section_key}"}

        stats = fn(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        logger.info("Section '%s' sync: %s", section_key, stats.as_message())
        return {"status": "ok", "section": section_key, "summary": stats.as_message()}

    except GoogleCredentialError as exc:
        logger.error("Credential error in section sync '%s': %s", section_key, exc)
        return {"status": "credential_error", "error": str(exc)}
    except Exception as exc:
        logger.exception("Error in section sync '%s'", section_key)
        raise self.retry(exc=exc)