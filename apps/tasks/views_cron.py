# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\views_cron.py
# apps/tasks/views_cron.py
from __future__ import annotations

import datetime
import logging
import threading
from datetime import timedelta
from typing import Any, Dict

import pytz
from django.conf import settings
from django.core.cache import cache
from django.http import JsonResponse, HttpResponseForbidden
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
logger = logging.getLogger(__name__)

# Weekly performance services
from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,
)

# Celery tasks and orchestrators
from apps.tasks.tasks import (
    generate_recurring_checklists,    # master-based recurring generator
    send_due_today_assignments,       # 10:00 IST fan-out (leave aware; centrally locked)
    pre10am_unblock_and_generate,     # 09:55 IST safeguard
)

from apps.tasks.services.holiday_guard import get_holiday_status
# Optional lightweight “today-only” materializer (safe, no emails)
try:
    from apps.tasks.materializer import materialize_today_for_all  # type: ignore
except Exception:  # pragma: no cover
    materialize_today_for_all = None  # type: ignore


IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))

# ✅ IMPORTANT FIX:
# The fanout pipeline can legitimately take longer than 180 seconds in production.
# If the lock expires too early, a second trigger can start another pipeline concurrently.
FANOUT_LOCK_TTL_SECONDS = int(getattr(settings, "DUE10_FANOUT_LOCK_TTL_SECONDS", 45 * 60))

# -----------------------------------------------------------------------------
# Auth helpers (accept old + new headers/params)
# -----------------------------------------------------------------------------
def _get_cron_token(request, token: str = "") -> str:
    """
    Accept token via:
      - explicit view 'token' argument (path converter if used)
      - headers: X-CRON-TOKEN, X-Cron-Key
      - querystring: ?token=..., ?key=...
    """
    return (
        token
        or request.headers.get("X-CRON-TOKEN", "")
        or request.headers.get("X-Cron-Key", "")
        or request.GET.get("token", "")
        or request.GET.get("key", "")
    )


def _cron_authorized(request, token: str = "") -> bool:
    expected = (getattr(settings, "CRON_SECRET", "") or "").strip()
    provided = (_get_cron_token(request, token) or "").strip()
    # If CRON_SECRET is empty, allow (useful for local/dev)
    return True if not expected else (provided == expected)


# -----------------------------------------------------------------------------
# Shared idempotency helpers (CANONICAL)
# -----------------------------------------------------------------------------
def _today_ist_date() -> str:
    return timezone.now().astimezone(IST).date().isoformat()


def _next_3am_ist_ttl_seconds() -> int:
    now_ist = timezone.now().astimezone(IST)
    next3 = (now_ist + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    return max(int((next3 - now_ist).total_seconds()), 60)


def _fanout_done_key(day_iso: str) -> str:
    return f"due10_fanout_done:{day_iso}"


def _fanout_lock_key(day_iso: str) -> str:
    return f"due10_fanout_lock:{day_iso}"


def _fanout_result_key(day_iso: str) -> str:
    return f"due10_fanout_result:{day_iso}"


def _acquire_fanout_lock(day_iso: str, seconds: int | None = None) -> bool:
    """
    Prevent concurrent duplicate runs (e.g., two endpoints triggered).
    Non-blocking: only the first caller acquires the lock.

    ✅ FIXED: lock TTL is long enough to cover real execution time.
    """
    ttl = int(seconds if seconds is not None else FANOUT_LOCK_TTL_SECONDS)
    return cache.add(_fanout_lock_key(day_iso), True, ttl)


def _mark_fanout_done(day_iso: str) -> None:
    cache.set(_fanout_done_key(day_iso), True, _next_3am_ist_ttl_seconds())


def _fanout_already_done(day_iso: str) -> bool:
    return bool(cache.get(_fanout_done_key(day_iso), False))


def _store_fanout_result(day_iso: str, payload: dict) -> None:
    # best-effort debug/observability for the day; expires by next 3AM IST window
    try:
        cache.set(_fanout_result_key(day_iso), payload, _next_3am_ist_ttl_seconds())
    except Exception:
        pass


def _release_fanout_lock(day_iso: str) -> None:
    try:
        cache.delete(_fanout_lock_key(day_iso))
    except Exception:
        pass


# -----------------------------------------------------------------------------
# CANONICAL 10:00 IST pipeline (single source of truth)
# -----------------------------------------------------------------------------
def _run_due_today_pipeline(day_iso: str) -> Dict[str, Any]:
    """
    Canonical pipeline for the 10:00 IST fan-out.

    Holiday Calendar is the master validation.

    On an official holiday or Sunday:
    - no checklist occurrence is materialized;
    - no recurring checklist is generated;
    - no due-today reminder email is sent;
    - no existing task is changed.

    Working-day order:
      0) Materialize missing today rows.
      1) Generate future recurring rows.
      2) Send due-today emails.
    """
    started_at = timezone.now().isoformat()
    holiday_status = get_holiday_status(timezone.now())

    payload: Dict[str, Any] = {
        "ok": True,
        "day": day_iso,
        "started_at": started_at,
        "materialized": None,
        "generated": None,
        "fanout": None,
        "errors": [],
    }

    try:
        if holiday_status["is_off_day"]:
            logger_message = (
                "Checklist generation skipped - Official Holiday | "
                f"Holiday detected: "
                f"{holiday_status['date'].isoformat()} | "
                f"Holiday Name: "
                f"{holiday_status['holiday_name'] or 'Sunday'} | "
                "Scheduler: _run_due_today_pipeline"
            )

            logger.info(logger_message)

            payload.update(
                {
                    "skipped": True,
                    "reason": holiday_status["reason"],
                    "holiday_name": holiday_status[
                        "holiday_name"
                    ],
                    "materialized": {
                        "created": 0,
                        "skipped": True,
                        "reason": holiday_status["reason"],
                    },
                    "generated": {
                        "checked": 0,
                        "created": 0,
                        "skipped": True,
                        "reason": holiday_status["reason"],
                    },
                    "fanout": {
                        "sent": 0,
                        "skipped": True,
                        "reason": holiday_status["reason"],
                    },
                }
            )

            try:
                _mark_fanout_done(day_iso)
            except Exception:
                pass

            return payload

        try:
            if callable(materialize_today_for_all):
                materialize_result = materialize_today_for_all(
                    dry_run=False
                )

                payload["materialized"] = getattr(
                    materialize_result,
                    "as_dict",
                    lambda: getattr(
                        materialize_result,
                        "__dict__",
                        {},
                    ),
                )()

            else:
                payload["materialized"] = {
                    "skipped": True,
                    "reason": "materializer_unavailable",
                }

        except Exception as exc:
            payload["materialized"] = {
                "ok": False,
                "error": str(exc),
            }
            payload["errors"].append(
                f"materialize:"
                f"{type(exc).__name__}:"
                f"{exc}"
            )

        try:
            generation_result = (
                generate_recurring_checklists.run(
                    dry_run=False
                )
            )
            payload["generated"] = generation_result

        except Exception as exc:
            payload["generated"] = {
                "ok": False,
                "error": str(exc),
            }
            payload["errors"].append(
                f"generate:"
                f"{type(exc).__name__}:"
                f"{exc}"
            )

        try:
            fanout_result = send_due_today_assignments.run()
            payload["fanout"] = fanout_result

        except Exception as exc:
            payload["fanout"] = {
                "ok": False,
                "error": str(exc),
            }
            payload["errors"].append(
                f"fanout:"
                f"{type(exc).__name__}:"
                f"{exc}"
            )

        try:
            _mark_fanout_done(day_iso)
        except Exception:
            pass

        return payload

    finally:
        payload["finished_at"] = (
            timezone.now().isoformat()
        )

        _store_fanout_result(
            day_iso,
            payload,
        )

        _release_fanout_lock(
            day_iso,
        )


def start_due_today_fanout(
    *,
    day_iso: str,
    background: bool = True,
) -> Dict[str, Any]:
    """
    Public trigger used by both cron entrypoints.

    Holiday Calendar is checked before:
    - acquiring a fan-out lock;
    - creating a background thread;
    - materializing checklist rows;
    - generating recurring checklist rows;
    - sending reminder emails.
    """
    holiday_status = get_holiday_status(
        timezone.now()
    )

    if holiday_status["is_off_day"]:
        logger.info(
            "Checklist generation skipped - Official Holiday | "
            "Holiday detected: %s | Holiday Name: %s | "
            "Scheduler: start_due_today_fanout",
            holiday_status["date"].isoformat(),
            holiday_status["holiday_name"] or "Sunday",
        )

        logger.info(
            "Checklist reminder skipped - Official Holiday | "
            "Holiday detected: %s | Holiday Name: %s | "
            "Scheduler: start_due_today_fanout",
            holiday_status["date"].isoformat(),
            holiday_status["holiday_name"] or "Sunday",
        )

        return {
            "ok": True,
            "accepted": False,
            "reason": holiday_status["reason"],
            "day": holiday_status["date"].isoformat(),
            "holiday_name": holiday_status["holiday_name"],
        }

    if not getattr(
        settings,
        "FEATURE_EMAIL_NOTIFICATIONS",
        True,
    ):
        return {
            "ok": True,
            "accepted": False,
            "reason": "feature_flag_off",
            "day": day_iso,
        }

    if _fanout_already_done(day_iso):
        return {
            "ok": True,
            "accepted": False,
            "reason": "already_done_today",
            "day": day_iso,
        }

    if not _acquire_fanout_lock(day_iso):
        return {
            "ok": True,
            "accepted": False,
            "reason": "already_running",
            "day": day_iso,
        }

    if not background:
        result = _run_due_today_pipeline(
            day_iso
        )

        return {
            "ok": True,
            "accepted": True,
            "day": day_iso,
            "mode": "inline",
            "result": result,
        }

    thread = threading.Thread(
        target=_run_due_today_pipeline,
        args=(day_iso,),
        daemon=True,
        name="cron-due-today-bg",
    )
    thread.start()

    return {
        "ok": True,
        "accepted": True,
        "day": day_iso,
        "mode": "background_thread",
        "note": (
            "Fan-out pipeline started in background "
            "(materialize -> generate -> send)."
        ),
    }


# -----------------------------------------------------------------------------
# Weekly congrats hook
# -----------------------------------------------------------------------------
@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    """
    1) Upsert WeeklyScore for last week (pure ORM; IST window; idempotent).
    2) If email feature is on, send congratulations mails (>= 90%) once per user/week.
    Always JSON (so cron logs are readable).
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        score_summary = upsert_weekly_scores_for_last_week()

        mail_summary = {}
        if getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            mail_summary = send_weekly_congratulations_mails() or {}

        payload = {
            "ok": True,
            "method": request.method,
            "at": datetime.datetime.utcnow().isoformat(),
            "scores": score_summary,
            "emails": mail_summary,
        }
        return JsonResponse(payload)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


# -----------------------------------------------------------------------------
# Due-today fan-out (10:00 IST) – canonical trigger endpoint
# -----------------------------------------------------------------------------
@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Canonical 10:00 IST fan-out trigger.
    Returns immediately (JSON) and performs heavy work in background thread.

    HARDENED:
      • Single pipeline owner (this file).
      • Other cron endpoint(s) must call start_due_today_fanout() instead of duplicating logic.
      • Shared idempotency keys: due10_fanout_done:<day>, due10_fanout_lock:<day>.
    """
    day_iso = _today_ist_date()

    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        res = start_due_today_fanout(day_iso=day_iso, background=True)
        res.update({"method": request.method, "at": datetime.datetime.utcnow().isoformat()})
        return JsonResponse(res, status=200)

    except Exception as e:
        _release_fanout_lock(day_iso)
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


# -----------------------------------------------------------------------------
# 09:55 IST safeguard
# -----------------------------------------------------------------------------
@csrf_exempt
@require_http_methods(["GET"])
def pre10am_unblock_and_generate_hook(request, token: str = ""):
    """
    09:55 IST safeguard:
      1) Complete overdue daily 'Pending' rows (yesterday or earlier)
      2) Generate 'future' recurring rows

    The pre10 orchestrator owns materialization and generation. This endpoint
    must not invoke the materializer separately, otherwise the same pipeline is
    executed twice.
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        uid_param = request.GET.get("user_id")
        try:
            uid = int(uid_param) if uid_param else None
        except Exception:
            uid = None

        result = pre10am_unblock_and_generate(user_id=uid)

        return JsonResponse(
            {
                "ok": True,
                "method": request.method,
                "at": timezone.now().isoformat(),
                **(result or {}),
            }
        )
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )