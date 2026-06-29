# FILE: apps/kam/tasks.py
# PURPOSE:
#   1. Celery tasks for automatic Google Sheet -> PostgreSQL sync.
#   2. Weekly consolidated KAM Performance Report email.
#   3. Optional monthly/manual KAM Performance Report tasks.
#
# PRODUCTION NOTES:
#   - Reuses existing sync service: apps.kam.sheets.run_sync_now
#   - Reuses existing analytics service: apps.kam.analytics.services.build_kam_performance_report
#   - Reuses existing email service: apps.kam.email.send_monthly_kam_performance_report_email
#   - Does not create duplicate KPI calculations.
#   - Does not create duplicate scheduler architecture.
#
# CELERY SETUP:
#   celery -A employee_management beat -l info
#   celery -A employee_management worker -l info
#
# REQUIRED CELERY BEAT SCHEDULE:
#
# In employee_management/settings.py:
#
# "weekly_kam_performance_report_monday_10am": {
#     "task": "apps.kam.tasks.send_weekly_kam_performance_report",
#     "schedule": crontab(hour=10, minute=0, day_of_week="1"),
#     "args": (),
# },
#
# day_of_week="1" = Monday in Celery crontab.
# TIME_ZONE / CELERY_TIMEZONE should be Asia/Kolkata.

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

try:
    from celery import shared_task

    _CELERY_AVAILABLE = True
except ImportError:
    _CELERY_AVAILABLE = False

    def shared_task(fn=None, **kwargs):  # type: ignore
        """
        Safe stub decorator.

        This prevents local/dev environments without Celery installed from crashing
        on import. In production, Celery should be installed and this stub is not used.
        """
        if fn is not None:
            return fn

        def _wrap(f):
            return f

        return _wrap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(value or 0)
    except Exception:
        return Decimal(0)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _report_get(report: dict, *path: str, default: Any = 0) -> Any:
    current: Any = report or {}

    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)

    if current is None:
        return default

    return current


def _display_number(value: Any, decimals: int = 2) -> str:
    number = _safe_float(value)

    if decimals <= 0:
        return f"{number:,.0f}"

    return f"{number:,.{decimals}f}"


def _display_money(value: Any) -> str:
    number = _safe_float(value)
    return f"Rs. {number:,.0f}"


def _display_pct(value: Any) -> str:
    number = _safe_float(value)
    return f"{number:,.1f}%"


def _display_mt(value: Any) -> str:
    number = _safe_float(value)
    return f"{number:,.2f} MT"


def _display_user_name(user) -> str:
    if not user:
        return "-"

    try:
        full_name = (user.get_full_name() or "").strip()
        if full_name:
            return full_name
    except Exception:
        pass

    username = (getattr(user, "username", "") or "").strip()
    if username:
        return username

    email = (getattr(user, "email", "") or "").strip()
    if email:
        return email

    return "-"


def _previous_month_bounds():
    """
    Returns timezone-aware datetime bounds for the previous calendar month.

    Used by optional monthly report task.
    """
    from django.utils import timezone

    now = timezone.localtime(timezone.now())
    first_day_current_month = now.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    if first_day_current_month.month == 1:
        previous_month_year = first_day_current_month.year - 1
        previous_month = 12
    else:
        previous_month_year = first_day_current_month.year
        previous_month = first_day_current_month.month - 1

    start_dt = first_day_current_month.replace(
        year=previous_month_year,
        month=previous_month,
        day=1,
    )
    end_dt = first_day_current_month

    return start_dt, end_dt


def _previous_week_bounds():
    """
    Returns timezone-aware datetime bounds for previous completed week.

    Weekly report schedule:
    - Runs Monday 10:00 AM IST.
    - Report period is previous Monday 00:00 to current Monday 00:00.
    - end_dt is exclusive.
    """
    from django.utils import timezone

    now = timezone.localtime(timezone.now())
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Python weekday: Monday = 0
    current_week_monday = today_start - timezone.timedelta(days=today_start.weekday())

    start_dt = current_week_monday - timezone.timedelta(days=7)
    end_dt = current_week_monday

    return start_dt, end_dt


def _period_label(*, start_dt, end_dt) -> str:
    """
    Build human-readable inclusive period label.
    """
    from django.utils import timezone

    return f"{start_dt.date()} to {(end_dt - timezone.timedelta(days=1)).date()}"


def _collect_active_kam_ids_for_period(*, start_dt, end_dt) -> list[int]:
    """
    Finds every KAM who has activity or target assignment.

    This keeps the email report comprehensive without hardcoding KAM names.
    """
    from django.contrib.auth import get_user_model

    from apps.kam.models import (
        CallLog,
        CollectionTxn,
        InvoiceFact,
        LeadFact,
        TargetSetting,
        VisitPlan,
    )

    User = get_user_model()

    start_date = start_dt.date()
    end_date = end_dt.date()

    kam_ids: set[int] = set()

    try:
        kam_ids.update(
            InvoiceFact.objects
            .filter(invoice_date__gte=start_date, invoice_date__lt=end_date)
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from InvoiceFact.")

    try:
        kam_ids.update(
            VisitPlan.objects
            .filter(visit_date__gte=start_date, visit_date__lt=end_date)
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from VisitPlan.")

    try:
        kam_ids.update(
            CallLog.objects
            .filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from CallLog.")

    try:
        kam_ids.update(
            CollectionTxn.objects
            .filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from CollectionTxn.")

    try:
        kam_ids.update(
            LeadFact.objects
            .filter(doe__gte=start_date, doe__lt=end_date)
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from LeadFact.")

    try:
        kam_ids.update(
            TargetSetting.objects
            .exclude(kam_id__isnull=True)
            .values_list("kam_id", flat=True)
            .distinct()
        )
    except Exception:
        logger.exception("Failed collecting KAM IDs from TargetSetting.")

    valid_ids = list(
        User.objects
        .filter(id__in=sorted(kam_ids), is_active=True)
        .exclude(is_superuser=True)
        .exclude(username__iexact="admin")
        .exclude(email__icontains="admin")
        .values_list("id", flat=True)
    )

    return valid_ids


def _build_email_context_from_reports(
    *,
    reports: list[dict],
    reporting_period: str,
    report_type: str = "Weekly",
) -> dict:
    """
    Converts existing build_kam_performance_report output into the email template
    context.

    Important:
    - No KPI recalculation here.
    - Values are read from the existing analytics service report payload.
    - Same template can be reused for weekly/monthly reports.
    """
    ranked_reports = sorted(
        reports,
        key=lambda report: _safe_float(_report_get(report, "score", "overall_score", default=0)),
        reverse=True,
    )

    summary_table: list[dict] = []
    kam_sections: list[dict] = []

    for rank, report in enumerate(ranked_reports, start=1):
        basic = report.get("basic") or {}
        sales = report.get("sales") or {}
        visits = report.get("visits") or {}
        calls = report.get("calls") or {}
        collections = report.get("collections") or {}
        leads = report.get("leads") or {}
        score = report.get("score") or {}
        risk = report.get("risk") or {}
        targets = report.get("targets") or {}

        kam_name = (
            basic.get("name")
            or basic.get("username")
            or basic.get("email")
            or "-"
        )

        sales_total = sales.get("total_sales_mt") or 0
        visits_actual = visits.get("actual_visits") or 0
        calls_total = calls.get("total_calls") or 0
        collections_total = collections.get("total_collected") or 0
        leads_total = leads.get("total_leads") or 0
        conversion_pct = leads.get("conversion_ratio") or sales.get("conversion_pct") or 0
        target_pct = sales.get("achievement_pct") or 0
        performance_pct = score.get("overall_score") or 0
        total_overdue = collections.get("total_overdue") or risk.get("delayed_collections") or 0
        risk_customers = risk.get("risk_customers") or 0

        summary_table.append({
            "rank": rank,
            "kam": kam_name,
            "sales": _display_mt(sales_total),
            "visits": _display_number(visits_actual, 0),
            "collections": _display_money(collections_total),
            "target_pct": _display_pct(target_pct),
            "performance_pct": _display_pct(performance_pct),
        })

        kam_sections.append({
            "name": kam_name,
            "designation": (
                basic.get("designation")
                or basic.get("department")
                or "-"
            ),
            "manager": basic.get("manager") or "-",
            "reporting_period": reporting_period,

            "sales": _display_mt(sales_total),
            "visits": _display_number(visits_actual, 0),
            "calls": _display_number(calls_total, 0),
            "collections": _display_money(collections_total),
            "leads": _display_number(leads_total, 0),
            "conversion": _display_pct(conversion_pct),
            "targets": (
                _display_mt(sales.get("target_mt") or targets.get("sales_target_mt") or 0)
            ),
            "achievement_pct": _display_pct(target_pct),
            "overdues": _display_money(total_overdue),
            "risk": _display_number(risk_customers, 0),
            "performance_pct": _display_pct(performance_pct),

            # Reserved for future inline chart images.
            # The email template already supports CID images.
            "chart_cids": [],
        })

    top_performer = summary_table[0]["kam"] if summary_table else "-"
    needs_improvement = summary_table[-1]["kam"] if summary_table else "-"

    management_summary = {
        "top_performer": top_performer,
        "needs_improvement": needs_improvement,
        "recommendations": (
            "Review low-performing KAMs for target achievement gap, pending visits, "
            "collection delays, overdue exposure, and lead conversion follow-up. "
            "Prioritise high-risk overdue customers and weekly target recovery actions."
        ),
    }

    return {
        "report_type": report_type,
        "reporting_period": reporting_period,
        "summary_table": summary_table,
        "kam_sections": kam_sections,
        "management_summary": management_summary,
    }


def _build_monthly_email_context_from_reports(
    *,
    reports: list[dict],
    reporting_period: str,
) -> dict:
    """
    Backward-compatible wrapper.

    Existing code may call this old helper name.
    """
    return _build_email_context_from_reports(
        reports=reports,
        reporting_period=reporting_period,
        report_type="Monthly",
    )


def _build_reports_for_kams(*, start_dt, end_dt) -> tuple[list[dict], list[dict]]:
    """
    Build existing performance reports for all active KAMs in the period.

    Returns:
    - reports
    - failed_kams
    """
    from django.contrib.auth import get_user_model

    from apps.kam.analytics.services import build_kam_performance_report

    User = get_user_model()

    kam_ids = _collect_active_kam_ids_for_period(
        start_dt=start_dt,
        end_dt=end_dt,
    )

    kams = (
        User.objects
        .filter(id__in=kam_ids, is_active=True)
        .exclude(is_superuser=True)
        .exclude(username__iexact="admin")
        .exclude(email__icontains="admin")
        .order_by("first_name", "last_name", "username")
    )

    reports: list[dict] = []
    failed_kams: list[dict] = []

    for kam in kams:
        try:
            report = build_kam_performance_report(
                kam_id=kam.id,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            reports.append(report)

        except Exception as exc:
            logger.exception(
                "Failed to build KAM report. kam_id=%s kam=%s",
                kam.id,
                _display_user_name(kam),
            )
            failed_kams.append({
                "id": kam.id,
                "name": _display_user_name(kam),
                "error": str(exc),
            })

    return reports, failed_kams


# ---------------------------------------------------------------------------
# Periodic Google Sheet -> PostgreSQL sync
# ---------------------------------------------------------------------------
@shared_task(
    name="apps.kam.tasks.sync_google_sheet_to_db",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=600,
    time_limit=720,
    acks_late=True,
)
def sync_google_sheet_to_db(self):
    """
    Periodic task: Syncs Google Sheet -> PostgreSQL.

    Schedule:
    - every 30 minutes via Celery beat.

    Error handling:
    - GoogleCredentialError: no retry useful; credentials need manual fix.
    - RuntimeError: retry because env/config may be transient during deployment.
    - Any other exception: retry with Celery retry policy.
    """
    try:
        from apps.kam.sheets import run_sync_now

        try:
            from apps.common.google_auth import GoogleCredentialError  # type: ignore
        except ImportError:
            class GoogleCredentialError(Exception):  # type: ignore
                pass

    except ImportError:
        from apps.kam.sheets import run_sync_now

        class GoogleCredentialError(Exception):  # type: ignore
            pass

    logger.info("KAM periodic sync starting")

    try:
        result = run_sync_now()

        logger.info("KAM periodic sync complete: %s", result.get("summary"))

        return {
            "status": "ok",
            "summary": result.get("summary"),
            "counts": {
                "customers": result.get("customers_upserted", 0),
                "sales": result.get("sales_upserted", 0),
                "leads": result.get("leads_upserted", 0),
                "overdues": result.get("overdues_upserted", 0),
                "skipped": result.get("skipped", 0),
            },
        }

    except GoogleCredentialError as exc:
        logger.error("KAM sync: credential error. Manual fix required: %s", exc)

        return {
            "status": "credential_error",
            "error": str(exc),
        }

    except RuntimeError as exc:
        logger.error("KAM sync: config/runtime error: %s", exc)
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
    Sync one KAM Google Sheet section on demand.

    Valid section_key:
    - customers
    - sales_f
    - sheet1
    - frontend
    - enquiry_f
    - overdues
    """
    from apps.kam import sheets_adapter

    try:
        from apps.common.google_auth import GoogleCredentialError  # type: ignore
    except ImportError:
        class GoogleCredentialError(Exception):  # type: ignore
            pass

    logger.info("KAM single-section sync starting: %s", section_key)

    valid_sections = {
        "customers",
        "sales_f",
        "sheet1",
        "frontend",
        "enquiry_f",
        "overdues",
    }

    if section_key not in valid_sections:
        logger.error("Invalid KAM section_key: %s", section_key)

        return {
            "status": "error",
            "error": f"Invalid section: {section_key}",
        }

    try:
        sheet_id = sheets_adapter._require_env("KAM_SALES_SHEET_ID")
        service = sheets_adapter.build_sheets_service()
        tab_mapping = sheets_adapter._load_kam_names_tab(service, sheet_id)
        db_lookup = sheets_adapter._build_user_lookup()
        env_usermap = sheets_adapter._load_env_usermap()
        local_cache = {}

        sync_fn = sheets_adapter._STEP_FN_MAP.get(section_key)

        if not sync_fn:
            return {
                "status": "error",
                "error": f"No sync function for {section_key}",
            }

        stats = sync_fn(
            service,
            sheet_id,
            tab_mapping,
            db_lookup,
            env_usermap,
            local_cache,
        )

        logger.info(
            "KAM section sync complete. section=%s summary=%s",
            section_key,
            stats.as_message(),
        )

        return {
            "status": "ok",
            "section": section_key,
            "summary": stats.as_message(),
        }

    except GoogleCredentialError as exc:
        logger.error(
            "Credential error in KAM section sync. section=%s error=%s",
            section_key,
            exc,
        )

        return {
            "status": "credential_error",
            "error": str(exc),
        }

    except Exception as exc:
        logger.exception("Error in KAM section sync. section=%s", section_key)
        raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# Weekly consolidated KAM Performance Report email
# ---------------------------------------------------------------------------
@shared_task(
    name="apps.kam.tasks.send_weekly_kam_performance_report",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=900,
    time_limit=1020,
    acks_late=True,
)
def send_weekly_kam_performance_report(self):
    """
    Sends the automatic consolidated weekly KAM Performance Report.

    Production schedule:
    - Every Monday
    - 10:00 AM IST
    - One consolidated email
    - Every active KAM with activity/targets included
    - TO: pankaj@blueoceansteels.com
    - CC: amreen@blueoceansteels.com

    Reporting period:
    - Previous Monday 00:00 to current Monday 00:00.
    """
    try:
        from apps.kam.email import send_monthly_kam_performance_report_email

        start_dt, end_dt = _previous_week_bounds()
        reporting_period = _period_label(start_dt=start_dt, end_dt=end_dt)

        logger.info(
            "Weekly KAM performance report starting. period=%s",
            reporting_period,
        )

        reports, failed_kams = _build_reports_for_kams(
            start_dt=start_dt,
            end_dt=end_dt,
        )

        email_context = _build_email_context_from_reports(
            reports=reports,
            reporting_period=reporting_period,
            report_type="Weekly",
        )

        sent_ok = send_monthly_kam_performance_report_email(
            reporting_period=email_context["reporting_period"],
            kam_sections=email_context["kam_sections"],
            summary_table=email_context["summary_table"],
            management_summary=email_context["management_summary"],
            chart_attachments=[],
        )

        logger.info(
            "Weekly KAM performance report complete. period=%s sent=%s kam_count=%s failed_kams=%s",
            reporting_period,
            sent_ok,
            len(email_context["kam_sections"]),
            len(failed_kams),
        )

        return {
            "status": "ok" if sent_ok else "email_failed",
            "report_type": "weekly",
            "reporting_period": reporting_period,
            "kam_count": len(email_context["kam_sections"]),
            "failed_kams": failed_kams,
            "to": ["pankaj@blueoceansteels.com"],
            "cc": ["amreen@blueoceansteels.com"],
        }

    except Exception as exc:
        logger.exception("Weekly KAM performance report task failed.")
        raise self.retry(exc=exc)


@shared_task(
    name="apps.kam.tasks.send_weekly_kam_performance_report_for_current_week",
    bind=True,
    max_retries=1,
    default_retry_delay=60,
    soft_time_limit=900,
    time_limit=1020,
)
def send_weekly_kam_performance_report_for_current_week(self):
    """
    Manual/debug helper task.

    Sends consolidated KAM report for current week-to-date.
    Useful for testing without waiting until Monday.

    Do not schedule this task in Celery beat.
    """
    try:
        from django.utils import timezone

        from apps.kam.email import send_monthly_kam_performance_report_email

        now = timezone.localtime(timezone.now())
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = today_start - timezone.timedelta(days=today_start.weekday())
        end_dt = now

        reporting_period = f"{start_dt.date()} to {end_dt.date()}"

        reports, failed_kams = _build_reports_for_kams(
            start_dt=start_dt,
            end_dt=end_dt,
        )

        email_context = _build_email_context_from_reports(
            reports=reports,
            reporting_period=reporting_period,
            report_type="Weekly",
        )

        sent_ok = send_monthly_kam_performance_report_email(
            reporting_period=email_context["reporting_period"],
            kam_sections=email_context["kam_sections"],
            summary_table=email_context["summary_table"],
            management_summary=email_context["management_summary"],
            chart_attachments=[],
        )

        return {
            "status": "ok" if sent_ok else "email_failed",
            "report_type": "weekly_current_week",
            "reporting_period": reporting_period,
            "kam_count": len(email_context["kam_sections"]),
            "failed_kams": failed_kams,
            "to": ["pankaj@blueoceansteels.com"],
            "cc": ["amreen@blueoceansteels.com"],
        }

    except Exception as exc:
        logger.exception("Manual current-week KAM performance report task failed.")
        raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# Optional monthly consolidated KAM Performance Report email
# ---------------------------------------------------------------------------
@shared_task(
    name="apps.kam.tasks.send_monthly_kam_performance_report",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=900,
    time_limit=1020,
    acks_late=True,
)
def send_monthly_kam_performance_report(self):
    """
    Optional monthly consolidated KAM Performance Report.

    Keep this task for manual/monthly use if needed.
    It is not required for Monday 10 AM weekly reporting unless scheduled.
    """
    try:
        from apps.kam.email import send_monthly_kam_performance_report_email

        start_dt, end_dt = _previous_month_bounds()
        reporting_period = _period_label(start_dt=start_dt, end_dt=end_dt)

        logger.info(
            "Monthly KAM performance report starting. period=%s",
            reporting_period,
        )

        reports, failed_kams = _build_reports_for_kams(
            start_dt=start_dt,
            end_dt=end_dt,
        )

        email_context = _build_email_context_from_reports(
            reports=reports,
            reporting_period=reporting_period,
            report_type="Monthly",
        )

        sent_ok = send_monthly_kam_performance_report_email(
            reporting_period=email_context["reporting_period"],
            kam_sections=email_context["kam_sections"],
            summary_table=email_context["summary_table"],
            management_summary=email_context["management_summary"],
            chart_attachments=[],
        )

        logger.info(
            "Monthly KAM performance report complete. period=%s sent=%s kam_count=%s failed_kams=%s",
            reporting_period,
            sent_ok,
            len(email_context["kam_sections"]),
            len(failed_kams),
        )

        return {
            "status": "ok" if sent_ok else "email_failed",
            "report_type": "monthly",
            "reporting_period": reporting_period,
            "kam_count": len(email_context["kam_sections"]),
            "failed_kams": failed_kams,
            "to": ["pankaj@blueoceansteels.com"],
            "cc": ["amreen@blueoceansteels.com"],
        }

    except Exception as exc:
        logger.exception("Monthly KAM performance report task failed.")
        raise self.retry(exc=exc)


@shared_task(
    name="apps.kam.tasks.send_monthly_kam_performance_report_for_current_month",
    bind=True,
    max_retries=1,
    default_retry_delay=60,
    soft_time_limit=900,
    time_limit=1020,
)
def send_monthly_kam_performance_report_for_current_month(self):
    """
    Manual/debug helper task.

    Sends the same consolidated email for the current month-to-date period.
    This is useful for production verification without waiting for next month.

    Do not schedule this task in Celery beat.
    """
    try:
        from django.utils import timezone

        from apps.kam.email import send_monthly_kam_performance_report_email

        now = timezone.localtime(timezone.now())
        start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = now

        reporting_period = f"{start_dt.date()} to {end_dt.date()}"

        reports, failed_kams = _build_reports_for_kams(
            start_dt=start_dt,
            end_dt=end_dt,
        )

        email_context = _build_email_context_from_reports(
            reports=reports,
            reporting_period=reporting_period,
            report_type="Monthly",
        )

        sent_ok = send_monthly_kam_performance_report_email(
            reporting_period=email_context["reporting_period"],
            kam_sections=email_context["kam_sections"],
            summary_table=email_context["summary_table"],
            management_summary=email_context["management_summary"],
            chart_attachments=[],
        )

        return {
            "status": "ok" if sent_ok else "email_failed",
            "report_type": "monthly_current_month",
            "reporting_period": reporting_period,
            "kam_count": len(email_context["kam_sections"]),
            "failed_kams": failed_kams,
            "to": ["pankaj@blueoceansteels.com"],
            "cc": ["amreen@blueoceansteels.com"],
        }

    except Exception as exc:
        logger.exception("Manual current-month KAM performance report task failed.")
        raise self.retry(exc=exc)