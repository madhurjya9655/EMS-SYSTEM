# apps/kam/analytics/services.py
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta

from django.apps import apps
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import Q, F
from django.utils import timezone

from apps.kam.models import (
    Customer,
    InvoiceFact,
    LeadFact,
    OverdueSnapshot,
    TargetSetting,
    TargetLine,
    TargetHeader,
    VisitPlan,
    VisitActual,
    CallLog,
    CollectionTxn,
    CollectionPlan,
)

User = get_user_model()


ZERO = Decimal("0")


def _dec(value) -> Decimal:
    if value is None:
        return ZERO
    try:
        return Decimal(value)
    except Exception:
        return ZERO


def _float(value) -> float:
    return float(_dec(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _int(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _pct(numerator, denominator) -> float:
    numerator = _dec(numerator)
    denominator = _dec(denominator)
    if denominator <= 0:
        return 0.0
    return _float((numerator / denominator) * Decimal("100"))


def _score(value) -> float:
    value = _dec(value)
    if value < 0:
        value = ZERO
    if value > 100:
        value = Decimal("100")
    return _float(value)


def _performance_status(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Average"
    return "Needs Improvement"


def _safe_profile_value(user, *names, default="-"):
    profile = getattr(user, "profile", None)
    for name in names:
        try:
            value = getattr(profile, name, None) if profile else None
            if value:
                return str(value)
        except Exception:
            pass
    return default


def _kam_display_name(user) -> str:
    full_name = (user.get_full_name() or "").strip()
    return full_name or user.username or user.email or f"User #{user.id}"


def _inclusive_end_date(end_dt) -> date:
    return (end_dt - timezone.timedelta(days=1)).date()


def _sales_qs(kam_id: int, start_dt, end_dt):
    return (
        InvoiceFact.objects
        .filter(
            kam_id=kam_id,
            invoice_date__gte=start_dt.date(),
            invoice_date__lt=end_dt.date(),
            source_tab="Sales (F)",
        )
    )


def _lead_qs(kam_id: int, start_dt, end_dt):
    return (
        LeadFact.objects
        .filter(
            kam_id=kam_id,
            doe__isnull=False,
            doe__gte=start_dt.date(),
            doe__lt=end_dt.date(),
        )
    )


def _visit_plan_qs(kam_id: int, start_dt, end_dt):
    return (
        VisitPlan.objects
        .filter(
            kam_id=kam_id,
            visit_date__gte=start_dt.date(),
            visit_date__lt=end_dt.date(),
        )
    )


def _visit_actual_qs(kam_id: int, start_dt, end_dt):
    return (
        VisitActual.objects
        .filter(
            plan__kam_id=kam_id,
            plan__visit_date__gte=start_dt.date(),
            plan__visit_date__lt=end_dt.date(),
        )
        .select_related("plan", "plan__customer")
    )


def _call_qs(kam_id: int, start_dt, end_dt):
    return (
        CallLog.objects
        .filter(
            kam_id=kam_id,
            call_datetime__gte=start_dt,
            call_datetime__lt=end_dt,
        )
    )


def _collection_txn_qs(kam_id: int, start_dt, end_dt):
    return (
        CollectionTxn.objects
        .filter(
            kam_id=kam_id,
            txn_datetime__gte=start_dt,
            txn_datetime__lt=end_dt,
        )
    )


def _collection_plan_qs(kam_id: int):
    return CollectionPlan.objects.filter(kam_id=kam_id)


def _latest_overdue_qs(kam_id: int):
    latest_date = (
        OverdueSnapshot.objects
        .filter(kam_id=kam_id)
        .aggregate(d=models.Max("snapshot_date"))
        .get("d")
    )

    if not latest_date:
        return OverdueSnapshot.objects.none(), None

    return (
        OverdueSnapshot.objects
        .filter(kam_id=kam_id, snapshot_date=latest_date)
        .select_related("customer")
    ), latest_date


def _target_for_window(kam_id: int, start_dt, end_dt) -> Dict:
    start_date = start_dt.date()
    end_date = _inclusive_end_date(end_dt)

    setting = (
        TargetSetting.objects
        .filter(kam_id=kam_id, from_date__lte=start_date, to_date__gte=end_date)
        .order_by("-created_at")
        .first()
    )

    if setting:
        return {
            "sales_target_mt": _dec(setting.sales_target_mt),
            "leads_target_mt": _dec(setting.leads_target_mt),
            "collections_target_amount": _dec(setting.collections_target_amount),
            "calls_target": _int(setting.calls_target),
            "visits_target": 0,
            "source": "TargetSetting",
        }

    month_period = start_date.strftime("%Y-%m")
    line = (
        TargetLine.objects
        .filter(
            kam_id=kam_id,
            header__period_type=TargetHeader.PERIOD_MONTH,
            header__period_id=month_period,
        )
        .select_related("header")
        .first()
    )

    if line:
        return {
            "sales_target_mt": _dec(line.sales_target_mt),
            "leads_target_mt": _dec(line.leads_target_mt),
            "collections_target_amount": _dec(line.collections_plan_amount),
            "calls_target": _int(line.calls_target),
            "visits_target": _int(line.visits_target),
            "source": "TargetLine",
        }

    return {
        "sales_target_mt": ZERO,
        "leads_target_mt": ZERO,
        "collections_target_amount": ZERO,
        "calls_target": 0,
        "visits_target": 0,
        "source": "None",
    }


def _basic_info(kam) -> Dict:
    manager_name = "-"

    try:
        mapping = (
            kam.kam_manager_mappings
            .filter(active=True)
            .select_related("manager")
            .order_by("-assigned_at")
            .first()
        )
        if mapping:
            manager_name = _kam_display_name(mapping.manager)
    except Exception:
        pass

    return {
        "id": kam.id,
        "name": _kam_display_name(kam),
        "username": kam.username,
        "email": kam.email or "-",
        "department": _safe_profile_value(kam, "department", "department_name"),
        "manager": manager_name,
        "joining_date": _safe_profile_value(kam, "joining_date", "date_of_joining", "doj"),
        "region": _safe_profile_value(kam, "region", "territory", "zone", "location"),
    }


def _sales_metrics(kam_id: int, start_dt, end_dt, targets: Dict) -> Dict:
    sales = _sales_qs(kam_id, start_dt, end_dt)

    agg = sales.aggregate(
        total_sales_mt=models.Sum("qty_mt"),
        total_value=models.Sum("invoice_value"),
        invoices=models.Count("id"),
        customers=models.Count("customer_id", distinct=True),
    )

    lead_qty = _lead_qs(kam_id, start_dt, end_dt).aggregate(q=models.Sum("qty_mt")).get("q")

    total_sales_mt = _dec(agg.get("total_sales_mt"))
    target_mt = _dec(targets.get("sales_target_mt"))

    return {
        "total_sales_mt": _float(total_sales_mt),
        "won_mt": _float(total_sales_mt),
        "sales_value": _float(agg.get("total_value")),
        "invoice_count": _int(agg.get("invoices")),
        "customer_count": _int(agg.get("customers")),
        "target_mt": _float(target_mt),
        "achievement_pct": _pct(total_sales_mt, target_mt),
        "conversion_pct": _pct(total_sales_mt, lead_qty),
    }


def _lead_metrics(kam_id: int, start_dt, end_dt) -> Dict:
    leads = _lead_qs(kam_id, start_dt, end_dt)

    agg = leads.aggregate(
        total=models.Count("id"),
        converted=models.Count("id", filter=Q(status="WON")),
        pending=models.Count("id", filter=Q(status__in=["OPEN", "NEGOTIATION"])),
        lost=models.Count("id", filter=Q(status="LOST")),
        total_qty=models.Sum("qty_mt"),
        converted_qty=models.Sum("qty_mt", filter=Q(status="WON")),
    )

    total = _int(agg.get("total"))
    converted = _int(agg.get("converted"))

    return {
        "total_leads": total,
        "converted_leads": converted,
        "pending_leads": _int(agg.get("pending")),
        "lost_leads": _int(agg.get("lost")),
        "total_qty_mt": _float(agg.get("total_qty")),
        "converted_qty_mt": _float(agg.get("converted_qty")),
        "conversion_ratio": _pct(converted, total),
    }


def _visit_metrics(kam_id: int, start_dt, end_dt, targets: Dict) -> Dict:
    plans = _visit_plan_qs(kam_id, start_dt, end_dt)
    actuals = _visit_actual_qs(kam_id, start_dt, end_dt)

    planned = plans.count()
    actual = actuals.count()

    successful = actuals.filter(successful=True).count()

    today = timezone.localdate()

    missed = (
        plans
        .filter(visit_date__lt=today)
        .exclude(actual__isnull=False)
        .count()
    )

    on_time = (
        plans
        .filter(actual__isnull=False)
        .filter(
            Q(visit_date_to__isnull=True, actual__actual_datetime__date__lte=F("visit_date"))
            |
            Q(visit_date_to__isnull=False, actual__actual_datetime__date__lte=F("visit_date_to"))
        )
        .count()
    )

    visited_customers = (
        actuals
        .exclude(plan__customer_id__isnull=True)
        .values("plan__customer_id")
        .distinct()
        .count()
    )

    assigned_customers = (
        Customer.objects
        .filter(Q(kam_id=kam_id) | Q(primary_kam_id=kam_id))
        .distinct()
        .count()
    )

    visits_target = _int(targets.get("visits_target")) or planned

    return {
        "planned_visits": planned,
        "actual_visits": actual,
        "successful_visits": successful,
        "visit_success_pct": _pct(successful, actual),
        "missed_visits": missed,
        "on_time_visits": on_time,
        "on_time_visit_pct": _pct(on_time, actual),
        "customer_coverage": _pct(visited_customers, assigned_customers),
        "visited_customers": visited_customers,
        "assigned_customers": assigned_customers,
        "target_visits": visits_target,
        "achievement_pct": _pct(actual, visits_target),
    }


def _call_metrics(kam_id: int, start_dt, end_dt, targets: Dict) -> Dict:
    calls = _call_qs(kam_id, start_dt, end_dt)

    total = calls.count()

    productive = (
        calls
        .filter(
            Q(outcome__icontains="productive")
            | Q(outcome__icontains="positive")
            | Q(outcome__icontains="converted")
            | Q(outcome__icontains="won")
            | Q(outcome__icontains="success")
            | Q(duration_minutes__gt=0)
        )
        .count()
    )

    followups = (
        calls
        .filter(
            Q(outcome__icontains="follow")
            | Q(notes__icontains="follow")
            | Q(summary__icontains="follow")
        )
        .count()
    )

    called_customer_ids = list(
        calls
        .exclude(customer_id__isnull=True)
        .values_list("customer_id", flat=True)
        .distinct()
    )

    converted_from_calls = 0
    if called_customer_ids:
        converted_from_calls = (
            _sales_qs(kam_id, start_dt, end_dt)
            .filter(customer_id__in=called_customer_ids)
            .values("customer_id")
            .distinct()
            .count()
        )

    target_calls = _int(targets.get("calls_target"))

    return {
        "total_calls": total,
        "productive_calls": productive,
        "followups": followups,
        "converted_from_calls": converted_from_calls,
        "productive_pct": _pct(productive, total),
        "conversion_from_calls_pct": _pct(converted_from_calls, len(called_customer_ids)),
        "target_calls": target_calls,
        "achievement_pct": _pct(total, target_calls),
    }


def _collection_metrics(kam_id: int, start_dt, end_dt, targets: Dict) -> Dict:
    plans = _collection_plan_qs(kam_id)

    plan_agg = plans.aggregate(
        total_overdue=models.Sum("overdue_amount"),
        total_actual=models.Sum("actual_amount"),
        pending_count=models.Count("id", filter=Q(collection_status__in=[
            CollectionPlan.STATUS_OPEN,
            CollectionPlan.STATUS_PARTIAL,
        ])),
    )

    txn_agg = _collection_txn_qs(kam_id, start_dt, end_dt).aggregate(
        collected_in_range=models.Sum("amount"),
        txn_count=models.Count("id"),
    )

    total_overdue = _dec(plan_agg.get("total_overdue"))
    total_actual = _dec(plan_agg.get("total_actual"))
    pending_collection = total_overdue - total_actual
    if pending_collection < 0:
        pending_collection = ZERO

    target_amount = _dec(targets.get("collections_target_amount"))

    return {
        "total_overdue": _float(total_overdue),
        "total_collected": _float(total_actual),
        "collected_in_range": _float(txn_agg.get("collected_in_range")),
        "pending_collection": _float(pending_collection),
        "pending_count": _int(plan_agg.get("pending_count")),
        "collection_efficiency_pct": _pct(total_actual, total_overdue),
        "target_amount": _float(target_amount),
        "achievement_pct": _pct(total_actual, target_amount),
        "txn_count": _int(txn_agg.get("txn_count")),
    }


def _task_metrics(kam_id: int, start_dt, end_dt) -> Dict:
    """
    Production-safe adapter.

    If your project has a task model, connect it here after confirming actual model path/fields.

    This returns available=False instead of fake numbers when task module is not found.
    """

    candidate_models = [
        ("tasks", "Task"),
        ("task", "Task"),
        ("crm", "Task"),
        ("kam", "Task"),
    ]

    TaskModel = None

    for app_label, model_name in candidate_models:
        try:
            TaskModel = apps.get_model(app_label, model_name)
            if TaskModel:
                break
        except Exception:
            TaskModel = None

    if TaskModel is None:
        return {
            "available": False,
            "planned_tasks": None,
            "completed_tasks": None,
            "pending_tasks": None,
            "missed_tasks": None,
            "on_time_completion_pct": None,
            "achievement_pct": None,
        }

    fields = {f.name for f in TaskModel._meta.fields}

    owner_field = "kam" if "kam" in fields else "assigned_to" if "assigned_to" in fields else "user" if "user" in fields else None
    date_field = "due_date" if "due_date" in fields else "task_date" if "task_date" in fields else "created_at"
    completed_field = "completed" if "completed" in fields else "is_completed" if "is_completed" in fields else None
    completed_at_field = "completed_at" if "completed_at" in fields else None

    if not owner_field or not completed_field:
        return {
            "available": False,
            "planned_tasks": None,
            "completed_tasks": None,
            "pending_tasks": None,
            "missed_tasks": None,
            "on_time_completion_pct": None,
            "achievement_pct": None,
        }

    filters = {
        f"{owner_field}_id": kam_id,
        f"{date_field}__gte": start_dt.date() if date_field != "created_at" else start_dt,
        f"{date_field}__lt": end_dt.date() if date_field != "created_at" else end_dt,
    }

    qs = TaskModel.objects.filter(**filters)

    planned = qs.count()
    completed = qs.filter(**{completed_field: True}).count()
    pending = qs.filter(**{completed_field: False}).count()

    today = timezone.localdate()
    missed = (
        qs
        .filter(**{completed_field: False})
        .filter(**{f"{date_field}__lt": today})
        .count()
    )

    on_time = completed
    if completed_at_field and date_field != "created_at":
        on_time = (
            qs
            .filter(**{completed_field: True})
            .filter(**{f"{completed_at_field}__date__lte": F(date_field)})
            .count()
        )

    return {
        "available": True,
        "planned_tasks": planned,
        "completed_tasks": completed,
        "pending_tasks": pending,
        "missed_tasks": missed,
        "on_time_completion_pct": _pct(on_time, completed),
        "achievement_pct": _pct(completed, planned),
    }


def _risk_metrics(kam_id: int) -> Dict:
    overdue_qs, latest_date = _latest_overdue_qs(kam_id)

    agg = overdue_qs.aggregate(
        risk_customers=models.Count("customer_id", filter=Q(overdue__gt=0), distinct=True),
        exposure=models.Sum("exposure"),
        overdue=models.Sum("overdue"),
        delayed_collections=models.Sum("ageing_90_plus"),
    )

    assigned_credit_limit = (
        Customer.objects
        .filter(Q(kam_id=kam_id) | Q(primary_kam_id=kam_id))
        .aggregate(v=models.Sum("credit_limit"))
        .get("v")
    )

    exposure = _dec(agg.get("exposure"))

    return {
        "snapshot_date": latest_date.isoformat() if latest_date else None,
        "risk_customers": _int(agg.get("risk_customers")),
        "exposure": _float(exposure),
        "credit_limit": _float(assigned_credit_limit),
        "credit_limit_usage_pct": _pct(exposure, assigned_credit_limit),
        "delayed_collections": _float(agg.get("delayed_collections")),
        "total_overdue": _float(agg.get("overdue")),
    }


def _weekly_trend(kam_id: int, anchor_date: Optional[date] = None) -> List[Dict]:
    anchor_date = anchor_date or timezone.localdate()

    monday = anchor_date - timedelta(days=anchor_date.weekday())

    rows = []

    for i in range(5, -1, -1):
        week_start = monday - timedelta(days=i * 7)
        week_end_exclusive = week_start + timedelta(days=6)

        sales = (
            InvoiceFact.objects
            .filter(
                kam_id=kam_id,
                source_tab="Sales (F)",
                invoice_date__gte=week_start,
                invoice_date__lt=week_end_exclusive,
            )
            .aggregate(v=models.Sum("qty_mt"))
            .get("v")
        )

        collections = (
            CollectionTxn.objects
            .filter(
                kam_id=kam_id,
                txn_datetime__date__gte=week_start,
                txn_datetime__date__lt=week_end_exclusive,
            )
            .aggregate(v=models.Sum("amount"))
            .get("v")
        )

        visits = (
            VisitActual.objects
            .filter(
                plan__kam_id=kam_id,
                plan__visit_date__gte=week_start,
                plan__visit_date__lt=week_end_exclusive,
            )
            .count()
        )

        rows.append({
            "label": f"{week_start.strftime('%d %b')} - {(week_end_exclusive - timedelta(days=1)).strftime('%d %b')}",
            "sales_mt": _float(sales),
            "collections": _float(collections),
            "visits": visits,
        })

    return rows


def _monthly_trend(kam_id: int, anchor_date: Optional[date] = None) -> List[Dict]:
    anchor_date = anchor_date or timezone.localdate()
    first_this_month = anchor_date.replace(day=1)

    rows = []

    for i in range(5, -1, -1):
        month_start = first_this_month

        for _ in range(i):
            if month_start.month == 1:
                month_start = month_start.replace(year=month_start.year - 1, month=12)
            else:
                month_start = month_start.replace(month=month_start.month - 1)

        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1)

        sales = (
            InvoiceFact.objects
            .filter(
                kam_id=kam_id,
                source_tab="Sales (F)",
                invoice_date__gte=month_start,
                invoice_date__lt=month_end,
            )
            .aggregate(v=models.Sum("qty_mt"))
            .get("v")
        )

        leads_total = (
            LeadFact.objects
            .filter(
                kam_id=kam_id,
                doe__gte=month_start,
                doe__lt=month_end,
            )
            .count()
        )

        leads_won = (
            LeadFact.objects
            .filter(
                kam_id=kam_id,
                doe__gte=month_start,
                doe__lt=month_end,
                status="WON",
            )
            .count()
        )

        rows.append({
            "label": month_start.strftime("%b %Y"),
            "sales_mt": _float(sales),
            "leads_total": leads_total,
            "leads_won": leads_won,
            "lead_conversion_pct": _pct(leads_won, leads_total),
        })

    return rows


def _score_metrics(sales: Dict, visits: Dict, collections: Dict, tasks: Dict, calls: Dict) -> Dict:
    sales_score = _score(sales.get("achievement_pct"))
    visits_score = _score(visits.get("achievement_pct"))
    collections_score = _score(collections.get("collection_efficiency_pct"))
    calls_score = _score(calls.get("achievement_pct"))

    if tasks.get("available"):
        tasks_score = _score(tasks.get("achievement_pct"))
    else:
        tasks_score = 0.0

    overall = (
        Decimal(str(sales_score)) * Decimal("0.30")
        + Decimal(str(visits_score)) * Decimal("0.20")
        + Decimal(str(collections_score)) * Decimal("0.25")
        + Decimal(str(tasks_score)) * Decimal("0.15")
        + Decimal(str(calls_score)) * Decimal("0.10")
    )

    overall_score = _score(overall)

    return {
        "overall_score": overall_score,
        "status": _performance_status(overall_score),
        "weights": {
            "sales": 30,
            "visits": 20,
            "collections": 25,
            "tasks": 15,
            "calls": 10,
        },
        "component_scores": {
            "sales": sales_score,
            "visits": visits_score,
            "collections": collections_score,
            "tasks": tasks_score,
            "calls": calls_score,
        },
    }


def build_kam_performance_report(kam_id: int, start_dt, end_dt) -> Dict:
    kam = User.objects.get(id=kam_id, is_active=True)

    targets = _target_for_window(kam_id, start_dt, end_dt)

    sales = _sales_metrics(kam_id, start_dt, end_dt, targets)
    leads = _lead_metrics(kam_id, start_dt, end_dt)
    visits = _visit_metrics(kam_id, start_dt, end_dt, targets)
    calls = _call_metrics(kam_id, start_dt, end_dt, targets)
    collections = _collection_metrics(kam_id, start_dt, end_dt, targets)
    tasks = _task_metrics(kam_id, start_dt, end_dt)
    risk = _risk_metrics(kam_id)

    score = _score_metrics(
        sales=sales,
        visits=visits,
        collections=collections,
        tasks=tasks,
        calls=calls,
    )

    weekly_trend = _weekly_trend(kam_id, _inclusive_end_date(end_dt))
    monthly_trend = _monthly_trend(kam_id, _inclusive_end_date(end_dt))

    return {
        "basic": _basic_info(kam),
        "date_range": {
            "from": start_dt.date().isoformat(),
            "to": _inclusive_end_date(end_dt).isoformat(),
        },
        "targets": {
            "source": targets["source"],
            "sales_target_mt": _float(targets["sales_target_mt"]),
            "leads_target_mt": _float(targets["leads_target_mt"]),
            "collections_target_amount": _float(targets["collections_target_amount"]),
            "calls_target": _int(targets["calls_target"]),
            "visits_target": _int(targets["visits_target"]),
        },
        "sales": sales,
        "leads": leads,
        "visits": visits,
        "calls": calls,
        "collections": collections,
        "tasks": tasks,
        "risk": risk,
        "score": score,
        "charts": {
            "weekly_trend": weekly_trend,
            "monthly_trend": monthly_trend,
            "collection_trend": weekly_trend,
            "lead_conversion_trend": monthly_trend,
        },
    }