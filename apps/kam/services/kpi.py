from datetime import date
from django.db.models import Sum, Count, Q
from ..models import (InvoiceFact, CollectionTxn, VisitPlan, CallLog, LeadFact,
                      OverdueSnapshot, KpiSnapshotDaily, TargetHeader, TargetLine)

def week_id_for(d: date): return f"{d.isocalendar().year}-W{int(d.isocalendar().week):02d}"
def month_id_for(d: date): return f"{d.year}-{int(d.month):02d}"

def compute_kam_kpis_quick(user, asof_date: date):
    # light calc for demo (current week/month)
    iso = asof_date.isocalendar()
    week_start = asof_date - timedelta(days=iso.weekday-1) if hasattr(iso,'weekday') else asof_date
    week_end = week_start + timedelta(days=6)
    month_start = asof_date.replace(day=1)

    sales_mt = InvoiceFact.objects.filter(kam=user, invoice_date__range=(week_start, week_end)).aggregate(s=Sum("qty_mt"))["s"] or 0
    collection_amount = CollectionTxn.objects.filter(kam=user, txn_datetime__date__range=(week_start, week_end)).aggregate(s=Sum("amount"))["s"] or 0
    visits_planned = VisitPlan.objects.filter(kam=user, visit_date__range=(week_start, week_end)).count()
    visits_actual = VisitPlan.objects.filter(kam=user, visit_date__range=(week_start, week_end), actual__isnull=False).count()
    calls = CallLog.objects.filter(kam=user, call_datetime__date__range=(week_start, week_end)).count()
    leads_total_mt = LeadFact.objects.filter(kam=user, doe__range=(week_start, week_end)).aggregate(s=Sum("qty_mt"))["s"] or 0
    leads_won_mt = LeadFact.objects.filter(kam=user, status="WON", doe__range=(month_start, asof_date)).aggregate(s=Sum("qty_mt"))["s"] or 0
    nbd_won_count = LeadFact.objects.filter(kam=user, status="WON", doe__range=(month_start, asof_date)).count()

    od = OverdueSnapshot.objects.filter(snapshot_date=asof_date, customer__primary_kam=user)
    overdues = od.aggregate(s=Sum("overdue"))["s"] or 0
    exposure = od.aggregate(s=Sum("exposure"))["s"] or 0
    credit_limit = od.aggregate(s=Sum("customer__credit_limit"))["s"] or 0

    tmp = type("K", (), {})()
    for k,v in dict(sales_mt=sales_mt, collection_amount=collection_amount, visits_planned=visits_planned,
                    visits_actual=visits_actual, calls=calls, leads_total_mt=leads_total_mt, leads_won_mt=leads_won_mt,
                    nbd_won_count=nbd_won_count, overdue=overdues, exposure=exposure, credit_limit=credit_limit).items():
        setattr(tmp, k, v)
    return tmp
