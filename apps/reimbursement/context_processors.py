# apps/reimbursement/context_processors.py
from __future__ import annotations

from typing import Dict

from django.core.cache import cache

from .models import ReimbursementLine


def _is_finance_user(request) -> bool:
    """
    Gate the expensive count to Finance/Admin users only.
    Falls back safely if perms are not configured.
    """
    user = getattr(request, "user", None)
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True

    # Try common finance permissions; ignore if they don't exist.
    has_perm = getattr(user, "has_perm", lambda *_: False)
    return any(
        has_perm(code)
        for code in (
            "reimbursement_finance_pending",
            "reimbursement_finance_review",
            "reimbursement_review_finance",
        )
    )


def finance_badges(request) -> Dict[str, int]:
    """
    Expose lightweight badge counts for Finance UI.

    FINANCE_REJECTED_RESUB_COUNT
      = number of INCLUDED bill lines that were previously rejected by Finance
        and have been corrected by the employee (EMPLOYEE_RESUBMITTED).
      This powers the red badge shown next to the "Rejected Bills" entry.

    Design:
    - Zero cost for non-finance users (skip DB).
    - Cached for 30 seconds to avoid redundant COUNTs on busy pages.
    - Fully defensive: never break page render on DB hiccups.
    """
    if not _is_finance_user(request):
        return {"FINANCE_REJECTED_RESUB_COUNT": 0}

    cache_key = "reimb.badge.rejected_resub.count"
    cached = cache.get(cache_key)
    if isinstance(cached, int):
        return {"FINANCE_REJECTED_RESUB_COUNT": cached}

    try:
        count = ReimbursementLine.objects.filter(
            status=ReimbursementLine.Status.INCLUDED,
            bill_status=ReimbursementLine.BillStatus.EMPLOYEE_RESUBMITTED,
        ).count()
    except Exception:
        count = 0

    # Small TTL to keep things fresh while reducing DB hits.
    cache.set(cache_key, count, timeout=30)

    return {"FINANCE_REJECTED_RESUB_COUNT": count}
