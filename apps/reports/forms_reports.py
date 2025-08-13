# apps/reports/forms_reports.py
"""
Compatibility shim for legacy imports.

Some parts of the project (older views/templates) may still import forms from
`apps.reports.forms_reports`. To prevent code duplication and drift, this
module re-exports the canonical forms from `apps.reports.forms`, and provides
a small legacy `WeeklyMISFilterForm` that extends the main filter with two
optional commitment fields.

- PCReportFilterForm          -> imported from .forms
- WeeklyMISCommitmentForm     -> imported from .forms
- WeeklyMISFilterForm         -> subclass with two extra optional fields
"""

from django import forms
from .forms import (
    PCReportFilterForm as _PCReportFilterForm,
    WeeklyMISCommitmentForm as _WeeklyMISCommitmentForm,
)


class PCReportFilterForm(_PCReportFilterForm):
    """
    Backward-compatible alias to the canonical filter form
    defined in `apps.reports.forms`.
    """
    pass


class WeeklyMISCommitmentForm(_WeeklyMISCommitmentForm):
    """
    Backward-compatible alias to the canonical commitment form
    defined in `apps.reports.forms`.
    """
    pass


class WeeklyMISFilterForm(PCReportFilterForm):
    """
    Legacy filter still expected by some templates.
    Behaves like PCReportFilterForm but exposes two optional commitment fields.
    """
    checklist_commitment = forms.IntegerField(
        required=False,
        min_value=0,
        label="This Week Commitment (Checklist)",
        widget=forms.NumberInput(attrs={"class": "form-control"}),
    )
    delegation_commitment = forms.IntegerField(
        required=False,
        min_value=0,
        label="This Week Commitment (Delegation)",
        widget=forms.NumberInput(attrs={"class": "form-control"}),
    )


__all__ = [
    "PCReportFilterForm",
    "WeeklyMISCommitmentForm",
    "WeeklyMISFilterForm",
]
