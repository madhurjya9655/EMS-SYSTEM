# apps/tasks/management/commands/backfill_delays.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Tuple, Type
from datetime import timezone as dt_timezone  # <-- use Python's UTC (Django 5-safe)

import pytz
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation, HelpTicket

IST = pytz.timezone("Asia/Kolkata")


# ---------------------------
# Helpers
# ---------------------------
def _ensure_aware_ist(dt):
    """
    Ensure a datetime is timezone-aware.
    If naive, interpret it as IST wall-clock and localize.
    """
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return IST.localize(dt)


def _minutes_between_floor_nonneg(a, b) -> int:
    """
    Floor((a - b)/60s) but never negative.
    Both timestamps are coerced to aware and then compared in UTC.
    """
    if a is None or b is None:
        return 0
    # Django 5: there is no timezone.utc — use Python's datetime.timezone.utc
    a_utc = _ensure_aware_ist(a).astimezone(dt_timezone.utc)
    b_utc = _ensure_aware_ist(b).astimezone(dt_timezone.utc)
    seconds = (a_utc - b_utc).total_seconds()
    mins = math.floor(seconds / 60.0)
    return max(int(mins), 0)


@dataclass
class ModelSpec:
    name: str
    model: Type
    status_field: str
    done_status: str
    planned_attr: str
    completed_attr: str
    duration_field: str


CHECKLIST_SPEC = ModelSpec(
    name="Checklist",
    model=Checklist,
    status_field="status",
    done_status="Completed",
    planned_attr="planned_date",
    completed_attr="completed_at",
    duration_field="actual_duration_minutes",
)

DELEGATION_SPEC = ModelSpec(
    name="Delegation",
    model=Delegation,
    status_field="status",
    done_status="Completed",
    planned_attr="planned_date",
    completed_attr="completed_at",
    duration_field="actual_duration_minutes",
)

HELPTICKET_SPEC = ModelSpec(
    name="HelpTicket",
    model=HelpTicket,
    status_field="status",
    done_status="Closed",
    planned_attr="planned_date",
    completed_attr="resolved_at",
    duration_field="actual_duration_minutes",
)


# ---------------------------
# Core recompute/update
# ---------------------------
def _recompute_for_queryset(
    *,
    spec: ModelSpec,
    batch_size: int,
    commit: bool,
) -> Tuple[int, int]:
    """
    Returns (scanned_count, changed_count).

    Recomputes actual_duration_minutes = floor((completed_at - planned_date) / 60s), clamped to >= 0.
    Updates only rows where the value differs. Uses bulk_update; no signals/emails are triggered.
    """
    Model = spec.model
    scanned = 0
    changed = 0

    qs = (
        Model.objects.filter(**{spec.status_field: spec.done_status})
        .only("id", spec.planned_attr, spec.completed_attr, spec.duration_field)
        .order_by("id")
    )

    to_update_batch = []

    def _flush_batch():
        nonlocal changed
        if not to_update_batch:
            return
        if commit:
            with transaction.atomic():
                Model.objects.bulk_update(
                    to_update_batch, [spec.duration_field], batch_size=len(to_update_batch)
                )
        changed += len(to_update_batch)
        to_update_batch.clear()

    for obj in qs.iterator(chunk_size=batch_size):
        scanned += 1
        planned = getattr(obj, spec.planned_attr, None)
        completed = getattr(obj, spec.completed_attr, None)
        current_val = getattr(obj, spec.duration_field, 0) or 0

        new_val = _minutes_between_floor_nonneg(completed, planned)

        if int(new_val) != int(current_val):
            setattr(obj, spec.duration_field, int(new_val))
            to_update_batch.append(obj)

        if len(to_update_batch) >= batch_size:
            _flush_batch()

    _flush_batch()
    return scanned, changed


# ---------------------------
# Command
# ---------------------------
class Command(BaseCommand):
    help = (
        "Recompute persisted actual_duration_minutes for completed/closed tasks.\n"
        "Naive timestamps are treated as IST; comparisons are performed in UTC.\n"
        "DRY-RUN by default. Use --commit to persist."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--commit",
            action="store_true",
            help="Persist the recomputed durations (otherwise dry-run).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Batch size for scanning and bulk_update (default: 500).",
        )
        parser.add_argument(
            "--only",
            action="append",
            choices=["checklist", "delegation", "help_ticket"],
            help=(
                "Limit to specific model(s). Can be repeated. "
                "Choices: checklist, delegation, help_ticket"
            ),
        )

    def handle(self, *args, **opts):
        commit = bool(opts.get("commit"))
        batch_size = int(opts.get("batch_size") or 500)
        only: Iterable[str] | None = opts.get("only")

        self.stdout.write(self.style.SUCCESS("Starting backfill of actual_duration_minutes"))
        self.stdout.write(f"  Timezone base : {IST.zone}")
        self.stdout.write(f"  Batch size    : {batch_size}")
        self.stdout.write(f"  Mode          : {'COMMIT' if commit else 'DRY-RUN'}\n")

        plan = []
        want = {s.lower() for s in (only or [])}

        def _want(key: str) -> bool:
            return not want or key in want

        if _want("checklist"):
            plan.append(CHECKLIST_SPEC)
        if _want("delegation"):
            plan.append(DELEGATION_SPEC)
        if _want("help_ticket"):
            plan.append(HELPTICKET_SPEC)

        total_scanned = 0
        total_changed = 0

        for spec in plan:
            self.stdout.write(self.style.WARNING(f"[{spec.name}] scanning..."))
            scanned, changed = _recompute_for_queryset(
                spec=spec,
                batch_size=batch_size,
                commit=commit,
            )
            total_scanned += scanned
            total_changed += changed
            self.stdout.write(
                f"  {spec.name}: scanned={scanned:,} "
                f"{'updated' if commit else 'would_update'}={changed:,}"
            )

        self.stdout.write("")
        if commit:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Done. Scanned {total_scanned:,} row(s), updated {total_changed:,}."
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    f"Dry-run complete. Scanned {total_scanned:,} row(s), would update {total_changed:,}."
                )
            )
            self.stdout.write("Run again with --commit to persist changes.")
