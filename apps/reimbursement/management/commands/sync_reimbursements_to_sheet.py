# apps/reimbursement/management/commands/sync_reimbursements_to_sheet.py
from __future__ import annotations

import sys
import time
from typing import Iterable, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db.models import QuerySet

from apps.reimbursement.models import ReimbursementRequest
from apps.reimbursement.integrations.sheets import sync_request


def batched(qs: QuerySet, *, batch_size: int = 200) -> Iterable[list[ReimbursementRequest]]:
    start = 0
    total = qs.count()
    while start < total:
        yield list(qs[start : start + batch_size])
        start += batch_size


class Command(BaseCommand):
    help = "Backfill all ReimbursementRequest rows to Google Sheet (idempotent upsert)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--all",
            action="store_true",
            help="Sync every ReimbursementRequest.",
        )
        parser.add_argument(
            "--id-gte", type=int, default=None, help="Sync requests with id >= this value."
        )
        parser.add_argument(
            "--id-lte", type=int, default=None, help="Sync requests with id <= this value."
        )
        parser.add_argument(
            "--sleep", type=float, default=0.0, help="Seconds to sleep between batches (optional)."
        )
        parser.add_argument(
            "--batch-size", type=int, default=200, help="Batch size (default 200)."
        )

    def handle(self, *args, **opts) -> None:
        all_flag: bool = opts["all"]
        id_gte: Optional[int] = opts["id_gte"]
        id_lte: Optional[int] = opts["id_lte"]
        sleep_s: float = float(opts["sleep"] or 0)
        batch_size: int = int(opts["batch_size"] or 200)

        if not (all_flag or id_gte is not None or id_lte is not None):
            self.stderr.write(
                self.style.ERROR("Provide --all or a range via --id-gte/--id-lte.")
            )
            sys.exit(2)

        qs = (
            ReimbursementRequest.objects
            .select_related("created_by", "manager", "management", "verified_by")
            .prefetch_related("lines__expense_item")
            .order_by("id")
        )
        if id_gte is not None:
            qs = qs.filter(id__gte=id_gte)
        if id_lte is not None:
            qs = qs.filter(id__lte=id_lte)

        total = qs.count()
        self.stdout.write(self.style.NOTICE(f"Syncing {total} requests... (idempotent)"))

        done = 0
        for chunk in batched(qs, batch_size=batch_size):
            for req in chunk:
                try:
                    sync_request(req)  # upsert: updates if exists, inserts otherwise
                except Exception as e:
                    self.stderr.write(self.style.WARNING(f"req={req.id} sync error: {e}"))
            done += len(chunk)
            self.stdout.write(self.style.SUCCESS(f"Progress: {done}/{total}"))
            if sleep_s > 0:
                time.sleep(sleep_s)

        self.stdout.write(self.style.SUCCESS("Backfill complete."))
