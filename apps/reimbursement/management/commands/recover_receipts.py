#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reimbursement\management\commands\recover_receipts.py
import csv
import os
import re
from datetime import datetime
from decimal import Decimal

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from apps.reimbursement.models import ReimbursementLine


KEYWORDS = {
    "hotel": ["hotel", "stay", "room"],
    "lunch": ["lunch"],
    "breakfast": ["breakfast", "break", "fast"],
    "dinner": ["dinner"],
    "tea": ["tea"],
    "coffee": ["coffee"],
    "cab": ["cab", "taxi", "uber", "ola", "auto", "rickshaw"],
    "flight": ["flight", "air", "airport"],
    "bus": ["bus"],
    "train": ["train"],
    "fuel": ["fuel", "petrol", "diesel"],
    "parking": ["parking"],
    "water": ["water"],
}


def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def amount_tokens(amount):
    d = Decimal(amount).quantize(Decimal("1"))
    return {str(int(d)), str(amount).replace(".00", "")}


def scan_files():
    root = os.path.join(settings.MEDIA_ROOT, "reimbursement", "receipts")
    out = []

    for base, _, names in os.walk(root):
        for name in names:
            full = os.path.join(base, name)
            rel = os.path.relpath(full, settings.MEDIA_ROOT).replace("\\", "/")
            try:
                st = os.stat(full)
            except OSError:
                continue

            folder = rel.split("/")
            folder_date = ""
            if len(folder) >= 5:
                y, m, d = folder[2], folder[3], folder[4]
                if y.isdigit() and m.isdigit() and d.isdigit():
                    folder_date = f"{y}-{m}-{d}"

            out.append({
                "rel": rel,
                "name": name,
                "text": norm(name),
                "folder_date": folder_date,
                "size": st.st_size,
            })

    return out


def keyword_hits(description, filename_text):
    desc = norm(description)
    hits = []
    for label, words in KEYWORDS.items():
        if any(w in desc for w in words) and any(w in filename_text for w in words):
            hits.append(label)
    return hits


def score_line_file(line, f):
    item = line.expense_item
    desc = f"{line.description or ''} {getattr(item, 'description', '') or ''}"
    expense_date = getattr(item, "date", None)

    score = 0
    reasons = []

    # Amount match
    for tok in amount_tokens(line.amount):
        if re.search(rf"(^|[^0-9]){re.escape(tok)}([^0-9]|$)", f["text"]):
            score += 55
            reasons.append(f"amount:{tok}")
            break

    # Date match: exact expense date equals folder date
    if expense_date and f["folder_date"]:
        if str(expense_date) == f["folder_date"]:
            score += 25
            reasons.append("exact_date")
        elif str(expense_date)[:7] == f["folder_date"][:7]:
            score += 10
            reasons.append("same_month")

    # Keyword match
    hits = keyword_hits(desc, f["text"])
    if hits:
        score += min(25, 10 * len(hits))
        reasons.append("keyword:" + ",".join(hits))

    # Request-level nearby folder boost
    submitted = getattr(line.request, "submitted_at", None)
    if submitted and f["folder_date"] and str(submitted.date())[:7] == f["folder_date"][:7]:
        score += 5
        reasons.append("request_month")

    return min(score, 100), "; ".join(reasons)


class Command(BaseCommand):
    help = "Recover missing ReimbursementLine.receipt_file from media using confidence scoring."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--id-gte", type=int)
        parser.add_argument("--id-lte", type=int)
        parser.add_argument("--min-confidence", type=int, default=90)
        parser.add_argument("--audit", default="receipt_recovery_audit.csv")
        parser.add_argument("--rollback")

    def handle(self, *args, **opts):
        if opts["rollback"]:
            return self.rollback(opts["rollback"])

        dry = opts["dry_run"]
        apply = opts["apply"]

        if not dry and not apply:
            self.stderr.write("Use --dry-run or --apply")
            return

        files = scan_files()
        self.stdout.write(f"Media files scanned: {len(files)}")

        qs = (
            ReimbursementLine.objects
            .select_related("request", "expense_item")
            .filter(Q(receipt_file__isnull=True) | Q(receipt_file=""))
            .order_by("request_id", "id")
        )

        if opts["id_gte"]:
            qs = qs.filter(request_id__gte=opts["id_gte"])
        if opts["id_lte"]:
            qs = qs.filter(request_id__lte=opts["id_lte"])

        rows = []
        used_files = set()

        for line in qs:
            candidates = []
            for f in files:
                if f["rel"] in used_files:
                    continue
                score, reason = score_line_file(line, f)
                if score > 0:
                    candidates.append((score, f, reason))

            candidates.sort(key=lambda x: x[0], reverse=True)
            if not candidates:
                rows.append([line.id, line.request_id, line.amount, line.description, "", 0, "no_match"])
                continue

            best_score, best_file, reason = candidates[0]
            confidence = "HIGH" if best_score >= opts["min_confidence"] else "REVIEW"

            rows.append([
                line.id,
                line.request_id,
                line.amount,
                line.description,
                best_file["rel"],
                best_score,
                confidence,
                reason,
            ])

            if best_score >= opts["min_confidence"]:
                used_files.add(best_file["rel"])

        with open(opts["audit"], "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["line_id", "request_id", "amount", "description", "matched_file", "score", "confidence", "reason"])
            w.writerows(rows)

        high = [r for r in rows if len(r) >= 7 and r[6] == "HIGH"]
        self.stdout.write(f"Candidates: {len(rows)}")
        self.stdout.write(f"HIGH matches: {len(high)}")
        self.stdout.write(f"Audit written: {opts['audit']}")

        for r in rows:
            if len(r) >= 7 and r[6] == "HIGH":
                self.stdout.write(f"HIGH line={r[0]} req={r[1]} amount={r[2]} -> {r[4]} score={r[5]} reason={r[7]}")

        if dry:
            self.stdout.write("DRY RUN ONLY. No DB changes.")
            return

        with transaction.atomic():
            for r in high:
                line = ReimbursementLine.objects.select_for_update().get(pk=r[0])
                if line.receipt_file:
                    continue
                line.receipt_file = r[4]
                line.save(update_fields=["receipt_file", "updated_at"])

        self.stdout.write(self.style.SUCCESS(f"Updated {len(high)} HIGH confidence rows."))

    def rollback(self, path):
        self.stderr.write("Rollback requires old receipt values. Use DB backup or enhanced audit before applying.")