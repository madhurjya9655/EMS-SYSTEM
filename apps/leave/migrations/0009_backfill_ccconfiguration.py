from django.db import migrations


DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS "leave_ccconfiguration" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "is_active" BOOLEAN NOT NULL DEFAULT 1,
    "display_name" VARCHAR(200) NOT NULL DEFAULT '',
    "department" VARCHAR(100) NOT NULL DEFAULT '',
    "sort_order" INTEGER UNSIGNED NOT NULL DEFAULT 0,
    "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "user_id" INTEGER NOT NULL REFERENCES "auth_user" ("id") DEFERRABLE INITIALLY DEFERRED
);

-- unique_together ("user",)
CREATE UNIQUE INDEX IF NOT EXISTS "leave_ccconfiguration_user_id_uniq"
    ON "leave_ccconfiguration" ("user_id");

-- helpful ordering/indexes
CREATE INDEX IF NOT EXISTS "leave_ccconfiguration_sort_dept_idx"
    ON "leave_ccconfiguration" ("sort_order", "department");
"""


def forwards(apps, schema_editor):
    with schema_editor.connection.cursor() as c:
        # create table + indexes if missing
        c.executescript(DDL)

def backwards(apps, schema_editor):
    # keep data; noop on reverse
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0008_add_handover_effective_dates"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
