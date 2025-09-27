from django.db import migrations

SQL = """
PRAGMA foreign_keys=ON;

-- add columns if missing
CREATE TABLE IF NOT EXISTS __tmp_probe (id INTEGER);
DROP TABLE __tmp_probe;

-- helper to check columns
"""

def forwards(apps, schema_editor):
    c = schema_editor.connection.cursor()

    def has_col(table, col):
        c.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in c.fetchall())

    table = "leave_leavehandover"

    # created_at
    if not has_col(table, "created_at"):
        c.execute(f'ALTER TABLE {table} ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP')

    # updated_at
    if not has_col(table, "updated_at"):
        c.execute(f'ALTER TABLE {table} ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP')

    # optional index for updated_at lookups/sorts
    c.execute(f'CREATE INDEX IF NOT EXISTS {table}_updated_at_idx ON {table} (updated_at)')


def backwards(apps, schema_editor):
    # keep data; no-op on reverse
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0009_backfill_ccconfiguration"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
