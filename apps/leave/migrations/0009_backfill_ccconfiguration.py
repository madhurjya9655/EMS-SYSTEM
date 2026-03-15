from django.db import migrations


def forwards(apps, schema_editor):
    connection = schema_editor.connection
    vendor = connection.vendor

    with connection.cursor() as cursor:
        def table_exists(table_name):
            if vendor == "sqlite":
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    [table_name],
                )
                return cursor.fetchone() is not None

            if vendor == "postgresql":
                cursor.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                    """,
                    [table_name],
                )
                return cursor.fetchone() is not None

            raise NotImplementedError(f"Unsupported database vendor: {vendor}")

        if not table_exists("leave_ccconfiguration"):
            if vendor == "sqlite":
                cursor.execute(
                    """
                    CREATE TABLE "leave_ccconfiguration" (
                        "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        "is_active" BOOLEAN NOT NULL DEFAULT 1,
                        "display_name" VARCHAR(200) NOT NULL DEFAULT '',
                        "department" VARCHAR(100) NOT NULL DEFAULT '',
                        "sort_order" INTEGER NOT NULL DEFAULT 0,
                        "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        "updated_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        "user_id" INTEGER NOT NULL
                            REFERENCES "auth_user" ("id") DEFERRABLE INITIALLY DEFERRED
                    )
                    """
                )
            elif vendor == "postgresql":
                cursor.execute(
                    """
                    CREATE TABLE leave_ccconfiguration (
                        id BIGSERIAL PRIMARY KEY,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        display_name VARCHAR(200) NOT NULL DEFAULT '',
                        department VARCHAR(100) NOT NULL DEFAULT '',
                        sort_order INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        user_id INTEGER NOT NULL
                            REFERENCES auth_user (id) DEFERRABLE INITIALLY DEFERRED
                    )
                    """
                )

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS leave_ccconfiguration_user_id_uniq
            ON leave_ccconfiguration (user_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS leave_ccconfiguration_sort_dept_idx
            ON leave_ccconfiguration (sort_order, department)
            """
        )


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