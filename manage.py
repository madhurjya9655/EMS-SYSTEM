#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from pathlib import Path


def _load_dotenv() -> None:
    """Load environment variables from a .env file at the project root (if present)."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return  # dotenv is optional

    # Project root (same dir as manage.py)
    root = Path(__file__).resolve().parent
    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)


def main() -> None:
    """Run administrative tasks."""
    # Make console I/O UTF-8 safe (especially useful on Windows shells)
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    # Load .env before importing Django so settings can read env vars
    _load_dotenv()

    # Respect an externally provided module; otherwise use the project default
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "employee_management.settings")

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Is it installed and available on your PYTHONPATH? "
            "Did you forget to activate a virtual environment?"
        ) from exc

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
