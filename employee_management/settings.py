import os
from pathlib import Path
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Load .env locally; on Render, env vars are provided automatically
# -----------------------------------------------------------------------------
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def env_list(name: str, default_csv: str = "") -> list[str]:
    """Read a comma-separated env var into a clean list (no blanks)."""
    raw = os.getenv(name, default_csv) or ""
    return [part.strip() for part in raw.split(",") if part.strip()]


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, str(default))
    return str(raw).lower() in ("1", "true", "yes", "on")


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-__dev-only-use-this__")
DEBUG = env_bool("DEBUG", True)

# Site URL used in emails and absolute links
SITE_URL = os.getenv("SITE_URL", "https://ems-system-d26q.onrender.com")

# Host names the app can serve
ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    "ems-system-v944.onrender.com,ems-system-d26q.onrender.com,localhost,127.0.0.1",
)

# CSRF trusted *origins* must include scheme (https:// or http://)
CSRF_TRUSTED_ORIGINS = env_list(
    "CSRF_TRUSTED_ORIGINS",
    "https://ems-system-v944.onrender.com,https://ems-system-d26q.onrender.com",
)

# Add local dev origins automatically when DEBUG=True
if DEBUG:
    for local_origin in ("http://localhost:8000", "http://127.0.0.1:8000"):
        if local_origin not in CSRF_TRUSTED_ORIGINS:
            CSRF_TRUSTED_ORIGINS.append(local_origin)

# When running behind Render's proxy, ensure Django treats requests as HTTPS
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
# (optional) allow host header from proxy
USE_X_FORWARDED_HOST = True

# CSRF/session cookie modern defaults
CSRF_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SAMESITE = "Lax"

# Keep slash-appending behavior
APPEND_SLASH = True


# -----------------------------------------------------------------------------
# Apps
# -----------------------------------------------------------------------------
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",

    # Your apps
    "apps.common",                     # <-- contains the global custom_filters
    "apps.recruitment",
    "apps.leave",
    "apps.core",
    "apps.sales",
    "apps.reimbursement",
    "apps.petty_cash",
    "apps.tasks.apps.TasksConfig",
    "apps.reports",
    "apps.users",
    "dashboard",
    "apps.settings.apps.SettingsConfig",

    # 3rd-party
    "widget_tweaks",
    "crispy_forms",
    "crispy_bootstrap5",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # static files in prod
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "employee_management.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
            # ✅ Make common filters (delay_since, minutes_to_hhmm, etc.) available everywhere
            "builtins": [
                "apps.common.templatetags.custom_filters",
            ],
        },
    },
]

WSGI_APPLICATION = "employee_management.wsgi.application"


# -----------------------------------------------------------------------------
# Database (SQLite) + enhanced datetime BLOB handling
# -----------------------------------------------------------------------------
DB_PATH = os.getenv("SQLITE_PATH") or str(BASE_DIR / "db.sqlite3")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

import sqlite3  # noqa: E402

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
        "OPTIONS": {
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        },
    }
}

# Robust SQLite datetime conversion helpers
def _robust_sqlite_decoder(val):
    """Decode bytes/memoryview into clean strings for datetime parsing."""
    if val is None:
        return None
    if isinstance(val, memoryview):
        try:
            val = val.tobytes()
        except Exception:
            return str(val)
    if isinstance(val, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1", "ascii"):
            try:
                s = val.decode(enc).strip().replace("\x00", "")
                if s:
                    return s
            except Exception:
                continue
        try:
            return val.decode("utf-8", errors="ignore").strip()
        except Exception:
            return str(val)
    if isinstance(val, str):
        return val.strip().replace("\x00", "") or None
    try:
        return str(val)
    except Exception:
        return None

try:
    for dt_type in [
        "timestamp", "datetime", "timestamptz",
        "timestamp with time zone", "date", "time",
        "TIMESTAMP", "DATETIME", "DATE", "TIME",
    ]:
        sqlite3.register_converter(dt_type, _robust_sqlite_decoder)
except Exception:
    pass

from django.db.backends.signals import connection_created  # noqa: E402

def _configure_sqlite_for_robust_datetime_handling(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    try:
        def universal_text_factory(data):
            if data is None:
                return None
            if isinstance(data, memoryview):
                try:
                    data = data.tobytes()
                except Exception:
                    return str(data)
            if isinstance(data, (bytes, bytearray)):
                for enc in ("utf-8", "latin-1", "ascii", "cp1252"):
                    try:
                        res = data.decode(enc).strip().replace("\x00", "")
                        if res:
                            return res
                    except Exception:
                        continue
                try:
                    return data.decode("utf-8", errors="ignore").strip()
                except Exception:
                    return str(data)
            if isinstance(data, str):
                return data.strip().replace("\x00", "")
            try:
                return str(data)
            except Exception:
                return ""
        connection.connection.text_factory = universal_text_factory
    except Exception:
        pass

connection_created.connect(_configure_sqlite_for_robust_datetime_handling)

try:
    from django.db.backends.sqlite3.operations import DatabaseOperations
    _orig_convert = DatabaseOperations.convert_datetimefield_value

    def safe_convert_datetimefield_value(self, value, expression, connection):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = _robust_sqlite_decoder(value)
        if isinstance(value, str):
            value = value.strip().replace("\x00", "") or None
            if value is None:
                return None
        try:
            return _orig_convert(self, value, expression, connection)
        except (TypeError, ValueError) as e:
            if "fromisoformat" in str(e) or "argument must be str" in str(e):
                try:
                    if hasattr(value, "decode"):
                        fixed = value.decode("utf-8", errors="ignore").strip()
                        return _orig_convert(self, fixed, expression, connection)
                except Exception:
                    return None
                return None
            raise

    DatabaseOperations.convert_datetimefield_value = safe_convert_datetimefield_value
except Exception:
    pass


# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]


# -----------------------------------------------------------------------------
# I18N / TZ
# -----------------------------------------------------------------------------
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Kolkata"  # IST
USE_I18N = True
USE_TZ = True


# -----------------------------------------------------------------------------
# Static & Media  (Django 5 compatible)
# -----------------------------------------------------------------------------
STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

# Django 5 uses STORAGES (STATICFILES_STORAGE is removed)
STORAGES = {
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
}

# WhiteNoise cache lifetime for hashed files (optional)
WHITENOISE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year

MEDIA_URL = "/media/"
# Use Render disk if available; else local media folder
MEDIA_ROOT = os.getenv("MEDIA_ROOT") or ("/var/data/media" if os.getenv("RENDER") else str(BASE_DIR / "media"))
Path(MEDIA_ROOT).mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "dashboard:home"
LOGOUT_REDIRECT_URL = "login"


# -----------------------------------------------------------------------------
# Email (Gmail-ready; values come from env)
# -----------------------------------------------------------------------------
EMAIL_BACKEND = os.getenv("EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USE_TLS = env_bool("EMAIL_USE_TLS", True)
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", EMAIL_HOST_USER or "no-reply@example.com")
EMAIL_TIMEOUT = int(os.getenv("EMAIL_TIMEOUT", "10"))
EMAIL_FAIL_SILENTLY = env_bool("EMAIL_FAIL_SILENTLY", False)

# Feature flag: send emails when auto-generating recurring tasks?
SEND_EMAILS_FOR_AUTO_RECUR = env_bool("SEND_EMAILS_FOR_AUTO_RECUR", False)


# -----------------------------------------------------------------------------
# Security (stronger defaults when DEBUG=False)
# -----------------------------------------------------------------------------
if not DEBUG:
    SECURE_SSL_REDIRECT = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = int(os.getenv("SECURE_HSTS_SECONDS", "3600"))
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", True)
    SECURE_REFERRER_POLICY = os.getenv("SECURE_REFERRER_POLICY", "strict-origin-when-cross-origin")


# -----------------------------------------------------------------------------
# Crispy Forms
# -----------------------------------------------------------------------------
CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"


# -----------------------------------------------------------------------------
# Google API (optional; used elsewhere)
# -----------------------------------------------------------------------------
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_SCOPES = os.getenv("GOOGLE_SHEET_SCOPES")
