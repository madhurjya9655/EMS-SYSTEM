import os
from pathlib import Path
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Load .env (locally); on Render, env vars are provided automatically
# -----------------------------------------------------------------------------
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def env_list(name: str, default_csv: str = "") -> list[str]:
    """
    Read a comma-separated env var into a clean list (no blanks).
    """
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

# Host names the app can serve
ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    # Render services + local dev
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

# Optional: make CSRF/session cookies modern & predictable
CSRF_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SAMESITE = "Lax"

# Keep slash-appending behavior (avoids some subtle CSRF edge cases on POST)
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
    "apps.recruitment",
    "apps.leave",
    "apps.core",
    "apps.sales",
    "apps.reimbursement",
    "apps.petty_cash",                 # ✅ restored (fixes RuntimeError)
    "apps.tasks.apps.TasksConfig",     # ensure signals load
    "apps.reports",
    "apps.users",
    "dashboard",
    "apps.settings.apps.SettingsConfig",  # ensure AppConfig is used

    # 3rd-party
    "widget_tweaks",
    "crispy_forms",
    "crispy_bootstrap5",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
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
        },
    },
]

WSGI_APPLICATION = "employee_management.wsgi.application"


# -----------------------------------------------------------------------------
# Database (SQLite) + robust decoders so datetimes never come back as bytes
# -----------------------------------------------------------------------------
DB_PATH = os.getenv("SQLITE_PATH") or str(BASE_DIR / "db.sqlite3")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

import sqlite3  # noqa: E402

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
        # Enable declared-type and "AS <type>" column-name converters.
        "OPTIONS": {
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        },
    }
}

# Always coerce SQLite date/time-ish blobs to clean text for Django to parse.
def _decode_to_str(val):
    if val is None:
        return None
    # Some SQLite builds hand back memoryview for text-ish fields.
    if isinstance(val, memoryview):
        val = val.tobytes()
    if isinstance(val, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1"):
            try:
                return val.decode(enc)
            except Exception:
                continue
        # Last resort: ignore errors but guarantee a str
        try:
            return val.decode("utf-8", "ignore")
        except Exception:
            return str(val)
    # If it's already a str or datetime, Django will handle it later.
    return str(val)

try:
    # Apply to common datetime-ish declared types
    sqlite3.register_converter("timestamp", _decode_to_str)
    sqlite3.register_converter("datetime", _decode_to_str)
    sqlite3.register_converter("timestamptz", _decode_to_str)
    sqlite3.register_converter("timestamp with time zone", _decode_to_str)
    sqlite3.register_converter("date", _decode_to_str)
except Exception:
    # Best-effort; safe to continue if this fails
    pass

from django.db.backends.signals import connection_created  # noqa: E402

def _sqlite_force_text(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    # Text factory used for TEXT columns; make it resilient to bytes/memoryview
    def _tf(x):
        if isinstance(x, memoryview):
            x = x.tobytes()
        if isinstance(x, (bytes, bytearray)):
            try:
                return x.decode("utf-8")
            except Exception:
                return x.decode("latin-1", "ignore")
        return str(x)
    try:
        connection.connection.text_factory = _tf
    except Exception:
        pass

connection_created.connect(_sqlite_force_text)


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
USE_TZ = True  # store aware datetimes; your code converts to IST when needed


# -----------------------------------------------------------------------------
# Static & Media
# -----------------------------------------------------------------------------
STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

# WhiteNoise: efficient static serving on Render
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
WHITENOISE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year for hashed files

MEDIA_URL = "/media/"
MEDIA_ROOT = os.getenv("MEDIA_ROOT") or str(BASE_DIR / "media")
Path(MEDIA_ROOT).mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "dashboard:home"
LOGOUT_REDIRECT_URL = "login"


# -----------------------------------------------------------------------------
# Email (Gmail-ready; values come from .env)
# -----------------------------------------------------------------------------
EMAIL_BACKEND = os.getenv("EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USE_TLS = env_bool("EMAIL_USE_TLS", True)
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", EMAIL_HOST_USER or "no-reply@example.com")

# Safety: never block requests on slow SMTP
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
