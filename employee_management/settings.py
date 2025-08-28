import os
import sqlite3
from pathlib import Path
from typing import List
from dotenv import load_dotenv

from django.db.backends.signals import connection_created

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


# =============================================================================
# ENV HELPERS
# =============================================================================

def env_list(name: str, default_csv: str = "") -> List[str]:
    """Parse comma-separated environment variable into list"""
    raw = os.getenv(name, default_csv) or ""
    return [part.strip() for part in raw.split(",") if part.strip()]


def env_bool(name: str, default: bool = False) -> bool:
    """Parse boolean environment variable"""
    raw = os.getenv(name, str(default))
    return str(raw).lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int = 0) -> int:
    """Parse integer environment variable"""
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


# =============================================================================
# CORE SETTINGS
# =============================================================================

SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-__dev-only-use-this__")
DEBUG = env_bool("DEBUG", True)
SITE_URL = os.getenv("SITE_URL", "https://ems-system-d26q.onrender.com")
ON_RENDER = bool(os.environ.get("RENDER"))

ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    "ems-system-d26q.onrender.com,localhost,127.0.0.1,0.0.0.0",
)

CSRF_TRUSTED_ORIGINS = env_list(
    "CSRF_TRUSTED_ORIGINS",
    "https://ems-system-d26q.onrender.com",
)

# Add local origins in debug mode
if DEBUG:
    for local_origin in ("http://localhost:8000", "http://127.0.0.1:8000", "http://0.0.0.0:8000"):
        if local_origin not in CSRF_TRUSTED_ORIGINS:
            CSRF_TRUSTED_ORIGINS.append(local_origin)

# Proxy/SSL awareness for Render (critical for session cookies)
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True

# Cookie & security defaults
CSRF_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_HTTPONLY = True
SESSION_EXPIRE_AT_BROWSER_CLOSE = False  # persist logins
APPEND_SLASH = True

# Make sure cookies are secure in prod and on Render
SESSION_COOKIE_SECURE = True if (ON_RENDER or not DEBUG) else False
CSRF_COOKIE_SECURE = True if (ON_RENDER or not DEBUG) else False

# Extra hardening
SECURE_CONTENT_TYPE_NOSNIFF = True

# =============================================================================
# APPLICATIONS
# =============================================================================

DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
]

THIRD_PARTY_APPS = [
    "widget_tweaks",
    "crispy_forms",
    "crispy_bootstrap5",
]

LOCAL_APPS = [
    "apps.common",
    "apps.recruitment",
    "apps.leave",
    "apps.core",
    "apps.sales",
    "apps.reimbursement",
    "apps.petty_cash",
    "apps.tasks.apps.TasksConfig",
    "apps.reports",
    "apps.users.apps.UsersConfig",
    "dashboard.apps.DashboardConfig",
    "apps.settings.apps.SettingsConfig",
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# =============================================================================
# MIDDLEWARE
# =============================================================================

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.middleware.gzip.GZipMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "employee_management.urls"

# =============================================================================
# TEMPLATES
# =============================================================================

_template_options = {
    "context_processors": [
        "django.template.context_processors.debug",
        "django.template.context_processors.request",
        "django.contrib.auth.context_processors.auth",
        "django.contrib.messages.context_processors.messages",
    ],
    "builtins": [
        "dashboard.templatetags.dashboard_extras",
        "apps.reports.templatetags.reports_extras",
    ],
    "libraries": {
        "common_filters": "apps.common.templatetags.common_filters",
        "user_filters": "apps.users.templatetags.user_filters",
        "group_tags": "apps.common.templatetags.group_tags",
        "permission_tags": "apps.common.templatetags.permission_tags",
        "model_extras": "apps.common.templatetags.model_extras",
    },
    "string_if_invalid": "" if DEBUG else "",
}

if DEBUG:
    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [BASE_DIR / "templates"],
            "APP_DIRS": True,
            "OPTIONS": _template_options,
        }
    ]
else:
    # Cached loader for production
    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoDjangoTemplates",
            "DIRS": [BASE_DIR / "templates"],
            "APP_DIRS": False,
            "OPTIONS": {
                **_template_options,
                "loaders": [
                    (
                        "django.template.loaders.cached.Loader",
                        [
                            "django.template.loaders.filesystem.Loader",
                            "django.template.loaders.app_directories.Loader",
                        ],
                    )
                ],
            },
        }
    ]

WSGI_APPLICATION = "employee_management.wsgi.application"

# =============================================================================
# DATABASE - OPTIMIZED SQLITE CONFIGURATION
# =============================================================================

DB_PATH = os.getenv("SQLITE_PATH") or str(BASE_DIR / "db.sqlite3")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
        "OPTIONS": {
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            "timeout": 60,
        },
    }
}

DATABASE_CONNECTION_POOLING = False
CONN_MAX_AGE = 0

# =============================================================================
# ROBUST SQLITE HANDLING (PRAGMAs + decoding)
# =============================================================================

def _robust_sqlite_decoder(val):
    if val is None:
        return None
    if isinstance(val, memoryview):
        try:
            val = val.tobytes()
        except Exception:
            return str(val)
    if isinstance(val, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1", "ascii", "cp1252"):
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
        "timestamp", "datetime", "timestamptz", "timestamp with time zone",
        "date", "time", "TIMESTAMP", "DATETIME", "DATE", "TIME",
    ]:
        sqlite3.register_converter(dt_type, _robust_sqlite_decoder)
except Exception:
    pass


def _configure_sqlite_connection(sender, connection, **kwargs):
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

    try:
        with connection.cursor() as cur:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA cache_size=50000;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA mmap_size=536870912;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=60000;")
            cur.execute("PRAGMA wal_autocheckpoint=1000;")
            cur.execute("PRAGMA optimize;")
    except Exception:
        pass

connection_created.connect(_configure_sqlite_connection)

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

# =============================================================================
# LOGGING - ENHANCED CONFIGURATION (UTF-8 safe)
# =============================================================================

LOGS_DIR = BASE_DIR / "logs"
if not DEBUG and ON_RENDER:
    LOGS_DIR = Path("/tmp/logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {"format": "{levelname} {asctime} {module} {process:d} {thread:d} {message}", "style": "{"},
        "simple": {"format": "{levelname} {asctime} {message}", "style": "{"},
        "detailed": {
            "format": "[{asctime}] {levelname} {name} {module}.{funcName}:{lineno} - {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "level": "WARNING",
            "class": "logging.StreamHandler",
            "formatter": "simple",
        },
        "file": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "filename": str(LOGS_DIR / "django.log"),
            "formatter": "verbose",
            "encoding": "utf-8",
        },
        "tasks_file": {
            "level": "DEBUG" if DEBUG else "INFO",
            "class": "logging.FileHandler",
            "filename": str(LOGS_DIR / "tasks.log"),
            "formatter": "detailed",
            "encoding": "utf-8",
        },
        "bulk_upload_file": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "filename": str(LOGS_DIR / "bulk_upload.log"),
            "formatter": "detailed",
            "encoding": "utf-8",
        },
    },
    "root": {"handlers": ["console"], "level": "WARNING"},
    "loggers": {
        "django": {"handlers": ["file"], "level": "INFO", "propagate": False},
        "django.db.backends": {"handlers": ["file"], "level": "WARNING", "propagate": False},
        "apps.tasks": {"handlers": ["tasks_file", "console"], "level": "DEBUG" if DEBUG else "INFO", "propagate": False},
        "apps.tasks.views": {"handlers": ["bulk_upload_file", "console"], "level": "INFO", "propagate": False},
        "apps.tasks.signals": {"handlers": ["tasks_file"], "level": "INFO", "propagate": False},
    },
}

# =============================================================================
# INTERNATIONALIZATION
# =============================================================================

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Kolkata"
USE_I18N = True
USE_TZ = True

# =============================================================================
# STATIC & MEDIA FILES
# =============================================================================

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

STORAGES = {
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
}

WHITENOISE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year

MEDIA_URL = "/media/"
MEDIA_ROOT = os.getenv("MEDIA_ROOT") or ("/var/data/media" if ON_RENDER else str(BASE_DIR / "media"))
Path(MEDIA_ROOT).mkdir(parents=True, exist_ok=True)

# =============================================================================
# AUTHENTICATION
# =============================================================================

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "/dashboard/"   # keep simple, avoids reverse issues on Render
LOGOUT_REDIRECT_URL = "login"

# =============================================================================
# EMAIL CONFIGURATION - ENHANCED
# =============================================================================

EMAIL_BACKEND = os.getenv("EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = env_int("EMAIL_PORT", 587)
EMAIL_USE_TLS = env_bool("EMAIL_USE_TLS", True)
EMAIL_USE_SSL = env_bool("EMAIL_USE_SSL", False)
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", EMAIL_HOST_USER or "EMS System <no-reply@example.com>")
EMAIL_TIMEOUT = env_int("EMAIL_TIMEOUT", 30)
EMAIL_FAIL_SILENTLY = env_bool("EMAIL_FAIL_SILENTLY", False if DEBUG else True)

SEND_EMAILS_FOR_AUTO_RECUR = env_bool("SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = env_bool("SEND_RECUR_EMAILS_ONLY_AT_10AM", True)

EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "[EMS] ")

# =============================================================================
# SECURITY SETTINGS (Prod)
# =============================================================================

if not DEBUG:
    SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", True)
    # Cookies already set above; keep True here too
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

    SECURE_HSTS_SECONDS = env_int("SECURE_HSTS_SECONDS", 31536000)
    SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", True)
    SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", True)

    SECURE_CONTENT_TYPE_NOSNIFF = True
    # Kept for compatibility; harmless in modern Django
    SECURE_BROWSER_XSS_FILTER = True
    X_FRAME_OPTIONS = "DENY"
    SECURE_REFERRER_POLICY = os.getenv("SECURE_REFERRER_POLICY", "strict-origin-when-cross-origin")

# =============================================================================
# THIRD-PARTY PACKAGES
# =============================================================================

CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_SCOPES = os.getenv("GOOGLE_SHEET_SCOPES")

# =============================================================================
# TASK SYSTEM CONFIGURATION - OPTIMIZED
# =============================================================================

BULK_UPLOAD_BATCH_SIZE = env_int("BULK_UPLOAD_BATCH_SIZE", 200)
BULK_UPLOAD_MAX_ROWS = env_int("BULK_UPLOAD_MAX_ROWS", 5000)
EMAIL_BATCH_SIZE = env_int("EMAIL_BATCH_SIZE", 20)
EMAIL_SEND_DELAY = float(os.getenv("EMAIL_SEND_DELAY", "0.01"))

TASK_PROCESSING_TIMEOUT = env_int("TASK_PROCESSING_TIMEOUT", 600)
RECURRING_TASK_BATCH_SIZE = env_int("RECURRING_TASK_BATCH_SIZE", 100)

AUTO_CREATE_RECURRING_TASKS = env_bool("AUTO_CREATE_RECURRING_TASKS", True)
RECURRING_TASK_LOOKAHEAD_DAYS = env_int("RECURRING_TASK_LOOKAHEAD_DAYS", 30)

DASHBOARD_CACHE_TIMEOUT = env_int("DASHBOARD_CACHE_TIMEOUT", 300)
TASK_LIST_PAGE_SIZE = env_int("TASK_LIST_PAGE_SIZE", 50)

# =============================================================================
# PERFORMANCE SETTINGS
# =============================================================================

# Default to DB-backed sessions (stable across dyno restarts)
SESSION_ENGINE = "django.contrib.sessions.backends.db"
SESSION_COOKIE_AGE = 60 * 60 * 24 * 7  # 1 week

FILE_UPLOAD_MAX_MEMORY_SIZE = env_int("FILE_UPLOAD_MAX_MEMORY_SIZE", 10 * 1024 * 1024)
DATA_UPLOAD_MAX_MEMORY_SIZE = env_int("DATA_UPLOAD_MAX_MEMORY_SIZE", 10 * 1024 * 1024)
DATA_UPLOAD_MAX_NUMBER_FIELDS = env_int("DATA_UPLOAD_MAX_NUMBER_FIELDS", 2000)

REDIS_URL = os.getenv("REDIS_URL", "").strip()
if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": REDIS_URL,
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "COMPRESSOR": "django_redis.compressors.zlib.ZlibCompressor",
                "PARSER_CLASS": "redis.connection.HiredisParser",
            },
            "TIMEOUT": env_int("CACHE_TIMEOUT", 300),
        }
    }

else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "ems-fast-cache",
            "TIMEOUT": 300,
            "OPTIONS": {"MAX_ENTRIES": 2000, "CULL_FREQUENCY": 3},
        }
    }

# Faster JSON handling for responses (keeps emoji intact)
JSON_DUMPS_PARAMS = {"ensure_ascii": False}

# =============================================================================
# RENDER.COM SPECIFIC SETTINGS
# =============================================================================

if ON_RENDER:
    # Force DB sessions and secure cookies behind proxy
    SESSION_ENGINE = "django.contrib.sessions.backends.db"
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

    WEB_CONCURRENCY = env_int("WEB_CONCURRENCY", 2)
    MESSAGE_STORAGE = "django.contrib.messages.storage.session.SessionStorage"
    FILE_UPLOAD_MAX_MEMORY_SIZE = min(FILE_UPLOAD_MAX_MEMORY_SIZE, 5 * 1024 * 1024)
    DATA_UPLOAD_MAX_MEMORY_SIZE = min(DATA_UPLOAD_MAX_MEMORY_SIZE, 5 * 1024 * 1024)
    EMAIL_FAIL_SILENTLY = True
    STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

# =============================================================================
# DEVELOPMENT SETTINGS
# =============================================================================

if DEBUG:
    INTERNAL_IPS = ["127.0.0.1", "localhost"]

    # Use console backend if no SMTP creds during local dev
    if not EMAIL_HOST_USER:
        EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

    # Keep dev relaxed
    SECURE_SSL_REDIRECT = False
    SESSION_COOKIE_SECURE = False
    CSRF_COOKIE_SECURE = False

# =============================================================================
# CUSTOM SETTINGS VALIDATION
# =============================================================================

def validate_email_settings():
    if not DEBUG and EMAIL_BACKEND == "django.core.mail.backends.smtp.EmailBackend":
        if not EMAIL_HOST_USER or not EMAIL_HOST_PASSWORD:
            import warnings
            warnings.warn(
                "Email credentials not configured. Email functionality may not work.",
                RuntimeWarning,
            )

def validate_required_dirs():
    required_dirs = [MEDIA_ROOT, STATIC_ROOT, LOGS_DIR]
    for dir_path in required_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

validate_email_settings()
validate_required_dirs()

# =============================================================================
# FEATURE FLAGS
# =============================================================================

FEATURES = {
    "BULK_UPLOAD_ENABLED": env_bool("FEATURE_BULK_UPLOAD", True),
    "EMAIL_NOTIFICATIONS": env_bool("FEATURE_EMAIL_NOTIFICATIONS", True),
    "RECURRING_TASKS": env_bool("FEATURE_RECURRING_TASKS", True),
    "TASK_REMINDERS": env_bool("FEATURE_TASK_REMINDERS", True),
    "ADVANCED_REPORTING": env_bool("FEATURE_ADVANCED_REPORTING", True),
    "AUDIT_LOGGING": env_bool("FEATURE_AUDIT_LOGGING", True),
}

# =============================================================================
# CONSTANTS
# =============================================================================

TASK_PRIORITIES = [("Low", "Low"), ("Medium", "Medium"), ("High", "High")]
TASK_STATUSES = [("Pending", "Pending"), ("Completed", "Completed")]
RECURRING_MODES = [("Daily", "Daily"), ("Weekly", "Weekly"), ("Monthly", "Monthly"), ("Yearly", "Yearly")]
HELP_TICKET_STATUSES = [("Open", "Open"), ("In Progress", "In Progress"), ("Closed", "Closed")]
