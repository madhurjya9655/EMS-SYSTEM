# FILE: apps/common/google_auth.py

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


class GoogleCredentialError(RuntimeError):
    pass


DEFAULT_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
)


@dataclass(frozen=True)
class GoogleCredentialsBundle:
    credentials: Any  # google.oauth2.service_account.Credentials
    service_account_email: str


def _read_env_json(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise GoogleCredentialError(
            "GoogleCredentialError: Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT."
        ) from e


def _read_json_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise GoogleCredentialError(f"GoogleCredentialError: Credentials file not found at {path}") from e
    except PermissionError as e:
        raise GoogleCredentialError(f"GoogleCredentialError: No permission to read credentials file at {path}") from e
    except json.JSONDecodeError as e:
        raise GoogleCredentialError(f"GoogleCredentialError: Invalid JSON in credentials file at {path}") from e
    except OSError as e:
        raise GoogleCredentialError(f"GoogleCredentialError: Cannot read credentials file at {path}: {e}") from e


def _load_service_account_info() -> Dict[str, Any]:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT")
    if raw:
        return _read_env_json(raw)

    path = (
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
        or os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if path:
        return _read_json_file(path)

    raise GoogleCredentialError(
        "GoogleCredentialError:\n"
        "No Google credentials configured.\n"
        "Please configure GOOGLE_SERVICE_ACCOUNT_JSON in environment.\n"
        "Alternatives supported: GOOGLE_SERVICE_ACCOUNT, GOOGLE_SERVICE_ACCOUNT_JSON_PATH, GOOGLE_SERVICE_ACCOUNT_FILE."
    )


def get_google_credentials(scopes: Optional[Iterable[str]] = None) -> GoogleCredentialsBundle:
    from google.oauth2.service_account import Credentials  # type: ignore

    info = _load_service_account_info()
    scope_list = list(scopes) if scopes else list(DEFAULT_SCOPES)
    creds = Credentials.from_service_account_info(info, scopes=scope_list)
    email = info.get("client_email") or ""
    return GoogleCredentialsBundle(credentials=creds, service_account_email=email)