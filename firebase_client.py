import os
import time
from datetime import date, datetime

import requests
import streamlit as st


PROJECT_ID = "candiatescr"
DEFAULT_WEB_API_KEY = "AIzaSyAEIgrHYW7PN7CUL_UbY2_j7B3eKbz4IyA"
REST_ROOT = (
    f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}"
    f"/databases/(default)"
)

_TOKEN_CACHE = {"id_token": "", "expiry_epoch": 0}


def _secret_or_env(key: str, default: str = "") -> str:
    try:
        value = st.secrets.get(key, "")
        if value:
            return str(value).strip()
    except Exception:
        pass
    return os.environ.get(key, default).strip()


def get_project_id() -> str:
    return _secret_or_env("CANDIATESCR_FIREBASE_PROJECT_ID", PROJECT_ID) or PROJECT_ID


def get_web_api_key() -> str:
    return (
        _secret_or_env("CANDIATESCR_FIREBASE_WEB_API_KEY")
        or _secret_or_env("FLUTTER_FIREBASE_WEB_API_KEY")
        or DEFAULT_WEB_API_KEY
    )


def get_rest_root() -> str:
    project_id = get_project_id()
    return (
        f"https://firestore.googleapis.com/v1/projects/{project_id}"
        f"/databases/(default)"
    )


def get_access_key() -> str:
    return _secret_or_env("ADMIN_PANEL_ACCESS_KEY")


def describe_auth_state() -> str:
    if _secret_or_env("FLUTTER_FIREBASE_ID_TOKEN") or _secret_or_env(
        "CANDIATESCR_FIREBASE_ID_TOKEN"
    ):
        return "ID token configured"
    if (
        _secret_or_env("FLUTTER_SYNC_EMAIL") or _secret_or_env("CANDIATESCR_SYNC_EMAIL")
    ) and (
        _secret_or_env("FLUTTER_SYNC_PASSWORD")
        or _secret_or_env("CANDIATESCR_SYNC_PASSWORD")
    ):
        return "Email/password sync configured"
    return "No write auth configured"


def _fetch_id_token() -> str:
    explicit_token = (
        _secret_or_env("FLUTTER_FIREBASE_ID_TOKEN")
        or _secret_or_env("CANDIATESCR_FIREBASE_ID_TOKEN")
    )
    if explicit_token:
        return explicit_token

    now = int(time.time())
    if _TOKEN_CACHE["id_token"] and now < int(_TOKEN_CACHE["expiry_epoch"]):
        return _TOKEN_CACHE["id_token"]

    email = _secret_or_env("FLUTTER_SYNC_EMAIL") or _secret_or_env(
        "CANDIATESCR_SYNC_EMAIL"
    )
    password = _secret_or_env("FLUTTER_SYNC_PASSWORD") or _secret_or_env(
        "CANDIATESCR_SYNC_PASSWORD"
    )
    if not email or not password:
        return ""

    auth_url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={get_web_api_key()}"
    )
    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }
    response = requests.post(auth_url, json=payload, timeout=15)
    if response.status_code != 200:
        raise RuntimeError(
            f"Firebase sign-in failed ({response.status_code}): {response.text}"
        )

    body = response.json()
    id_token = str(body.get("idToken", "")).strip()
    expires_in = int(body.get("expiresIn", 0) or 0)
    if id_token and expires_in > 0:
        _TOKEN_CACHE["id_token"] = id_token
        _TOKEN_CACHE["expiry_epoch"] = int(time.time()) + max(expires_in - 120, 60)
    return id_token


def firestore_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    token = _fetch_id_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _python_to_firestore_value(value):
    if value is None:
        return {"nullValue": None}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"integerValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, datetime):
        iso_value = value.isoformat()
        if value.tzinfo is None:
            iso_value += "Z"
        return {"timestampValue": iso_value}
    if isinstance(value, date):
        return {"timestampValue": f"{value.isoformat()}T00:00:00Z"}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, dict):
        return {
            "mapValue": {
                "fields": {key: _python_to_firestore_value(val) for key, val in value.items()}
            }
        }
    if isinstance(value, list):
        return {
            "arrayValue": {
                "values": [_python_to_firestore_value(item) for item in value]
            }
        }
    return {"stringValue": str(value)}


def to_firestore_document(data: dict) -> dict:
    return {"fields": {key: _python_to_firestore_value(value) for key, value in data.items()}}


def _firestore_to_python_value(value: dict):
    if "nullValue" in value:
        return None
    if "booleanValue" in value:
        return bool(value["booleanValue"])
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "timestampValue" in value:
        return str(value["timestampValue"])
    if "stringValue" in value:
        return str(value["stringValue"])
    if "mapValue" in value:
        fields = value.get("mapValue", {}).get("fields", {})
        return {
            key: _firestore_to_python_value(child_value)
            for key, child_value in fields.items()
        }
    if "arrayValue" in value:
        values = value.get("arrayValue", {}).get("values", [])
        return [_firestore_to_python_value(item) for item in values]
    return value


def from_firestore_document(document: dict) -> dict:
    full_name = str(document.get("name", ""))
    doc_id = full_name.split("/")[-1] if full_name else ""
    doc_path = full_name.split("/documents/", 1)[-1] if "/documents/" in full_name else ""
    fields = document.get("fields", {})
    parsed = {
        key: _firestore_to_python_value(value)
        for key, value in fields.items()
    }
    parsed["doc_id"] = doc_id
    parsed["doc_path"] = doc_path
    parsed["_document_name"] = full_name
    return parsed


def add_document(collection_path: str, data: dict, doc_id: str = None) -> str:
    base_url = get_rest_root()
    api_key = get_web_api_key()
    headers = firestore_headers()
    payload = to_firestore_document(data)

    if doc_id:
        url = f"{base_url}/documents/{collection_path}/{doc_id}?key={api_key}"
        response = requests.patch(url, json=payload, headers=headers, timeout=20)
    else:
        url = f"{base_url}/documents/{collection_path}?key={api_key}"
        response = requests.post(url, json=payload, headers=headers, timeout=20)

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Firestore write failed for {collection_path} "
            f"({response.status_code}): {response.text}"
        )

    response_data = response.json()
    return str(response_data.get("name", "")).split("/")[-1] or (doc_id or "")


def batch_create(collection_path: str, documents: list[dict]) -> tuple[list[str], list[str]]:
    saved_ids = []
    errors = []
    for document in documents:
        payload = dict(document)
        doc_id = payload.pop("doc_id", None)
        try:
            saved_ids.append(add_document(collection_path, payload, doc_id=doc_id))
        except Exception as exc:
            errors.append(str(exc))
    return saved_ids, errors


def list_documents(
    collection_path: str,
    page_size: int = 200,
    max_documents: int = 1000,
) -> list[dict]:
    base_url = get_rest_root()
    api_key = get_web_api_key()
    headers = firestore_headers()
    url = f"{base_url}/documents/{collection_path}"
    documents: list[dict] = []
    page_token = ""

    while len(documents) < max_documents:
        params = {
            "key": api_key,
            "pageSize": min(page_size, max_documents - len(documents)),
        }
        if page_token:
            params["pageToken"] = page_token

        response = requests.get(url, params=params, headers=headers, timeout=20)
        if response.status_code != 200:
            raise RuntimeError(
                f"Firestore read failed for {collection_path} "
                f"({response.status_code}): {response.text}"
            )

        payload = response.json()
        page_documents = payload.get("documents", []) or []
        documents.extend(from_firestore_document(document) for document in page_documents)
        page_token = str(payload.get("nextPageToken", "")).strip()
        if not page_token or not page_documents:
            break

    return documents


def delete_document(document_path: str) -> None:
    if not document_path:
        raise ValueError("document_path is required for delete")

    base_url = get_rest_root()
    api_key = get_web_api_key()
    headers = firestore_headers()
    url = f"{base_url}/documents/{document_path}?key={api_key}"
    response = requests.delete(url, headers=headers, timeout=20)
    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Firestore delete failed for {document_path} "
            f"({response.status_code}): {response.text}"
        )


def batch_delete(document_paths: list[str]) -> tuple[int, list[str]]:
    deleted_count = 0
    errors = []
    for document_path in document_paths:
        try:
            delete_document(document_path)
            deleted_count += 1
        except Exception as exc:
            errors.append(str(exc))
    return deleted_count, errors
