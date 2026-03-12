from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from email.message import EmailMessage
import os
import smtplib

import pandas as pd
import streamlit as st

from firebase_client import (
    batch_create,
    batch_delete,
    describe_auth_state,
    get_access_key,
    get_document,
    get_project_id,
    list_documents,
    upsert_document,
)
from generators import (
    generate_hackathons,
    generate_jobs,
    generate_projects,
    generate_teams,
    parse_user_ids,
)


st.set_page_config(
    page_title="Candiatescr Super Admin Studio",
    page_icon="C",
    layout="wide",
)


MANAGED_COLLECTIONS = ["jobs", "hackathons", "teams", "projects"]
LOCAL_TZ = datetime.now().astimezone().tzinfo or timezone.utc
ANALYTICS_COLLECTION_LIMITS = {
    "users": 5000,
    "jobs": 2500,
    "hackathons": 2500,
    "job_applications": 3000,
    "site_analytics": 5000,
    "activities": 3000,
    "alerts": 3000,
    "notifications": 1500,
}
DIGEST_CONFIG_DOC = "daily_digest"
DIGEST_ROLE_OPTIONS = ["candidate", "student", "hr", "recruiter", "mentor", "admin"]
SMTP_EMAIL_KEYS = (
    "ADMIN_GMAIL_EMAIL",
    "GMAIL_ADDRESS",
    "GMAIL_EMAIL",
    "SMTP_EMAIL",
    "EMAIL_USER",
)
SMTP_PASSWORD_KEYS = (
    "ADMIN_GMAIL_PASSWORD",
    "GMAIL_APP_PASSWORD",
    "GMAIL_PASSWORD",
    "SMTP_PASSWORD",
    "EMAIL_PASS",
)


def _secret_or_env(key: str, default: str = "") -> str:
    try:
        value = st.secrets.get(key, "")
        if value:
            return str(value).strip()
    except Exception:
        pass
    return os.environ.get(key, default).strip()


def _first_secret_or_env(keys: tuple[str, ...], default: str = "") -> str:
    for key in keys:
        value = _secret_or_env(key)
        if value:
            return value
    return default.strip()


def _smtp_sender_email() -> str:
    session_value = str(st.session_state.get("admin_mail_sender_email") or "").strip()
    if session_value:
        return session_value
    return _first_secret_or_env(SMTP_EMAIL_KEYS)


def _smtp_sender_password() -> str:
    session_value = str(st.session_state.get("admin_mail_sender_password") or "").strip()
    if session_value:
        return session_value
    return _first_secret_or_env(SMTP_PASSWORD_KEYS)


def _smtp_from_name() -> str:
    return (
        _secret_or_env("SMTP_FROM_NAME")
        or _secret_or_env("MAIL_FROM_NAME")
        or "Candiatescr Opportunities"
    )


def _digest_delivery_provider() -> str:
    return "streamlit_gmail_smtp"


def _require_smtp_credentials() -> tuple[str, str]:
    sender_email = _smtp_sender_email()
    sender_password = _smtp_sender_password()
    if sender_email and sender_password:
        return sender_email, sender_password
    raise RuntimeError(
        "Gmail sender email and password are required. Set `GMAIL_ADDRESS` or "
        "`GMAIL_EMAIL`, plus `GMAIL_APP_PASSWORD`, in Streamlit secrets/environment, "
        "or enter them in the Daily Digest tab."
    )


def inject_styles():
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(0, 153, 255, 0.10), transparent 28%),
                radial-gradient(circle at left center, rgba(0, 200, 120, 0.10), transparent 24%),
                linear-gradient(180deg, #f6fbff 0%, #eef4f9 100%);
        }
        .hero {
            padding: 1.45rem 1.6rem;
            border-radius: 22px;
            background: linear-gradient(135deg, #0f172a 0%, #133b5c 50%, #0ea5e9 100%);
            color: white;
            border: 1px solid rgba(255,255,255,0.12);
            box-shadow: 0 24px 60px rgba(15, 23, 42, 0.18);
            margin-bottom: 1rem;
        }
        .hero h1 {
            margin: 0;
            font-size: 2rem;
        }
        .hero p {
            margin: 0.45rem 0 0;
            max-width: 880px;
            color: rgba(255,255,255,0.82);
        }
        .mini-card {
            background: rgba(255,255,255,0.84);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 18px;
            padding: 1rem;
            box-shadow: 0 14px 28px rgba(15, 23, 42, 0.07);
        }
        .mini-card h3 {
            margin: 0;
            font-size: 0.82rem;
            letter-spacing: 0.04em;
            color: #475569;
        }
        .mini-card strong {
            display: block;
            margin-top: 0.25rem;
            font-size: 1.4rem;
            color: #0f172a;
        }
        .insight-chip {
            display: inline-block;
            padding: 0.45rem 0.7rem;
            margin: 0 0.45rem 0.45rem 0;
            border-radius: 999px;
            background: rgba(14, 165, 233, 0.08);
            color: #0f172a;
            border: 1px solid rgba(14, 165, 233, 0.16);
            font-size: 0.82rem;
        }
        .small-note {
            color: #475569;
            font-size: 0.83rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def require_access():
    access_key = get_access_key()
    if not access_key:
        st.sidebar.warning(
            "ADMIN_PANEL_ACCESS_KEY is not configured. Panel is open locally."
        )
        return

    entered_key = st.sidebar.text_input(
        "Admin access key",
        type="password",
        help="Set ADMIN_PANEL_ACCESS_KEY in Streamlit secrets or environment.",
    )
    if entered_key != access_key:
        st.warning("Enter the admin access key to continue.")
        st.stop()


def show_preview(title: str, records: list[dict], preferred_columns: list[str]):
    st.write(title)
    if not records:
        st.info("No preview data available yet.")
        return
    frame = pd.DataFrame(records)
    visible_columns = [column for column in preferred_columns if column in frame.columns]
    st.dataframe(frame[visible_columns] if visible_columns else frame, use_container_width=True)


def clear_admin_cache():
    load_collection_cached.clear()
    load_document_cached.clear()


def _best_effort_upsert(collection_name: str, doc_id: str, payload: dict) -> str | None:
    try:
        return upsert_document(collection_name, doc_id, payload)
    except Exception:
        return None


def _best_effort_batch_create(collection_name: str, documents: list[dict]) -> list[str]:
    if not documents:
        return []
    try:
        _, errors = batch_create(collection_name, documents)
        return errors
    except Exception as exc:
        return [str(exc)]


def save_collection(collection_name: str, documents: list[dict]):
    if not documents:
        st.warning(f"No {collection_name} records to save.")
        return

    saved_ids, errors = batch_create(collection_name, documents)
    if saved_ids:
        clear_admin_cache()
        st.success(f"Saved {len(saved_ids)} documents to `{collection_name}`.")
        st.caption("Sample document IDs: " + ", ".join(saved_ids[:5]))
    if errors:
        st.error(f"{len(errors)} writes failed for `{collection_name}`.")
        for error in errors[:3]:
            st.code(error)


def _new_batch_id() -> str:
    return f"batch-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


def _ensure_batch_id() -> str:
    if "admin_seed_batch_id" not in st.session_state:
        st.session_state["admin_seed_batch_id"] = _new_batch_id()
    return str(st.session_state["admin_seed_batch_id"])


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _is_true(value) -> bool:
    return value is True or _normalize_text(value) in {"true", "1", "yes", "y", "on"}


def _matches_legacy_generated_record(collection_name: str, document: dict) -> bool:
    description = _normalize_text(document.get("description"))
    if collection_name == "jobs":
        return _normalize_text(document.get("source")) == "seed_admin_panel" or (
            "this is seeded demo data for the admin panel." in description
        )
    if collection_name == "hackathons":
        return description.startswith(
            "seeded hackathon for the flutter candidate experience."
        )
    if collection_name == "teams":
        return (
            description
            == "demo team for talent collaboration, prototyping, and hackathon prep."
        )
    if collection_name == "projects":
        return "is seeded demo data for portfolio and project discovery screens." in description
    return False


def _document_label(collection_name: str, document: dict) -> str:
    field_map = {
        "jobs": "jobTitle",
        "hackathons": "name",
        "teams": "teamName",
        "projects": "title",
    }
    return str(document.get(field_map.get(collection_name, ""), "")).strip() or document.get(
        "doc_id", "Untitled"
    )


def _parse_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, (int, float)):
        raw_value = float(value)
        if raw_value > 1_000_000_000_000:
            raw_value = raw_value / 1000.0
        parsed = datetime.fromtimestamp(raw_value, tz=timezone.utc)
    else:
        raw = str(value).strip()
        if not raw:
            return None
        candidate = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
            for pattern in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y",
            ):
                try:
                    parsed = datetime.strptime(raw, pattern)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(LOCAL_TZ)


def _first_datetime(document: dict, *fields: str) -> datetime | None:
    for field in fields:
        parsed = _parse_datetime(document.get(field))
        if parsed:
            return parsed
    return None


def _format_dt(value: datetime | None) -> str:
    if not value:
        return "-"
    return value.strftime("%d %b %Y, %I:%M %p")


def _series_template(days: int) -> list[date]:
    today = datetime.now(LOCAL_TZ).date()
    start = today - timedelta(days=days - 1)
    return [start + timedelta(days=index) for index in range(days)]


@st.cache_data(ttl=120, show_spinner=False)
def load_collection_cached(collection_name: str, max_documents: int) -> list[dict]:
    return list_documents(collection_name, page_size=250, max_documents=max_documents)


@st.cache_data(ttl=120, show_spinner=False)
def load_document_cached(collection_name: str, doc_id: str) -> dict:
    return get_document(collection_name, doc_id)


def _safe_load_collection(collection_name: str) -> tuple[list[dict], str | None]:
    limit = ANALYTICS_COLLECTION_LIMITS.get(collection_name, 1000)
    try:
        return load_collection_cached(collection_name, limit), None
    except Exception as exc:
        return [], str(exc)


def _load_super_admin_data() -> dict:
    payload = {}
    errors: dict[str, str] = {}
    for collection_name in ANALYTICS_COLLECTION_LIMITS:
        rows, error = _safe_load_collection(collection_name)
        payload[collection_name] = rows
        if error:
            errors[collection_name] = error
    try:
        payload["digest_config"] = load_document_cached("system_config", DIGEST_CONFIG_DOC)
    except Exception as exc:
        payload["digest_config"] = {}
        errors["system_config/daily_digest"] = str(exc)
    try:
        payload["digest_runs"] = list_documents("digest_runs", page_size=25, max_documents=25)
    except Exception as exc:
        payload["digest_runs"] = []
        errors["digest_runs"] = str(exc)
    payload["errors"] = errors
    return payload


def _count_by_day(
    records: list[dict],
    date_fields: list[str],
    days: int = 14,
    unique_field: str | None = None,
) -> pd.DataFrame:
    day_range = _series_template(days)
    buckets = {day: set() if unique_field else 0 for day in day_range}
    for record in records:
        event_at = _first_datetime(record, *date_fields)
        if not event_at:
            continue
        day_key = event_at.date()
        if day_key not in buckets:
            continue
        if unique_field:
            identifier = str(record.get(unique_field, "")).strip() or str(
                record.get("doc_id", "")
            ).strip()
            if identifier:
                buckets[day_key].add(identifier)
        else:
            buckets[day_key] += 1
    rows = []
    for day_key in day_range:
        raw_value = buckets[day_key]
        rows.append(
            {
                "Date": day_key,
                "Value": len(raw_value) if isinstance(raw_value, set) else raw_value,
            }
        )
    return pd.DataFrame(rows)


def _series_value(frame: pd.DataFrame, day_value: date) -> int:
    if frame.empty:
        return 0
    rows = frame.loc[frame["Date"] == day_value, "Value"]
    return int(rows.iloc[0]) if not rows.empty else 0


def _normalize_user_role(document: dict) -> str:
    primary = _normalize_text(document.get("orgRole") or document.get("role"))
    if primary in {"hr", "recruiter"}:
        return "hr"
    if "mentor" in primary:
        return "mentor"
    if "admin" in primary:
        return "admin"
    if primary in {"candidate", "student", "learner", "user", ""}:
        return "candidate"
    return primary or "candidate"


def _user_role_matches(document: dict, allowed_roles: list[str]) -> bool:
    if not allowed_roles:
        return True
    normalized_allowed = {_normalize_text(role) for role in allowed_roles}
    primary = _normalize_text(document.get("role"))
    org_role = _normalize_text(document.get("orgRole"))
    bucket = _normalize_user_role(document)
    candidates = {primary, org_role, bucket}
    if bucket == "candidate":
        candidates.update({"candidate", "student"})
    if bucket == "hr":
        candidates.update({"hr", "recruiter"})
    return bool(candidates & normalized_allowed)


def _is_active_user(document: dict) -> bool:
    status = _normalize_text(document.get("status"))
    return status not in {"blocked", "banned", "disabled", "inactive", "deleted", "rejected"}


def _active_jobs(jobs: list[dict]) -> list[dict]:
    visible_statuses = {"", "active", "open", "published", "live", "ongoing", "recruiting"}
    active_jobs = []
    now = datetime.now(LOCAL_TZ)
    for job in jobs:
        status = _normalize_text(job.get("status"))
        deadline = _first_datetime(job, "deadline", "applicationDeadline", "closingDate")
        if status not in visible_statuses:
            continue
        if deadline and deadline < now:
            continue
        active_jobs.append(job)
    return active_jobs


def _active_hackathons(hackathons: list[dict]) -> list[dict]:
    visible_statuses = {"", "active", "open", "published", "live", "ongoing", "registration_open", "draft"}
    rows = []
    now = datetime.now(LOCAL_TZ)
    for hackathon in hackathons:
        status = _normalize_text(hackathon.get("status"))
        deadline = _first_datetime(
            hackathon,
            "registrationDeadline",
            "deadline",
            "endDate",
            "lastDate",
        )
        if status not in visible_statuses:
            continue
        if deadline and deadline < now and status not in {"draft"}:
            continue
        rows.append(hackathon)
    return rows


def _extract_login_events(activities: list[dict], site_analytics: list[dict]) -> tuple[list[dict], str]:
    direct_events: list[dict] = []
    for entry in activities:
        action = _normalize_text(entry.get("action"))
        description = _normalize_text(entry.get("description"))
        if action in {"login", "sign_in", "signin", "auth_login"} or "signed in" in description or "logged in" in description:
            event_at = _first_datetime(entry, "timestamp", "createdAt", "ts")
            user_id = str(entry.get("userId", "")).strip()
            if event_at and user_id:
                direct_events.append({"eventAt": event_at, "userId": user_id, "source": "activities"})
    if direct_events:
        return direct_events, "activities.login"

    proxy_events: list[dict] = []
    for entry in site_analytics:
        event_at = _first_datetime(entry, "ts", "timestamp", "createdAt")
        user_id = str(entry.get("userId", "")).strip()
        category = _normalize_text(entry.get("category"))
        route = _normalize_text(entry.get("route"))
        if not event_at or not user_id:
            continue
        if category == "dashboard" or route.startswith("/dashboard"):
            proxy_events.append(
                {"eventAt": event_at, "userId": user_id, "source": "site_analytics.dashboard"}
            )
    return proxy_events, "site_analytics.dashboard proxy"


def _activity_feed_rows(data: dict) -> list[dict]:
    rows: list[dict] = []

    for entry in data.get("activities", [])[:250]:
        event_at = _first_datetime(entry, "timestamp", "createdAt", "ts")
        if not event_at:
            continue
        rows.append(
            {
                "Time": event_at,
                "Source": "Activity",
                "Title": str(entry.get("action", "event")).replace("_", " ").title(),
                "Detail": str(entry.get("description", "")).strip() or "System event",
            }
        )

    for entry in data.get("job_applications", [])[:250]:
        event_at = _first_datetime(entry, "appliedAt", "applied_at", "createdAt", "timestamp")
        if not event_at:
            continue
        applicant = str(
            entry.get("userName")
            or entry.get("candidateName")
            or entry.get("applicant_name")
            or entry.get("applicant_email")
            or "Candidate"
        ).strip()
        job_title = str(
            entry.get("jobTitle") or entry.get("job_title") or entry.get("jobId") or "Unknown Job"
        ).strip()
        status = str(entry.get("status", "pending")).strip().upper()
        rows.append(
            {
                "Time": event_at,
                "Source": "Application",
                "Title": applicant,
                "Detail": f"{job_title} | {status}",
            }
        )

    for entry in data.get("notifications", [])[:150]:
        event_at = _first_datetime(entry, "timestamp", "createdAt")
        if not event_at:
            continue
        rows.append(
            {
                "Time": event_at,
                "Source": "Notification",
                "Title": str(entry.get("title", "Admin Notice")).strip(),
                "Detail": str(entry.get("message", "")).strip(),
            }
        )

    rows.sort(key=lambda row: row["Time"], reverse=True)
    return rows[:14]

def _insight_chips(
    users: list[dict],
    login_frame: pd.DataFrame,
    site_analytics: list[dict],
    jobs: list[dict],
    hackathons: list[dict],
) -> list[str]:
    today = datetime.now(LOCAL_TZ).date()
    last_7_days = today - timedelta(days=6)
    prior_7_days = today - timedelta(days=13)

    recent_signups = sum(
        1
        for user in users
        if (created_at := _first_datetime(user, "createdAt", "created_at"))
        and created_at.date() >= last_7_days
    )
    prior_signups = sum(
        1
        for user in users
        if (created_at := _first_datetime(user, "createdAt", "created_at"))
        and prior_7_days <= created_at.date() < last_7_days
    )

    growth_text = "flat"
    if prior_signups == 0 and recent_signups > 0:
        growth_text = f"{recent_signups} new signups in the last 7 days"
    elif prior_signups:
        delta = ((recent_signups - prior_signups) / prior_signups) * 100
        growth_text = f"{delta:+.0f}% signup growth vs previous 7 days"

    role_counts = {}
    for user in users:
        role = _normalize_user_role(user)
        role_counts[role] = role_counts.get(role, 0) + 1
    top_role = max(role_counts.items(), key=lambda item: item[1])[0] if role_counts else "candidate"

    category_counts = {}
    for visit in site_analytics:
        visit_at = _first_datetime(visit, "ts", "timestamp", "createdAt")
        if not visit_at or visit_at.date() < last_7_days:
            continue
        category = _normalize_text(visit.get("category")) or "other"
        category_counts[category] = category_counts.get(category, 0) + 1
    top_category = max(category_counts.items(), key=lambda item: item[1])[0] if category_counts else "dashboard"

    login_users_last_7 = int(login_frame[login_frame["Date"] >= last_7_days]["Value"].sum()) if not login_frame.empty else 0

    return [
        f"Last 7 days: {growth_text}",
        f"Largest user cohort: {top_role.title()}",
        f"Top engagement area this week: {top_category.replace('_', ' ').title()}",
        f"Login users across the last 7 days: {login_users_last_7}",
        f"Live opportunity inventory: {len(_active_jobs(jobs))} jobs and {len(_active_hackathons(hackathons))} hackathons",
    ]


def _recipient_options(users: list[dict], mode: str, identifier_text: str) -> list[dict]:
    active_users = [user for user in users if _is_active_user(user)]
    if mode == "All active users":
        return active_users
    if mode == "Candidates / students":
        return [user for user in active_users if _normalize_user_role(user) == "candidate"]
    if mode == "HR / recruiters":
        return [user for user in active_users if _normalize_user_role(user) == "hr"]
    if mode == "Mentors":
        return [user for user in active_users if _normalize_user_role(user) == "mentor"]
    if mode == "Admins":
        return [user for user in active_users if _normalize_user_role(user) == "admin"]
    if mode == "Specific user IDs":
        user_ids = {user_id.strip() for user_id in parse_user_ids(identifier_text) if user_id.strip()}
        return [user for user in active_users if str(user.get("doc_id", "")).strip() in user_ids]
    emails = {
        item.strip().lower()
        for item in identifier_text.replace("\n", ",").split(",")
        if item.strip()
    }
    return [
        user
        for user in active_users
        if _normalize_text(user.get("email")) in emails
    ]


def _default_digest_config() -> dict:
    return {
        "enabled": False,
        "audienceRoles": ["candidate", "student"],
        "includeJobs": True,
        "includeHackathons": True,
        "maxItems": 6,
        "jobLookbackDays": 7,
        "hackathonLookbackDays": 10,
        "subjectPrefix": "Daily Opportunity Digest",
        "introText": "Fresh roles, hackathons, and curated suggestions from Candiatescr.",
        "jobsUrl": "https://candiatescr.web.app/#/jobs",
        "hackathonsUrl": "https://candiatescr.web.app/#/hackathons",
        "deliveryProvider": _digest_delivery_provider(),
        "deliveryTimezone": "Asia/Kolkata",
        "updatedBy": "streamlit_super_admin",
    }


def _coerce_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _digest_candidates(users: list[dict], selected_roles: list[str]) -> list[dict]:
    recipients = []
    for user in users:
        if not _is_active_user(user):
            continue
        if _is_true(user.get("digestOptOut")):
            continue
        if not _normalize_text(user.get("email")):
            continue
        if not _user_role_matches(user, selected_roles):
            continue
        recipients.append(user)
    return recipients


def _digest_opportunity_rows(
    rows: list[dict],
    date_fields: list[str],
    lookback_days: int,
    max_items: int,
    label: str,
) -> list[dict]:
    threshold = datetime.now(LOCAL_TZ) - timedelta(days=lookback_days)
    prepared = []
    for row in rows:
        event_at = _first_datetime(row, *date_fields)
        if event_at and event_at < threshold:
            continue
        prepared.append((event_at or datetime.now(LOCAL_TZ), row))
    prepared.sort(key=lambda item: item[0], reverse=True)

    response = []
    for event_at, row in prepared[:max_items]:
        if label == "job":
            response.append(
                {
                    "Type": "Job",
                    "Title": str(row.get("jobTitle") or row.get("title") or row.get("doc_id") or "Untitled Job").strip(),
                    "Company": str(row.get("companyName") or row.get("company") or "Unknown Company").strip(),
                    "Status": str(row.get("status") or "active").strip(),
                    "When": _format_dt(event_at),
                }
            )
        else:
            response.append(
                {
                    "Type": "Hackathon",
                    "Title": str(row.get("name") or row.get("title") or row.get("doc_id") or "Untitled Hackathon").strip(),
                    "Company": str(row.get("companyName") or row.get("company") or "Unknown Host").strip(),
                    "Status": str(row.get("status") or "active").strip(),
                    "When": _format_dt(event_at),
                }
            )
    return response


def _digest_opportunity_docs(
    rows: list[dict],
    date_fields: list[str],
    lookback_days: int,
    max_items: int,
) -> list[dict]:
    threshold = datetime.now(LOCAL_TZ) - timedelta(days=lookback_days)
    prepared = []
    for row in rows:
        event_at = _first_datetime(row, *date_fields)
        if event_at and event_at < threshold:
            continue
        prepared.append((event_at or datetime.now(LOCAL_TZ), row))
    prepared.sort(key=lambda item: item[0], reverse=True)
    return [row for _, row in prepared[:max_items]]


def _build_digest_subject_local(
    config: dict,
    jobs: list[dict],
    hackathons: list[dict],
) -> str:
    prefix = str(config.get("subjectPrefix") or "Daily Opportunity Digest").strip()
    parts = []
    if jobs:
        parts.append(f"{len(jobs)} jobs")
    if hackathons:
        parts.append(f"{len(hackathons)} hackathons")
    return f"{prefix} | {' & '.join(parts)}" if parts else prefix


def _build_digest_text_local(
    recipient: dict,
    config: dict,
    jobs: list[dict],
    hackathons: list[dict],
) -> str:
    first_name = str(recipient.get("name") or "there").strip().split(" ")[0]
    lines = [
        f"Hello {first_name},",
        "",
        str(config.get("introText") or _default_digest_config()["introText"]).strip(),
        "",
    ]
    if jobs:
        lines.append("Latest Jobs:")
        for job in jobs:
            title = str(job.get("jobTitle") or job.get("title") or job.get("doc_id") or "Untitled Job").strip()
            company = str(job.get("companyName") or job.get("company") or "Candiatescr").strip()
            lines.append(f"- {title} | {company}")
        lines.append(f"Browse jobs: {str(config.get('jobsUrl') or _default_digest_config()['jobsUrl']).strip()}")
        lines.append("")
    if hackathons:
        lines.append("Live Hackathons:")
        for hackathon in hackathons:
            title = str(hackathon.get("name") or hackathon.get("title") or hackathon.get("doc_id") or "Untitled Hackathon").strip()
            company = str(hackathon.get("companyName") or hackathon.get("company") or "Candiatescr").strip()
            lines.append(f"- {title} | {company}")
        lines.append(
            "Browse hackathons: "
            f"{str(config.get('hackathonsUrl') or _default_digest_config()['hackathonsUrl']).strip()}"
        )
        lines.append("")
    lines.extend(["See you in the platform,", "The Candiatescr Team"])
    return "\n".join(lines)


def _build_digest_html_local(
    recipient: dict,
    config: dict,
    jobs: list[dict],
    hackathons: list[dict],
) -> str:
    first_name = str(recipient.get("name") or "there").strip().split(" ")[0]
    intro_text = str(config.get("introText") or _default_digest_config()["introText"]).strip()
    jobs_url = str(config.get("jobsUrl") or _default_digest_config()["jobsUrl"]).strip()
    hackathons_url = str(config.get("hackathonsUrl") or _default_digest_config()["hackathonsUrl"]).strip()

    def render_items(items: list[dict], kind: str) -> str:
        rows = []
        for item in items:
            title = (
                str(item.get("jobTitle") or item.get("title") or item.get("doc_id") or "Untitled Job").strip()
                if kind == "job"
                else str(item.get("name") or item.get("title") or item.get("doc_id") or "Untitled Hackathon").strip()
            )
            company = str(item.get("companyName") or item.get("company") or "Candiatescr").strip()
            rows.append(
                f"<li style='margin-bottom:8px;'><strong>{title}</strong><br><span style='color:#475569;'>{company}</span></li>"
            )
        return "".join(rows)

    jobs_block = (
        f"""
        <h3 style="margin:24px 0 10px 0;color:#0f172a;">Latest Jobs</h3>
        <ul style="padding-left:18px;margin:0;">{render_items(jobs, "job")}</ul>
        <p style="margin-top:12px;"><a href="{jobs_url}" style="color:#0f172a;font-weight:700;">Browse all jobs</a></p>
        """
        if jobs
        else ""
    )
    hackathons_block = (
        f"""
        <h3 style="margin:24px 0 10px 0;color:#0f172a;">Live Hackathons</h3>
        <ul style="padding-left:18px;margin:0;">{render_items(hackathons, "hackathon")}</ul>
        <p style="margin-top:12px;"><a href="{hackathons_url}" style="color:#0f172a;font-weight:700;">Browse all hackathons</a></p>
        """
        if hackathons
        else ""
    )

    return f"""
    <html>
      <body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
        <div style="max-width:680px;margin:24px auto;background:#ffffff;border-radius:18px;overflow:hidden;box-shadow:0 8px 30px rgba(15,23,42,0.12);">
          <div style="background:linear-gradient(135deg,#0f172a,#0ea5e9);padding:28px 24px;color:#ffffff;">
            <div style="font-size:12px;letter-spacing:1.1px;font-weight:700;opacity:0.85;">CANDIATESCR DAILY DIGEST</div>
            <h2 style="margin:10px 0 0 0;">Fresh opportunities for {first_name}</h2>
            <p style="margin:10px 0 0 0;opacity:0.92;">{intro_text}</p>
          </div>
          <div style="padding:24px;color:#1f2937;line-height:1.6;">
            {jobs_block}
            {hackathons_block}
          </div>
          <div style="background:#f8fafc;padding:14px 24px;font-size:12px;color:#6b7280;">
            You are receiving this because your Candiatescr digest is enabled.
          </div>
        </div>
      </body>
    </html>
    """


def _send_custom_email_via_gmail(
    to_email: str,
    subject: str,
    body: str,
    html_body: str,
) -> None:
    sender_email, sender_password = _require_smtp_credentials()

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{_smtp_from_name()} <{sender_email}>"
    message["To"] = to_email
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=45) as server:
        server.login(sender_email, sender_password)
        server.send_message(message)


def _run_streamlit_digest_delivery(
    config: dict,
    recipients: list[dict],
    jobs: list[dict],
    hackathons: list[dict],
) -> dict:
    run_key = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    started_at = datetime.utcnow()
    audit_errors: list[str] = []

    if not jobs and not hackathons:
        if not _best_effort_upsert(
            "digest_runs",
            run_key,
            {
                "status": "skipped",
                "reason": "no_content",
                "finishedAt": datetime.utcnow(),
                "triggeredBy": "streamlit_super_admin",
                "deliveryProvider": _digest_delivery_provider(),
            },
        ):
            audit_errors.append("Could not write digest_runs audit record.")
        return {
            "status": "skipped",
            "reason": "no_content",
            "runKey": run_key,
            "auditErrors": audit_errors,
        }

    if not recipients:
        if not _best_effort_upsert(
            "digest_runs",
            run_key,
            {
                "status": "skipped",
                "reason": "no_recipients",
                "finishedAt": datetime.utcnow(),
                "triggeredBy": "streamlit_super_admin",
                "deliveryProvider": _digest_delivery_provider(),
            },
        ):
            audit_errors.append("Could not write digest_runs audit record.")
        return {
            "status": "skipped",
            "reason": "no_recipients",
            "runKey": run_key,
            "auditErrors": audit_errors,
        }

    if not _best_effort_upsert(
        "digest_runs",
        run_key,
        {
            "status": "running",
            "startedAt": started_at,
            "triggeredBy": "streamlit_super_admin",
            "deliveryProvider": _digest_delivery_provider(),
        },
    ):
        audit_errors.append("Could not write digest_runs running status.")

    subject = _build_digest_subject_local(config, jobs, hackathons)
    sent_count = 0
    failures = []
    for recipient in recipients:
        email = str(recipient.get("email") or "").strip()
        if not email:
            continue
        try:
            _send_custom_email_via_gmail(
                to_email=email,
                subject=subject,
                body=_build_digest_text_local(recipient, config, jobs, hackathons),
                html_body=_build_digest_html_local(recipient, config, jobs, hackathons),
            )
            sent_count += 1
        except Exception as exc:
            failures.append({"email": email, "error": str(exc)})

    finished_at = datetime.utcnow()
    status = "completed" if not failures else "completed_with_errors"
    if not _best_effort_upsert(
        "digest_runs",
        run_key,
        {
            "status": status,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "triggeredBy": "streamlit_super_admin",
            "deliveryProvider": _digest_delivery_provider(),
            "totalRecipients": len(recipients),
            "sentCount": sent_count,
            "failedCount": len(failures),
            "jobsIncluded": len(jobs),
            "hackathonsIncluded": len(hackathons),
            "sampleErrors": failures[:20],
        },
    ):
        audit_errors.append("Could not write digest_runs completion status.")
    audit_errors.extend(
        _best_effort_batch_create(
            "notifications",
            [
                {
                    "title": "Daily digest dispatched",
                    "message": (
                        f"Sent {sent_count} daily digests with {len(jobs)} jobs and "
                        f"{len(hackathons)} hackathons via Streamlit Gmail SMTP."
                    ),
                    "timestamp": finished_at,
                    "type": "digest_job",
                    "recipientCount": sent_count,
                    "deliveryProvider": _digest_delivery_provider(),
                }
            ],
        )
    )
    return {
        "status": status,
        "runKey": run_key,
        "totalRecipients": len(recipients),
        "sentCount": sent_count,
        "failedCount": len(failures),
        "auditErrors": audit_errors,
    }


def _find_cleanup_candidates(
    batch_id: str | None,
    include_legacy: bool,
) -> list[dict]:
    rows: list[dict] = []
    for collection_name in MANAGED_COLLECTIONS:
        try:
            documents = list_documents(collection_name, page_size=200, max_documents=1000)
        except Exception as exc:
            rows.append(
                {
                    "collection": collection_name,
                    "doc_id": "",
                    "doc_path": "",
                    "label": f"Read failed: {exc}",
                    "match_type": "error",
                    "created_at": "",
                }
            )
            continue

        for document in documents:
            is_managed = _is_true(document.get("_adminSeed"))
            batch_matches = not batch_id or document.get("_adminSeedBatchId") == batch_id

            if is_managed and batch_matches:
                rows.append(
                    {
                        "collection": collection_name,
                        "doc_id": document.get("doc_id", ""),
                        "doc_path": document.get("doc_path", ""),
                        "label": _document_label(collection_name, document),
                        "match_type": "batch" if batch_id else "managed",
                        "created_at": document.get("_adminSeedCreatedAt", ""),
                    }
                )
                continue

            if include_legacy and not batch_id and _matches_legacy_generated_record(
                collection_name, document
            ):
                rows.append(
                    {
                        "collection": collection_name,
                        "doc_id": document.get("doc_id", ""),
                        "doc_path": document.get("doc_path", ""),
                        "label": _document_label(collection_name, document),
                        "match_type": "legacy",
                        "created_at": document.get("createdAt")
                        or document.get("timestamp")
                        or document.get("postedAt")
                        or "",
                    }
                )

    return [row for row in rows if row.get("match_type") != "error"] + [
        row for row in rows if row.get("match_type") == "error"
    ]


def render_sidebar_defaults():
    st.sidebar.markdown("## Seed Controls")
    st.sidebar.caption(f"Firebase project: `{get_project_id()}`")
    st.sidebar.caption(f"Auth state: {describe_auth_state()}")
    st.sidebar.text_input("Current batch ID", value=_ensure_batch_id(), disabled=True)
    if st.sidebar.button("Start New Batch", use_container_width=True):
        st.session_state["admin_seed_batch_id"] = _new_batch_id()
        st.rerun()
    st.sidebar.caption(
        "Generated records carry hidden metadata so this panel can safely clean them up later."
    )

    defaults = {
        "company_name": st.sidebar.text_input("Company name", value="Candiatescr Labs"),
        "company_id": st.sidebar.text_input("Company ID", value="cand-org-001"),
        "hr_id": st.sidebar.text_input("HR UID", value="seed-hr-admin"),
        "hr_email": st.sidebar.text_input("HR Email", value="seed-hr@candiatescr.local"),
        "org_id": st.sidebar.text_input("Org ID", value="cand-org-001"),
        "user_ids": parse_user_ids(
            st.sidebar.text_area(
                "Creator/User IDs",
                value="seed-user-001, seed-user-002, seed-user-003",
                help="Used for project ownership and team creators.",
            )
        ),
        "seed": int(
            st.sidebar.number_input(
                "Random seed",
                min_value=0,
                max_value=2147483647,
                value=20260309,
                step=1,
                help="Use any positive integer seed to reproduce the same dataset again.",
            )
        ),
        "batch_id": _ensure_batch_id(),
    }
    return defaults


def render_super_admin_overview_tab(data: dict):
    st.subheader("Super Admin Overview")
    top_left, top_right = st.columns([1, 3])
    with top_left:
        if st.button("Refresh Analytics Cache", use_container_width=True):
            clear_admin_cache()
            st.rerun()
    with top_right:
        st.caption(
            "Counts are based on Firestore reads cached for 2 minutes. Login counts use explicit `activities.login` events when available, otherwise dashboard visits from `site_analytics` are used as a proxy."
        )

    errors = data.get("errors", {})
    if errors:
        with st.expander("Collection read warnings"):
            for collection_name, error in errors.items():
                st.warning(f"{collection_name}: {error}")

    users = data.get("users", [])
    jobs = data.get("jobs", [])
    hackathons = data.get("hackathons", [])
    applications = data.get("job_applications", [])
    site_analytics = data.get("site_analytics", [])
    login_events, login_source = _extract_login_events(data.get("activities", []), site_analytics)

    today = datetime.now(LOCAL_TZ).date()
    yesterday = today - timedelta(days=1)

    signup_frame = _count_by_day(users, ["createdAt", "created_at"], days=14)
    login_frame = _count_by_day(login_events, ["eventAt"], days=14, unique_field="userId")
    jobs_frame = _count_by_day(_active_jobs(jobs), ["postedAt", "createdAt", "timestamp"], days=14)
    application_frame = _count_by_day(applications, ["appliedAt", "applied_at", "createdAt", "timestamp"], days=14)
    traffic_frame = _count_by_day(site_analytics, ["ts", "timestamp", "createdAt"], days=14)

    active_users_today = {
        str(entry.get("userId", "")).strip()
        for entry in site_analytics
        if (visit_at := _first_datetime(entry, "ts", "timestamp", "createdAt"))
        and visit_at.date() == today
        and str(entry.get("userId", "")).strip()
    }

    metric1, metric2, metric3, metric4, metric5 = st.columns(5)
    metric1.metric(
        "Signups Today",
        _series_value(signup_frame, today),
        _series_value(signup_frame, today) - _series_value(signup_frame, yesterday),
    )
    metric2.metric(
        "Login Users Today",
        _series_value(login_frame, today),
        _series_value(login_frame, today) - _series_value(login_frame, yesterday),
    )
    metric3.metric("Active Users Today", len(active_users_today))
    metric4.metric(
        "Applications Today",
        _series_value(application_frame, today),
        _series_value(application_frame, today) - _series_value(application_frame, yesterday),
    )
    metric5.metric("Open Jobs", len(_active_jobs(jobs)))

    stat1, stat2, stat3, stat4 = st.columns(4)
    stat1.markdown(
        f'<div class="mini-card"><h3>TOTAL USERS</h3><strong>{len(users)}</strong></div>',
        unsafe_allow_html=True,
    )
    stat2.markdown(
        f'<div class="mini-card"><h3>TOTAL HACKATHONS</h3><strong>{len(_active_hackathons(hackathons))}</strong></div>',
        unsafe_allow_html=True,
    )
    stat3.markdown(
        f'<div class="mini-card"><h3>TOTAL APPLICATIONS</h3><strong>{len(applications)}</strong></div>',
        unsafe_allow_html=True,
    )
    stat4.markdown(
        f'<div class="mini-card"><h3>LOGIN DATA SOURCE</h3><strong>{login_source}</strong></div>',
        unsafe_allow_html=True,
    )

    st.markdown("")
    trend_frame = signup_frame.rename(columns={"Value": "Signups"}).copy()
    trend_frame["Login Users"] = login_frame["Value"]
    trend_frame["Jobs Posted"] = jobs_frame["Value"]
    trend_frame["Applications"] = application_frame["Value"]
    trend_frame["Visits"] = traffic_frame["Value"]
    st.markdown("### 14-Day Growth Trends")
    st.line_chart(trend_frame.set_index("Date"), use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Role Distribution")
        role_counts = {}
        for user in users:
            role = _normalize_user_role(user).title()
            role_counts[role] = role_counts.get(role, 0) + 1
        if role_counts:
            role_frame = (
                pd.DataFrame(
                    [{"Role": role, "Users": count} for role, count in sorted(role_counts.items(), key=lambda item: item[1], reverse=True)]
                )
                .set_index("Role")
            )
            st.bar_chart(role_frame, use_container_width=True)
        else:
            st.info("No user profiles are available yet.")

    with col2:
        st.markdown("### Site Analytics Categories")
        category_counts = {}
        for visit in site_analytics:
            visit_at = _first_datetime(visit, "ts", "timestamp", "createdAt")
            if not visit_at or visit_at.date() < today - timedelta(days=6):
                continue
            category = _normalize_text(visit.get("category")) or "other"
            label = category.replace("_", " ").title()
            category_counts[label] = category_counts.get(label, 0) + 1
        if category_counts:
            category_frame = (
                pd.DataFrame(
                    [{"Category": name, "Visits": count} for name, count in sorted(category_counts.items(), key=lambda item: item[1], reverse=True)]
                )
                .set_index("Category")
            )
            st.bar_chart(category_frame, use_container_width=True)
        else:
            st.info("No site analytics events have been captured in the last 7 days.")

    st.markdown("### Key Insights")
    insight_html = "".join(
        f'<span class="insight-chip">{chip}</span>'
        for chip in _insight_chips(users, login_frame, site_analytics, jobs, hackathons)
    )
    st.markdown(insight_html, unsafe_allow_html=True)

    st.markdown("### Recent Activity")
    activity_rows = _activity_feed_rows(data)
    if activity_rows:
        activity_frame = pd.DataFrame(activity_rows)
        activity_frame["Time"] = activity_frame["Time"].apply(_format_dt)
        st.dataframe(activity_frame, use_container_width=True, hide_index=True)
    else:
        st.info("No recent activity found yet.")

    with st.expander("Collection coverage"):
        coverage_rows = []
        for collection_name, limit in ANALYTICS_COLLECTION_LIMITS.items():
            coverage_rows.append(
                {
                    "Collection": collection_name,
                    "Loaded docs": len(data.get(collection_name, [])),
                    "Read limit": limit,
                }
            )
        st.dataframe(pd.DataFrame(coverage_rows), use_container_width=True, hide_index=True)

def render_bulk_notifications_tab(data: dict):
    st.subheader("Bulk Notification Studio")
    st.caption(
        "This sends dashboard alerts to selected users by writing to the `alerts` collection and logs the campaign in `notifications`."
    )

    users = data.get("users", [])
    audience = st.selectbox(
        "Audience",
        [
            "All active users",
            "Candidates / students",
            "HR / recruiters",
            "Mentors",
            "Admins",
            "Specific user IDs",
            "Specific emails",
        ],
    )
    identifier_text = ""
    if audience in {"Specific user IDs", "Specific emails"}:
        label = "User IDs" if audience == "Specific user IDs" else "Emails"
        placeholder = "uid-001, uid-002" if audience == "Specific user IDs" else "person@company.com, user@school.edu"
        identifier_text = st.text_area(label, placeholder=placeholder, height=110)

    title = st.text_input("Notification title", value="Platform Update")
    alert_type = st.selectbox(
        "Alert type",
        ["announcement", "admin", "digest", "security", "meeting", "interview"],
        index=0,
    )
    body = st.text_area(
        "Message body",
        value="A new update is available in your Candiatescr dashboard.",
        height=140,
    )
    cta_url = st.text_input(
        "Optional call-to-action URL",
        value="https://candiatescr.web.app/#/dashboard",
    )

    recipients = _recipient_options(users, audience, identifier_text)
    recipient_preview = pd.DataFrame(
        [
            {
                "UID": user.get("doc_id", ""),
                "Name": user.get("name", ""),
                "Email": user.get("email", ""),
                "Role": _normalize_user_role(user),
                "Status": user.get("status", "active"),
            }
            for user in recipients[:25]
        ]
    )

    info1, info2, info3 = st.columns(3)
    info1.metric("Recipients", len(recipients))
    info2.metric("Active users", len([user for user in users if _is_active_user(user)]))
    info3.metric("Sample shown", min(len(recipients), 25))

    if not recipient_preview.empty:
        st.dataframe(recipient_preview, use_container_width=True, hide_index=True)
    else:
        st.info("No recipients match the selected audience yet.")

    if st.button("Send Bulk Notification", type="primary", use_container_width=True):
        if not body.strip():
            st.warning("Message body is required.")
            return
        if not recipients:
            st.warning("No users match the selected audience.")
            return

        campaign_id = f"broadcast-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        full_message = f"{title.strip()}: {body.strip()}" if title.strip() else body.strip()
        alert_docs = [
            {
                "userId": user.get("doc_id", ""),
                "type": alert_type,
                "message": full_message,
                "createdAt": datetime.utcnow(),
                "read": False,
                "metadata": {
                    "title": title.strip(),
                    "ctaUrl": cta_url.strip(),
                    "campaignId": campaign_id,
                    "source": "streamlit_super_admin",
                    "audience": audience,
                },
            }
            for user in recipients
            if str(user.get("doc_id", "")).strip()
        ]
        saved_ids, errors = batch_create("alerts", alert_docs)

        audit_doc = {
            "doc_id": campaign_id,
            "title": title.strip() or "Admin Broadcast",
            "message": body.strip(),
            "timestamp": datetime.utcnow(),
            "type": "admin_broadcast",
            "recipientCount": len(saved_ids),
            "audience": audience,
            "ctaUrl": cta_url.strip(),
            "createdBy": "streamlit_super_admin",
        }
        batch_create("notifications", [audit_doc])
        clear_admin_cache()

        if saved_ids:
            st.success(f"Sent {len(saved_ids)} alerts.")
        if errors:
            st.error(f"{len(errors)} alerts failed.")
            for error in errors[:3]:
                st.code(error)


def render_digest_tab(data: dict):
    st.subheader("Daily Opportunity Digest")
    st.caption(
        "This digest sends directly from the Streamlit admin panel through Gmail SMTP. Use a Gmail sender ID and password or app password below."
    )

    config = {**_default_digest_config(), **(data.get("digest_config") or {})}
    digest_runs = sorted(
        data.get("digest_runs", []),
        key=lambda row: str(row.get("doc_id", "")),
        reverse=True,
    )
    latest_run = digest_runs[0] if digest_runs else {}
    load_errors = data.get("errors", {})
    users = data.get("users", [])
    jobs = _active_jobs(data.get("jobs", []))
    hackathons = _active_hackathons(data.get("hackathons", []))

    if load_errors.get("digest_runs"):
        st.info(
            "Firestore blocked reading `digest_runs`, so Last run history may be incomplete. "
            "Email sending can still work."
        )

    enabled = st.checkbox("Enable daily digest", value=_is_true(config.get("enabled")))
    audience_roles = st.multiselect(
        "Audience roles",
        DIGEST_ROLE_OPTIONS,
        default=_coerce_list(config.get("audienceRoles")) or ["candidate", "student"],
    )
    include_jobs = st.checkbox("Include jobs", value=_is_true(config.get("includeJobs", True)))
    include_hackathons = st.checkbox(
        "Include hackathons", value=_is_true(config.get("includeHackathons", True))
    )

    cfg1, cfg2, cfg3 = st.columns(3)
    max_items = int(
        cfg1.number_input(
            "Max items per digest",
            min_value=1,
            max_value=12,
            value=int(config.get("maxItems", 6) or 6),
        )
    )
    job_lookback_days = int(
        cfg2.number_input(
            "Job lookback days",
            min_value=1,
            max_value=30,
            value=int(config.get("jobLookbackDays", 7) or 7),
        )
    )
    hackathon_lookback_days = int(
        cfg3.number_input(
            "Hackathon lookback days",
            min_value=1,
            max_value=45,
            value=int(config.get("hackathonLookbackDays", 10) or 10),
        )
    )

    subject_prefix = st.text_input(
        "Email subject prefix",
        value=str(config.get("subjectPrefix", "Daily Opportunity Digest")),
    )
    intro_text = st.text_area(
        "Digest intro text",
        value=str(config.get("introText", _default_digest_config()["introText"])),
        height=110,
    )

    url1, url2 = st.columns(2)
    jobs_url = url1.text_input(
        "Jobs CTA URL",
        value=str(config.get("jobsUrl", _default_digest_config()["jobsUrl"])),
    )
    hackathons_url = url2.text_input(
        "Hackathons CTA URL",
        value=str(config.get("hackathonsUrl", _default_digest_config()["hackathonsUrl"])),
    )

    st.markdown("### Sender Credentials")
    sender_col, password_col = st.columns(2)
    sender_col.text_input(
        "Gmail sender ID",
        value=_smtp_sender_email(),
        key="admin_mail_sender_email",
        help="This Gmail account sends the digest directly from Streamlit.",
    )
    password_col.text_input(
        "Gmail password / app password",
        value=_smtp_sender_password(),
        key="admin_mail_sender_password",
        type="password",
        help="Use a Gmail app password if Google blocks direct password sign-in.",
    )
    if _smtp_sender_email() and _smtp_sender_password():
        st.caption(
            f"Direct delivery is ready. Sender: `{_smtp_sender_email()}` via Gmail SMTP."
        )
    else:
        st.warning(
            "Enter a Gmail sender email and password to send digest emails from this admin panel."
        )

    digest_recipients = _digest_candidates(users, audience_roles)
    digest_jobs = (
        _digest_opportunity_docs(
            jobs,
            ["postedAt", "createdAt", "timestamp"],
            job_lookback_days,
            max_items,
        )
        if include_jobs
        else []
    )
    digest_hackathons = (
        _digest_opportunity_docs(
            hackathons,
            ["createdAt", "registrationDeadline", "deadline", "startDate"],
            hackathon_lookback_days,
            max_items,
        )
        if include_hackathons
        else []
    )
    preview_rows: list[dict] = []
    if digest_jobs:
        preview_rows.extend(
            _digest_opportunity_rows(
                digest_jobs,
                ["postedAt", "createdAt", "timestamp"],
                job_lookback_days,
                max_items,
                "job",
            )
        )
    if digest_hackathons:
        preview_rows.extend(
            _digest_opportunity_rows(
                digest_hackathons,
                ["createdAt", "registrationDeadline", "deadline", "startDate"],
                hackathon_lookback_days,
                max_items,
                "hackathon",
            )
        )

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Digest recipients", len(digest_recipients))
    d2.metric("Jobs in queue", len([row for row in preview_rows if row["Type"] == "Job"]))
    d3.metric("Hackathons in queue", len([row for row in preview_rows if row["Type"] == "Hackathon"]))
    d4.metric("Last run", str(latest_run.get("status", "not_run")).replace("_", " ").title())

    if latest_run:
        latest_finished = _format_dt(
            _first_datetime(latest_run, "finishedAt", "startedAt")
        )
        st.caption(
            f"Latest digest run: {latest_run.get('status', 'unknown')} | "
            f"sent {latest_run.get('sentCount', 0)} of "
            f"{latest_run.get('totalRecipients', 0)} recipients | "
            f"{latest_finished}"
        )

    action1, action2 = st.columns(2)
    if action1.button("Save Digest Configuration", type="primary", use_container_width=True):
        payload = {
            "enabled": enabled,
            "audienceRoles": audience_roles,
            "includeJobs": include_jobs,
            "includeHackathons": include_hackathons,
            "maxItems": max_items,
            "jobLookbackDays": job_lookback_days,
            "hackathonLookbackDays": hackathon_lookback_days,
            "subjectPrefix": subject_prefix.strip() or "Daily Opportunity Digest",
            "introText": intro_text.strip() or _default_digest_config()["introText"],
            "jobsUrl": jobs_url.strip() or _default_digest_config()["jobsUrl"],
            "hackathonsUrl": hackathons_url.strip() or _default_digest_config()["hackathonsUrl"],
            "deliveryProvider": _digest_delivery_provider(),
            "deliveryTimezone": "Asia/Kolkata",
            "updatedAt": datetime.utcnow(),
            "updatedBy": "streamlit_super_admin",
        }
        upsert_document("system_config", DIGEST_CONFIG_DOC, payload)
        batch_create(
            "notifications",
            [
                {
                    "title": "Digest configuration updated",
                    "message": f"Daily digest settings changed. Enabled={enabled}, recipients={len(digest_recipients)}.",
                    "timestamp": datetime.utcnow(),
                    "type": "digest_admin",
                    "recipientCount": len(digest_recipients),
                    "createdBy": "streamlit_super_admin",
                }
            ],
        )
        clear_admin_cache()
        st.success("Daily digest configuration saved.")

    if action2.button("Send Digest Now", use_container_width=True):
        try:
            response = _run_streamlit_digest_delivery(
                config={
                    **config,
                    "enabled": enabled,
                    "audienceRoles": audience_roles,
                    "includeJobs": include_jobs,
                    "includeHackathons": include_hackathons,
                    "maxItems": max_items,
                    "jobLookbackDays": job_lookback_days,
                    "hackathonLookbackDays": hackathon_lookback_days,
                    "subjectPrefix": subject_prefix.strip() or "Daily Opportunity Digest",
                    "introText": intro_text.strip() or _default_digest_config()["introText"],
                    "jobsUrl": jobs_url.strip() or _default_digest_config()["jobsUrl"],
                    "hackathonsUrl": hackathons_url.strip() or _default_digest_config()["hackathonsUrl"],
                    "deliveryProvider": _digest_delivery_provider(),
                },
                recipients=digest_recipients,
                jobs=digest_jobs,
                hackathons=digest_hackathons,
            )
            clear_admin_cache()
            status = str(response.get("status", "success"))
            if status == "skipped":
                reason = str(response.get("reason", "skipped")).replace("_", " ")
                st.warning(f"Digest not sent: {reason}.")
            else:
                st.success(
                    "Digest sent directly from Streamlit. "
                    f"Sent {response.get('sentCount', 0)} of "
                    f"{response.get('totalRecipients', response.get('recipientCount', 0))} recipients."
                )
            audit_errors = response.get("auditErrors", [])
            if audit_errors:
                st.warning(
                    "Email delivery finished, but some Firestore audit writes failed: "
                    + " | ".join(str(error) for error in audit_errors[:3])
                )
        except Exception as exc:
            st.error(f"Digest send failed: {exc}")

    st.markdown("### Digest Audience Preview")
    if digest_recipients:
        preview_frame = pd.DataFrame(
            [
                {
                    "UID": user.get("doc_id", ""),
                    "Name": user.get("name", ""),
                    "Email": user.get("email", ""),
                    "Role": _normalize_user_role(user),
                    "Last Login": _format_dt(_first_datetime(user, "lastLoginAt", "lastActiveAt")),
                }
                for user in digest_recipients[:25]
            ]
        )
        st.dataframe(preview_frame, use_container_width=True, hide_index=True)
    else:
        st.info("No active users currently match the selected digest audience.")

    st.markdown("### Opportunity Queue Preview")
    if preview_rows:
        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No jobs or hackathons currently match the digest filters.")

    st.markdown(
        '<p class="small-note">Recommended send window: daily at 08:00 Asia/Kolkata. Users are excluded automatically when `digestOptOut` is true, and delivery now runs directly from this Streamlit app via Gmail SMTP.</p>',
        unsafe_allow_html=True,
    )


def render_jobs_tab(defaults: dict):
    st.subheader("Jobs Seeder")
    count = int(st.number_input("Jobs to generate", 1, 500, 12, key="jobs_count"))
    job_docs = generate_jobs(
        count=count,
        company_name=defaults["company_name"],
        hr_id=defaults["hr_id"],
        hr_email=defaults["hr_email"],
        org_id=defaults["org_id"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 1,
    )
    show_preview(
        "Jobs preview",
        job_docs[: min(len(job_docs), 12)],
        ["doc_id", "jobTitle", "companyName", "location", "experienceLevel", "deadline"],
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Save Jobs", key="save_jobs"):
            save_collection("jobs", job_docs)
    with col2:
        st.caption("Writes to the Flutter root collection `jobs`.")


def render_hackathons_tab(defaults: dict):
    st.subheader("Hackathons Seeder")
    count = int(
        st.number_input("Hackathons to generate", 1, 250, 6, key="hackathons_count")
    )
    hack_docs = generate_hackathons(
        count=count,
        company_name=defaults["company_name"],
        company_id=defaults["company_id"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 2,
    )
    show_preview(
        "Hackathons preview",
        hack_docs[: min(len(hack_docs), 10)],
        ["doc_id", "name", "companyName", "status", "teamSizeMin", "teamSizeMax", "prize"],
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Save Hackathons", key="save_hackathons"):
            save_collection("hackathons", hack_docs)
    with col2:
        st.caption("Writes to the Flutter root collection `hackathons`.")


def render_teams_tab(defaults: dict):
    st.subheader("Teams Seeder")
    count = int(st.number_input("Teams to generate", 1, 500, 10, key="teams_count"))
    hackathon_names = [
        "FutureStack Challenge",
        "AI Mission Lab",
        "CivicCode Hack",
    ]
    team_docs = generate_teams(
        count=count,
        creator_ids=defaults["user_ids"],
        hackathon_names=hackathon_names,
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 3,
    )
    show_preview(
        "Teams preview",
        team_docs[: min(len(team_docs), 12)],
        ["doc_id", "teamName", "creatorId", "hackathonName", "requiredRoles", "createdAt"],
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Save Teams", key="save_teams"):
            save_collection("teams", team_docs)
    with col2:
        st.caption("Writes to the Flutter root collection `teams`.")


def render_projects_tab(defaults: dict):
    st.subheader("Projects Seeder")
    count = int(
        st.number_input("Projects to generate", 1, 500, 10, key="projects_count")
    )
    project_docs = generate_projects(
        count=count,
        user_ids=defaults["user_ids"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 4,
    )
    show_preview(
        "Projects preview",
        project_docs[: min(len(project_docs), 12)],
        ["doc_id", "title", "userId", "status", "projectLink"],
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Save Projects", key="save_projects"):
            save_collection("projects", project_docs)
    with col2:
        st.caption("Writes to the Flutter root collection `projects`.")


def render_full_pack_tab(defaults: dict):
    st.subheader("Full Seed Pack")
    col1, col2, col3, col4 = st.columns(4)
    jobs_count = int(col1.number_input("Jobs", 1, 500, 15, key="pack_jobs"))
    hack_count = int(col2.number_input("Hackathons", 1, 250, 5, key="pack_hacks"))
    team_count = int(col3.number_input("Teams", 1, 500, 12, key="pack_teams"))
    project_count = int(col4.number_input("Projects", 1, 500, 14, key="pack_projects"))

    hack_docs = generate_hackathons(
        count=hack_count,
        company_name=defaults["company_name"],
        company_id=defaults["company_id"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 10,
    )
    hackathon_names = [doc["name"] for doc in hack_docs]
    jobs_docs = generate_jobs(
        count=jobs_count,
        company_name=defaults["company_name"],
        hr_id=defaults["hr_id"],
        hr_email=defaults["hr_email"],
        org_id=defaults["org_id"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 11,
    )
    team_docs = generate_teams(
        count=team_count,
        creator_ids=defaults["user_ids"],
        hackathon_names=hackathon_names,
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 12,
    )
    project_docs = generate_projects(
        count=project_count,
        user_ids=defaults["user_ids"],
        seed_batch_id=defaults["batch_id"],
        seed=defaults["seed"] + 13,
    )

    pack_preview = pd.DataFrame(
        [
            {"Collection": "jobs", "Count": len(jobs_docs)},
            {"Collection": "hackathons", "Count": len(hack_docs)},
            {"Collection": "teams", "Count": len(team_docs)},
            {"Collection": "projects", "Count": len(project_docs)},
        ]
    )
    st.dataframe(pack_preview, use_container_width=True, hide_index=True)
    if st.button("Save Full Seed Pack", key="save_full_pack", type="primary"):
        save_collection("jobs", jobs_docs)
        save_collection("hackathons", hack_docs)
        save_collection("teams", team_docs)
        save_collection("projects", project_docs)

def render_cleanup_tab(defaults: dict):
    st.subheader("Cleanup Studio")
    st.caption(
        "Delete only records created by this panel. Original data is excluded unless it matches the exact legacy fingerprints from the older generator."
    )

    scope = st.radio(
        "Cleanup scope",
        ["Current batch only", "All admin-created records"],
        horizontal=True,
        help="Current batch is the safest option. The all-records option also catches older panel records written before hidden metadata was added.",
    )
    include_legacy = scope == "All admin-created records"
    scoped_batch_id = defaults["batch_id"] if scope == "Current batch only" else None

    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("Scan Deletable Records", key="scan_cleanup"):
            st.session_state["cleanup_records"] = _find_cleanup_candidates(
                batch_id=scoped_batch_id,
                include_legacy=include_legacy,
            )
            st.session_state["cleanup_scope"] = scope
    with col2:
        if scope == "Current batch only":
            st.caption(f"Current batch: `{defaults['batch_id']}`")
        else:
            st.caption(
                "Includes hidden managed records plus exact legacy fingerprints from the previous generator."
            )

    rows = st.session_state.get("cleanup_records", [])
    if st.session_state.get("cleanup_scope") != scope:
        rows = []

    if not rows:
        st.info("Run a scan to preview which documents are eligible for deletion.")
        return

    error_rows = [row for row in rows if row.get("match_type") == "error"]
    preview_rows = [row for row in rows if row.get("match_type") != "error"]

    if preview_rows:
        preview_frame = pd.DataFrame(preview_rows)
        st.dataframe(
            preview_frame[
                ["collection", "doc_id", "label", "match_type", "created_at"]
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Records queued for deletion: {len(preview_rows)}")

        if st.button(
            "Delete Previewed Records",
            key="delete_cleanup",
            type="primary",
        ):
            deleted_count, errors = batch_delete(
                [row["doc_path"] for row in preview_rows if row.get("doc_path")]
            )
            clear_admin_cache()
            if deleted_count:
                st.success(f"Deleted {deleted_count} generated records.")
            if errors:
                st.error(f"{len(errors)} deletes failed.")
                for error in errors[:3]:
                    st.code(error)
            st.session_state["cleanup_records"] = []
    else:
        st.success("No generated records matched the selected cleanup scope.")

    if error_rows:
        st.warning(f"{len(error_rows)} collections could not be scanned.")
        for row in error_rows[:3]:
            st.code(row["label"])


def main():
    inject_styles()
    require_access()
    defaults = render_sidebar_defaults()

    st.markdown(
        """
        <div class="hero">
            <h1>Candiatescr Super Admin Studio</h1>
            <p>
                Operations console for super-admin analytics, growth insights, bulk user alerts,
                daily opportunity digest control, and production-style data seeding for the
                Flutter Firebase project.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    stat1, stat2, stat3 = st.columns(3)
    stat1.markdown(
        f'<div class="mini-card"><h3>TARGET PROJECT</h3><strong>{get_project_id()}</strong></div>',
        unsafe_allow_html=True,
    )
    stat2.markdown(
        '<div class="mini-card"><h3>WRITE MODE</h3><strong>Firestore REST + Gmail SMTP</strong></div>',
        unsafe_allow_html=True,
    )
    stat3.markdown(
        f'<div class="mini-card"><h3>AUTH</h3><strong>{describe_auth_state()}</strong></div>',
        unsafe_allow_html=True,
    )

    super_admin_data = _load_super_admin_data()

    st.markdown("")
    tabs = st.tabs(
        [
            "Overview",
            "Bulk Notifications",
            "Daily Digest",
            "Jobs",
            "Hackathons",
            "Teams",
            "Projects",
            "Full Pack",
            "Cleanup",
        ]
    )
    with tabs[0]:
        render_super_admin_overview_tab(super_admin_data)
    with tabs[1]:
        render_bulk_notifications_tab(super_admin_data)
    with tabs[2]:
        render_digest_tab(super_admin_data)
    with tabs[3]:
        render_jobs_tab(defaults)
    with tabs[4]:
        render_hackathons_tab(defaults)
    with tabs[5]:
        render_teams_tab(defaults)
    with tabs[6]:
        render_projects_tab(defaults)
    with tabs[7]:
        render_full_pack_tab(defaults)
    with tabs[8]:
        render_cleanup_tab(defaults)

    with st.expander("Setup notes"):
        st.markdown(
            """
            Run with:

            ```bash
            streamlit run candiatescr_admin_panel/app.py
            ```

            Recommended secrets or environment variables:

            - `ADMIN_PANEL_ACCESS_KEY`
            - `FLUTTER_FIREBASE_ID_TOKEN` or `CANDIATESCR_FIREBASE_ID_TOKEN`
            - or `FLUTTER_SYNC_EMAIL` + `FLUTTER_SYNC_PASSWORD`
            - optional `CANDIATESCR_FIREBASE_WEB_API_KEY`
            - optional `GMAIL_ADDRESS` or `GMAIL_EMAIL`, plus `GMAIL_APP_PASSWORD`
            - or `SMTP_EMAIL` + `SMTP_PASSWORD`
            - or `EMAIL_USER` + `EMAIL_PASS`

            The Daily Digest tab sends mail directly from Streamlit through Gmail SMTP.
            For Gmail, use an app password if normal password login is blocked.
            """
        )


if __name__ == "__main__":
    main()
