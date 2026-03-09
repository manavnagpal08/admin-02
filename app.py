from __future__ import annotations

import pandas as pd
import streamlit as st

from firebase_client import batch_create, describe_auth_state, get_access_key, get_project_id
from generators import (
    generate_hackathons,
    generate_jobs,
    generate_projects,
    generate_teams,
    parse_user_ids,
)


st.set_page_config(
    page_title="Candiatescr Admin Data Studio",
    page_icon="C",
    layout="wide",
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
            padding: 1.4rem 1.6rem;
            border-radius: 20px;
            background: linear-gradient(135deg, #0f172a 0%, #133b5c 52%, #0ea5e9 100%);
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
            max-width: 760px;
            color: rgba(255,255,255,0.82);
        }
        .mini-card {
            background: rgba(255,255,255,0.82);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 18px;
            padding: 1rem;
            box-shadow: 0 14px 28px rgba(15, 23, 42, 0.07);
        }
        .mini-card h3 {
            margin: 0;
            font-size: 0.85rem;
            letter-spacing: 0.04em;
            color: #475569;
        }
        .mini-card strong {
            display: block;
            margin-top: 0.25rem;
            font-size: 1.4rem;
            color: #0f172a;
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


def save_collection(collection_name: str, documents: list[dict]):
    if not documents:
        st.warning(f"No {collection_name} records to save.")
        return

    saved_ids, errors = batch_create(collection_name, documents)
    if saved_ids:
        st.success(f"Saved {len(saved_ids)} documents to `{collection_name}`.")
        st.caption("Sample document IDs: " + ", ".join(saved_ids[:5]))
    if errors:
        st.error(f"{len(errors)} writes failed for `{collection_name}`.")
        for error in errors[:3]:
            st.code(error)


def render_sidebar_defaults():
    st.sidebar.markdown("## Seed Controls")
    st.sidebar.caption(f"Firebase project: `{get_project_id()}`")
    st.sidebar.caption(f"Auth state: {describe_auth_state()}")

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
                help="Use any positive integer seed to reproduce the same fake dataset.",
            )
        ),
    }
    return defaults


def render_jobs_tab(defaults: dict):
    st.subheader("Jobs Seeder")
    count = int(st.number_input("Jobs to generate", 1, 500, 12, key="jobs_count"))
    job_docs = generate_jobs(
        count=count,
        company_name=defaults["company_name"],
        hr_id=defaults["hr_id"],
        hr_email=defaults["hr_email"],
        org_id=defaults["org_id"],
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
        seed=defaults["seed"] + 10,
    )
    hackathon_names = [doc["name"] for doc in hack_docs]
    jobs_docs = generate_jobs(
        count=jobs_count,
        company_name=defaults["company_name"],
        hr_id=defaults["hr_id"],
        hr_email=defaults["hr_email"],
        org_id=defaults["org_id"],
        seed=defaults["seed"] + 11,
    )
    team_docs = generate_teams(
        count=team_count,
        creator_ids=defaults["user_ids"],
        hackathon_names=hackathon_names,
        seed=defaults["seed"] + 12,
    )
    project_docs = generate_projects(
        count=project_count,
        user_ids=defaults["user_ids"],
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


def main():
    inject_styles()
    require_access()
    defaults = render_sidebar_defaults()

    st.markdown(
        """
        <div class="hero">
            <h1>Candiatescr Admin Data Studio</h1>
            <p>
                Standalone Streamlit panel for seeding the Flutter Firebase project with
                jobs, hackathons, teams, and projects. Use this when you want the
                app to feel populated during demos, QA, or design reviews.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    stat1, stat2, stat3 = st.columns(3)
    stat1.markdown(
        '<div class="mini-card"><h3>TARGET PROJECT</h3><strong>candiatescr</strong></div>',
        unsafe_allow_html=True,
    )
    stat2.markdown(
        '<div class="mini-card"><h3>WRITE MODE</h3><strong>Firestore REST</strong></div>',
        unsafe_allow_html=True,
    )
    stat3.markdown(
        f'<div class="mini-card"><h3>AUTH</h3><strong>{describe_auth_state()}</strong></div>',
        unsafe_allow_html=True,
    )

    st.markdown("")
    tabs = st.tabs(["Jobs", "Hackathons", "Teams", "Projects", "Full Pack"])
    with tabs[0]:
        render_jobs_tab(defaults)
    with tabs[1]:
        render_hackathons_tab(defaults)
    with tabs[2]:
        render_teams_tab(defaults)
    with tabs[3]:
        render_projects_tab(defaults)
    with tabs[4]:
        render_full_pack_tab(defaults)

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
            """
        )


if __name__ == "__main__":
    main()
