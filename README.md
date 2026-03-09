# Candiatescr Admin Data Studio

Standalone Streamlit admin panel for seeding the Flutter Firebase project `candiatescr`.

It writes demo data into these root Firestore collections:

- `jobs`
- `hackathons`
- `teams`
- `projects`

## Run

```bash
streamlit run candiatescr_admin_panel/app.py
```

## Recommended configuration

Use Streamlit secrets or environment variables:

- `ADMIN_PANEL_ACCESS_KEY`
- `FLUTTER_FIREBASE_ID_TOKEN` or `CANDIATESCR_FIREBASE_ID_TOKEN`
- or `FLUTTER_SYNC_EMAIL` and `FLUTTER_SYNC_PASSWORD`
- optional `CANDIATESCR_FIREBASE_WEB_API_KEY`

## Files

- `app.py`: standalone Streamlit UI
- `firebase_client.py`: Firestore REST writer for `candiatescr`
- `generators.py`: dummy-data builders for jobs, hackathons, teams, and projects
