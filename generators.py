import random
import uuid
from datetime import datetime, timedelta


JOB_TITLES = [
    "Software Engineer",
    "Frontend Engineer",
    "Backend Engineer",
    "Full Stack Developer",
    "ML Engineer",
    "Data Analyst",
    "Cloud Engineer",
    "DevOps Engineer",
    "Product Designer",
    "QA Engineer",
    "Mobile Developer",
    "Security Analyst",
]

COMPANIES = [
    "NovaGrid Labs",
    "OrbitStack",
    "BrightForge",
    "BluePeak Systems",
    "PixelDock",
    "SignalNest",
    "AsterLoop",
    "CoreHarbor",
]

LOCATIONS = [
    "Remote",
    "Bengaluru",
    "Hyderabad",
    "Pune",
    "Mumbai",
    "Delhi NCR",
    "Chennai",
]

SKILLS = [
    "Python",
    "Dart",
    "Flutter",
    "React",
    "Node.js",
    "Firebase",
    "Machine Learning",
    "SQL",
    "AWS",
    "Docker",
    "Kubernetes",
    "Figma",
    "GraphQL",
    "REST APIs",
    "System Design",
]

JOB_TYPES = ["Full-time", "Internship", "Contract"]
EXPERIENCE_LEVELS = ["Entry", "Associate", "Mid", "Senior"]
JOB_CATEGORIES = ["Engineering", "Design", "Data", "Product"]

HACKATHON_NAMES = [
    "BuildOps Sprint",
    "FutureStack Challenge",
    "AI Mission Lab",
    "CloudRush Arena",
    "GreenTech Jam",
    "CivicCode Hack",
    "CreatorOS Buildathon",
]

TEAM_ROLES = [
    "Frontend",
    "Backend",
    "ML Engineer",
    "UI Designer",
    "Product Thinker",
    "Mobile Builder",
]

PROJECT_TOPICS = [
    "AI Resume Copilot",
    "Hackathon Team Matcher",
    "Realtime Hiring Dashboard",
    "Interview Signal Engine",
    "Campus Talent Graph",
    "Portfolio Ranking Toolkit",
]

PROJECT_STATUSES = ["In Progress", "Completed", "Prototype", "Research"]

ASSET_SEEDS = [
    "aurora",
    "grid",
    "stack",
    "forge",
    "orbit",
    "wave",
    "atlas",
]


def _rng(seed: int | None) -> random.Random:
    return random.Random(seed)


def _doc_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _pick_many(rnd: random.Random, values: list[str], min_count: int, max_count: int) -> list[str]:
    count = rnd.randint(min_count, min(max_count, len(values)))
    return rnd.sample(values, count)


def _asset_url(seed: str, width: int = 1200, height: int = 600) -> str:
    return f"https://picsum.photos/seed/{seed}/{width}/{height}"


def parse_user_ids(raw_value: str) -> list[str]:
    values = []
    for piece in raw_value.replace("\n", ",").split(","):
        cleaned = piece.strip()
        if cleaned:
            values.append(cleaned)
    return values or ["seed-user-001"]


def generate_jobs(
    count: int,
    company_name: str,
    hr_id: str,
    hr_email: str,
    org_id: str,
    seed: int | None = None,
) -> list[dict]:
    rnd = _rng(seed)
    jobs = []
    effective_company = company_name or rnd.choice(COMPANIES)
    for _ in range(count):
        title = rnd.choice(JOB_TITLES)
        requirements = _pick_many(rnd, SKILLS, 4, 7)
        qualifications = _pick_many(rnd, SKILLS, 2, 4)
        posted_at = datetime.utcnow() - timedelta(days=rnd.randint(0, 21))
        deadline = (datetime.utcnow() + timedelta(days=rnd.randint(10, 60))).date()
        doc_id = _doc_id("job")
        jobs.append(
            {
                "doc_id": doc_id,
                "jobTitle": title,
                "companyName": effective_company,
                "location": rnd.choice(LOCATIONS),
                "description": (
                    f"{title} role focused on shipping fast, scalable product features. "
                    f"Primary stack includes {', '.join(requirements[:3])}. "
                    "This is seeded demo data for the admin panel."
                ),
                "requirements": requirements,
                "jobType": rnd.choice(JOB_TYPES),
                "experienceLevel": rnd.choice(EXPERIENCE_LEVELS),
                "salaryRange": f"{rnd.randint(4, 12)}-{rnd.randint(13, 32)} LPA",
                "postedAt": posted_at,
                "category": rnd.choice(JOB_CATEGORIES),
                "applicantCount": rnd.randint(0, 75),
                "avgMatchScore": round(rnd.uniform(42, 91), 1),
                "logoUrl": _asset_url(rnd.choice(ASSET_SEEDS), 400, 400),
                "deadline": deadline.isoformat(),
                "hrId": hr_id or "seed-hr-admin",
                "hrEmail": hr_email or "seed-hr@candiatescr.local",
                "targetProjectId": None,
                "qualifications": qualifications,
                "orgId": org_id or "seed-org",
                "source": "seed_admin_panel",
            }
        )
    return jobs


def generate_hackathons(
    count: int,
    company_name: str,
    company_id: str,
    seed: int | None = None,
) -> list[dict]:
    rnd = _rng(seed)
    hackathons = []
    effective_company = company_name or rnd.choice(COMPANIES)
    effective_company_id = company_id or "seed-org"
    for _ in range(count):
        start_date = datetime.utcnow() + timedelta(days=rnd.randint(2, 25))
        end_date = start_date + timedelta(days=rnd.randint(5, 21))
        team_min = rnd.randint(1, 3)
        team_max = rnd.randint(team_min + 1, 6)
        skills = _pick_many(rnd, SKILLS, 4, 8)
        doc_id = _doc_id("hack")
        phases = [
            {
                "title": "Registration",
                "status": "open",
                "startAt": start_date - timedelta(days=5),
                "endAt": start_date - timedelta(days=1),
            },
            {
                "title": "Build",
                "status": "active",
                "startAt": start_date,
                "endAt": end_date - timedelta(days=1),
            },
            {
                "title": "Judging",
                "status": "queued",
                "startAt": end_date,
                "endAt": end_date + timedelta(days=2),
            },
        ]
        hackathons.append(
            {
                "doc_id": doc_id,
                "id": doc_id,
                "companyId": effective_company_id,
                "name": rnd.choice(HACKATHON_NAMES),
                "companyName": effective_company,
                "description": (
                    "Seeded hackathon for the Flutter candidate experience. "
                    f"Focus areas: {', '.join(skills[:4])}."
                ),
                "startDate": start_date,
                "endDate": end_date,
                "status": rnd.choice(["active", "draft", "active", "completed"]),
                "teamSizeMin": team_min,
                "teamSizeMax": team_max,
                "prize": f"Rs. {rnd.randint(50_000, 500_000):,}",
                "skillsRequired": skills,
                "participantCount": rnd.randint(10, 240),
                "teamCount": rnd.randint(3, 60),
                "submissionCount": rnd.randint(0, 48),
                "createdAt": datetime.utcnow() - timedelta(days=rnd.randint(1, 20)),
                "logoUrl": _asset_url(f"{doc_id}_logo", 400, 400),
                "posterUrl": _asset_url(f"{doc_id}_poster", 900, 1200),
                "bannerUrl": _asset_url(f"{doc_id}_banner", 1600, 700),
                "currentPhase": "Build",
                "phases": phases,
            }
        )
    return hackathons


def generate_teams(
    count: int,
    creator_ids: list[str],
    hackathon_names: list[str] | None = None,
    seed: int | None = None,
) -> list[dict]:
    rnd = _rng(seed)
    teams = []
    hackathon_names = hackathon_names or []
    for index in range(count):
        creator_id = rnd.choice(creator_ids)
        required_roles = _pick_many(rnd, TEAM_ROLES, 2, 4)
        members = [
            {
                "uid": creator_id,
                "role": "Creator",
                "joinedAt": datetime.utcnow().isoformat(),
            }
        ]
        if rnd.random() > 0.45:
            members.append(
                {
                    "uid": f"seed-member-{uuid.uuid4().hex[:6]}",
                    "role": rnd.choice(required_roles),
                    "joinedAt": datetime.utcnow().isoformat(),
                }
            )
        teams.append(
            {
                "doc_id": _doc_id("team"),
                "teamName": f"Build Crew {index + 1}",
                "description": (
                    "Demo team for talent collaboration, prototyping, and hackathon prep."
                ),
                "hackathonName": rnd.choice(hackathon_names) if hackathon_names else None,
                "creatorId": creator_id,
                "requiredRoles": required_roles,
                "members": members,
                "createdAt": datetime.utcnow().isoformat(),
            }
        )
    return teams


def generate_projects(
    count: int,
    user_ids: list[str],
    seed: int | None = None,
) -> list[dict]:
    rnd = _rng(seed)
    projects = []
    for _ in range(count):
        title = rnd.choice(PROJECT_TOPICS)
        projects.append(
            {
                "doc_id": _doc_id("project"),
                "userId": rnd.choice(user_ids),
                "title": title,
                "description": (
                    f"{title} is seeded demo data for portfolio and project discovery screens. "
                    "It includes enough content to make the UI feel populated."
                ),
                "projectLink": f"https://github.com/demo/{title.lower().replace(' ', '-')}",
                "status": rnd.choice(PROJECT_STATUSES),
                "timestamp": datetime.utcnow() - timedelta(days=rnd.randint(0, 120)),
            }
        )
    return projects
