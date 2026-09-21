"""
app.py — Job Application Pipeline: backend

What this replaces from the old CLI script:
- Screenshots now live in a Supabase Storage bucket instead of a local
  "screenshots/" folder.
- Results now go into a Supabase table instead of job_applications.xlsx.
- After a screenshot is successfully processed, it is DELETED from
  Supabase Storage (not moved to an "output" folder).
- A web dashboard (static/index.html) shows live progress while the
  pipeline runs, lists all jobs, and includes a chatbot. Asking the
  chatbot to "send mail to X" / "send the TestCo one" actually sends
  the email using GmailService (the exact class you provided) and
  updates that row's Email Status in Supabase.

SETUP
-----
1. pip install -r requirements.txt
2. Run schema.sql in your Supabase project's SQL editor to create the
   job_applications table.
3. In Supabase Storage, create a bucket (default name: "screenshots")
   and upload your recruiter/job screenshots into it.
4. Copy .env.example to .env and fill in:
     SUPABASE_URL, SUPABASE_KEY   (service_role key — needed for storage delete)
     GROQ_API_KEY (and optionally GROQ_API_KEY1..4)
5. Put your Gmail OAuth `credentials.json` next to this file (same as
   before — first send will open a browser to log in once, then it
   reuses token.json).
6. Run:  uvicorn app:app --reload
   Open http://127.0.0.1:8000 in a browser.
"""

import os
import re
import json
import time
import base64
import queue
import mimetypes
import threading
from io import BytesIO
from pathlib import Path
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv
from PIL import Image, ImageOps
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

from groq import Groq, RateLimitError, APIStatusError
from openai import OpenAI
from supabase import create_client, Client

from langchain_core.documents import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import SupabaseVectorStore
from pypdf import PdfReader
import docx as docx_lib

# Gmail — exact class provided by the user, unchanged.
from gmail_service import GmailService

load_dotenv()

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "screenshots")
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "job_applications")
# Bucket used to store the candidate's resume that gets attached to every
# outgoing application email. Only ever holds a single file — uploading a
# new one replaces whatever was there before.
SUPABASE_RESUME_BUCKET = os.environ.get("SUPABASE_RESUME_BUCKET", "resumes")

VISION_MODEL = os.environ.get("VISION_MODEL", "qwen/qwen3.8-27b")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "qwen/qwen3.8-27b")

# --- Chat assistant provider ---
# The extraction/vision pipelines above always use Groq. The chat *assistant*
# (the 💬 dashboard chatbot) can instead run on OpenRouter's free tier, which
# has a much friendlier rate limit than Groq's on-demand tier. Set
# CHAT_PROVIDER=openrouter in .env to switch; "groq" (default) keeps the
# original behavior using CHAT_MODEL above.
CHAT_PROVIDER = os.environ.get("CHAT_PROVIDER", "groq").strip().lower()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "minimax/minimax-m2.7:free")
# Optional — only used for OpenRouter's leaderboard attribution, safe to leave unset.
OPENROUTER_SITE_URL = os.environ.get("OPENROUTER_SITE_URL", "")
OPENROUTER_SITE_NAME = os.environ.get("OPENROUTER_SITE_NAME", "Job Application Pipeline")

# --- RAG / knowledge base (LangChain + Gemini embeddings + pgvector) ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# NOTE: gemini-embedding-2 has a known SDK quirk where passing a *list* of
# strings in one call collapses to a single embedding. We only ever embed
# one document at a time below, so this is safe either way. Switch this env
# var to "models/gemini-embedding-001" if you want the older, more battle-
# tested model instead.
EMBEDDING_MODEL = os.environ.get("GEMINI_EMBEDDING_MODEL", "models/gemini-embedding-2")
EMBEDDING_DIMENSIONS = int(os.environ.get("EMBEDDING_DIMENSIONS", "768"))
KNOWLEDGE_TABLE = os.environ.get("KNOWLEDGE_TABLE", "documents")

# --- LinkedIn job search (HarvestAPI) + resume-derived search keywords ---
HARVESTAPI_KEY = os.environ.get("HARVESTAPI_KEY")
HARVESTAPI_BASE = "https://api.harvestapi.io"
SEARCH_SETTINGS_TABLE = os.environ.get("SEARCH_SETTINGS_TABLE", "search_settings")
# How fresh a LinkedIn post must be to be considered. Postings older than this
# are skipped — a "4-5 day old" post is treated as stale.
LINKEDIN_MAX_POST_AGE_DAYS = int(os.environ.get("LINKEDIN_MAX_POST_AGE_DAYS", "5"))
# Many LinkedIn "job posts" are actually third-party aggregator/bot accounts
# (e.g. "RemoteYeah", "JobKash", "Singapore Jobs") reposting listings with no
# real way to reach anyone — not a genuine opening. When true, only postings
# where a real contact email was found AND the post reads as a direct listing
# from the actual hiring company/recruiter (not a repost) are saved.
LINKEDIN_REQUIRE_GENUINE_CONTACT = os.environ.get("LINKEDIN_REQUIRE_GENUINE_CONTACT", "true").lower() != "false"
# HarvestAPI's own postedLimit filter (applied server-side): '24h' | 'week' | 'month'.
# We ask for 'week' and then post-filter precisely by LINKEDIN_MAX_POST_AGE_DAYS.
LINKEDIN_POSTED_LIMIT = "week"

API_KEY_ENV_VARS = [
    "GROQ_API_KEY",
    "GROQ_API_KEY1",
    "GROQ_API_KEY2",
    "GROQ_API_KEY3",
    "GROQ_API_KEY4",
]

RETRY_BACKOFF_SECONDS = 3
MAX_IMAGE_DIMENSION = 1568
CROP_WHITESPACE_MARGINS = True

# The Groq free/on-demand tier for small models has a very low tokens-per-minute
# budget (as low as 7000 input / 1000 output). Tool results (list_jobs,
# search_knowledge, etc.) are hard-truncated, chat history is kept short, and
# every chat completion request explicitly caps its own output tokens at this
# value, so one chat turn can never blow the output-token-per-minute budget.
CHAT_MAX_OUTPUT_TOKENS = 1000
TOOL_RESULT_CHAR_LIMIT = 1200
CHAT_HISTORY_TURNS = 6  # messages (not pairs) kept from the frontend-provided history

CANDIDATE_NAME = "Sayan Malgope"
CANDIDATE_EMAIL = "malgopesayan19@gmail.com"
CANDIDATE_PHONE = "+91 8670096239"
CANDIDATE_LINKEDIN = "linkedin.com/in/malgopesayan"
CANDIDATE_GITHUB = "github.com/malgopesayan"

TARGET_PROFILE = """
Python Developer / AI Engineer skilled in building scalable backend systems and
APIs using FastAPI, with hands-on experience in Generative AI / LLM deployment
(RAG pipelines, LangChain agent workflows), enterprise authentication (Active
Directory/LDAP), and secure API design. Currently AI Engineer at Catnip Infotech
(Jan 2026 - present); previously Software Developer Intern at Nidhisha
Technologies working on Java/Spring Boot + React.js full-stack development,
REST APIs, Firebase, and AWS deployment. Core stack: Python, FastAPI, Java,
Spring Boot, React.js, SQL/MySQL, LangChain, RAG, scikit-learn, AWS. B.Tech in
Computer Science & Engineering. Looking for backend, full-stack, or AI/GenAI
engineering roles.
""".strip()

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
RESUME_EXTENSIONS = {".pdf", ".doc", ".docx"}


def sanitize_storage_filename(filename: str) -> str:
    """Every resume is stored/attached under one clean, fixed name — e.g.
    'Sayan_Malgope_Resume.pdf' — regardless of what the uploaded file was
    called. Only the original extension is kept."""
    suffix = Path(filename or "").suffix.lower()
    if suffix not in RESUME_EXTENSIONS:
        suffix = ".pdf"
    base = re.sub(r"[^A-Za-z0-9]+", "_", CANDIDATE_NAME).strip("_") or "Resume"
    return f"{base}_Resume{suffix}"

EXTRACTION_PROMPT = f"""
You are looking at a screenshot of a recruiter message, job posting, or
LinkedIn DM. Do two things:

1. Extract job/recruiter details from the image.
2. Draft a short, personalized application email for the candidate below,
   tailored to this specific job posting.

Return ONLY a JSON object (no markdown fences, no commentary):

{{
  "recruiter_email": "email address if visible, else empty string",
  "recruiter_name": "person's name if visible, else empty string",
  "company": "company name if identifiable, else empty string",
  "job_title": "job title / role if identifiable, else empty string",
  "location": "job location if mentioned, else empty string",
  "job_match_score": integer 0-100 rating how well this role matches the
      candidate profile below,
  "priority": one of "High", "Medium", "Low" based on the match score and
      role attractiveness,
  "email_subject": "a short, specific subject line for the application email,
      e.g. 'Application for <Job Title> - {CANDIDATE_NAME}'",
  "email_body": "a concise (roughly 120-180 word) plain-text application
      email body. Address the recruiter by name if known, otherwise use a
      generic greeting. Reference the specific job title/company from this
      screenshot. Highlight 2-3 of the candidate's most relevant skills/
      experience for THIS role (pick from the profile below - don't just
      dump the whole profile). End with a polite call to action and a
      sign-off using the candidate's name and contact details below.
      Do not use markdown formatting - plain text only, ready to paste into
      an email client."
}}

Candidate profile to match against and to draft the email from:
\"\"\"{TARGET_PROFILE}\"\"\"

Candidate contact details to sign the email with:
Name: {CANDIDATE_NAME}
Email: {CANDIDATE_EMAIL}
Phone: {CANDIDATE_PHONE}
LinkedIn: {CANDIDATE_LINKEDIN}
GitHub: {CANDIDATE_GITHUB}

If a field genuinely cannot be determined from the image, use an empty
string for text fields and 0 for job_match_score. Still draft
email_subject and email_body even if some job details are missing -
just keep them a bit more generic in that case.
"""


# --------------------------------------------------------------------------
# CLIENTS
# --------------------------------------------------------------------------

def load_groq_keys() -> list[str]:
    return [os.environ[v] for v in API_KEY_ENV_VARS if os.environ.get(v)]


def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY not set in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


_gmail_service = None


def get_gmail_service() -> GmailService:
    global _gmail_service
    if _gmail_service is None:
        _gmail_service = GmailService()
    return _gmail_service


_openrouter_client = None


def get_openrouter_client() -> OpenAI:
    global _openrouter_client
    if _openrouter_client is None:
        if not OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY not set in .env — required when CHAT_PROVIDER=openrouter.")
        headers = {}
        if OPENROUTER_SITE_URL:
            headers["HTTP-Referer"] = OPENROUTER_SITE_URL
        if OPENROUTER_SITE_NAME:
            headers["X-Title"] = OPENROUTER_SITE_NAME
        _openrouter_client = OpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            default_headers=headers or None,
        )
    return _openrouter_client


def get_chat_client_and_model():
    """Returns (client, model_name) for whichever provider the chat assistant
    is configured to use. Only affects /api/chat — extraction/vision pipelines
    always use Groq directly, unrelated to this."""
    if CHAT_PROVIDER == "openrouter":
        return get_openrouter_client(), OPENROUTER_MODEL
    groq_keys = load_groq_keys()
    if not groq_keys:
        raise RuntimeError("No Groq API key configured in .env")
    return Groq(api_key=groq_keys[0]), CHAT_MODEL


# --------------------------------------------------------------------------
# RESUME (single file, stored in its own Supabase Storage bucket, attached
# to every outgoing application email)
# --------------------------------------------------------------------------

def get_current_resume_info(supabase: Client) -> dict | None:
    """Returns {"name": ..., "updated_at": ...} for the current resume, or None."""
    files = supabase.storage.from_(SUPABASE_RESUME_BUCKET).list()
    files = [f for f in (files or []) if f.get("name") and not f["name"].startswith(".")]
    if not files:
        return None
    # Only one resume is ever kept, but be defensive and take the most recent.
    files.sort(key=lambda f: f.get("updated_at") or f.get("created_at") or "", reverse=True)
    f = files[0]
    return {"name": f["name"], "updated_at": f.get("updated_at") or f.get("created_at")}


def get_current_resume_bytes(supabase: Client) -> tuple[str, bytes] | tuple[None, None]:
    """Returns (filename, raw_bytes) for the current resume, or (None, None) if none uploaded
    (or if the resumes bucket isn't set up yet — this must never break sending mail)."""
    try:
        info = get_current_resume_info(supabase)
        if not info:
            return None, None
        raw = supabase.storage.from_(SUPABASE_RESUME_BUCKET).download(info["name"])
        return info["name"], raw
    except Exception as exc:  # noqa: BLE001 - resume attach is best-effort, never fatal
        print(f"⚠️  Couldn't fetch resume from '{SUPABASE_RESUME_BUCKET}' bucket, "
              f"sending without an attachment: {exc}")
        return None, None


def clear_resume_bucket(supabase: Client) -> None:
    files = supabase.storage.from_(SUPABASE_RESUME_BUCKET).list()
    names = [f["name"] for f in (files or []) if f.get("name") and not f["name"].startswith(".")]
    if names:
        supabase.storage.from_(SUPABASE_RESUME_BUCKET).remove(names)


# --------------------------------------------------------------------------
# SEARCH KEYWORDS — extracted from the resume by the model, but always
# manually editable afterward (via /api/keywords or the chatbot). Stored as
# a single row in `search_settings` so edits persist across restarts.
# --------------------------------------------------------------------------

KEYWORD_EXTRACTION_PROMPT_TEMPLATE = """
Read the resume text below and identify the 3-6 job titles/roles this
candidate is a strong fit for (e.g. "Python Developer", "AI Engineer",
"Backend Engineer").

For each role, produce 2-3 FULL search strings — not bare keywords — that
someone would actually type into LinkedIn's post search to find recruiters
or companies actively posting about that opening RIGHT NOW. Combine the role
with a hiring-intent phrase, varying the pattern, e.g.:
  "AI Engineer Hiring"
  "Hiring AI Engineer"
  "We're hiring a Python Developer"
  "Backend Engineer job opening"
  "Looking for a FastAPI Developer"
  "Urgently hiring Full Stack Developer"

Also include 2-4 pure skill/tech search strings paired with "hiring" or
"job" the same way (e.g. "LangChain Developer wanted", "Hiring for RAG
pipeline experience") rather than bare skill words alone — a lone skill like
"FastAPI" matches almost every post and isn't useful as a search string.

Rules: 10-16 total search strings, each a natural phrase of 2-6 words (not
single words), no duplicates, no generic filler like "hard worker".

Return ONLY a JSON object, no markdown fences, no commentary:
{{"keywords": ["...", "...", ...]}}

Resume text:
\"\"\"{resume_text}\"\"\"
"""


def get_search_keywords(supabase: Client) -> dict:
    """Returns {"keywords": [...], "updated_at": ...}. Empty list if never set."""
    resp = supabase.table(SEARCH_SETTINGS_TABLE).select("*").eq("id", 1).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return {"keywords": [], "updated_at": None}
    row = rows[0]
    return {"keywords": row.get("keywords") or [], "updated_at": row.get("updated_at")}


def save_search_keywords(supabase: Client, keywords: list[str]) -> dict:
    clean = [k.strip() for k in keywords if isinstance(k, str) and k.strip()]
    # de-dupe, preserve order
    seen = set()
    deduped = []
    for k in clean:
        low = k.lower()
        if low not in seen:
            seen.add(low)
            deduped.append(k)
    supabase.table(SEARCH_SETTINGS_TABLE).upsert({"id": 1, "keywords": deduped}).execute()
    return get_search_keywords(supabase)


def extract_keywords_from_resume_text(resume_text: str) -> list[str]:
    groq_keys = load_groq_keys()
    if not groq_keys or not resume_text.strip():
        return []
    client = Groq(api_key=groq_keys[0])
    prompt = KEYWORD_EXTRACTION_PROMPT_TEMPLATE.format(resume_text=resume_text[:6000])
    try:
        response = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_completion_tokens=500,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        cleaned = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
        data = json.loads(cleaned)
        return [k for k in (data.get("keywords") or []) if isinstance(k, str) and k.strip()]
    except Exception as exc:  # noqa: BLE001 - keyword extraction is best-effort
        print(f"⚠️  Couldn't extract keywords from resume: {exc}")
        return []


def regenerate_keywords_from_current_resume(supabase: Client) -> dict:
    filename, raw = get_current_resume_bytes(supabase)
    if not filename or not raw:
        raise RuntimeError("No resume on file to extract keywords from.")
    text = extract_resume_text(filename, raw)
    if not text.strip():
        raise RuntimeError(f"Couldn't extract readable text from {filename}.")
    keywords = extract_keywords_from_resume_text(text)
    if not keywords:
        raise RuntimeError("The model didn't return any keywords — try again or edit keywords manually.")
    return save_search_keywords(supabase, keywords)


# --------------------------------------------------------------------------
# KNOWLEDGE BASE / RAG (LangChain + Gemini embeddings + Supabase pgvector)
#
# Covers every job_applications row + the candidate's resume text, so the
# chatbot can semantically search "everything" instead of only exact-match
# lookups. Indexing is automatic: jobs are (re)indexed on insert/update and
# removed on delete; the resume is (re)indexed on upload.
# --------------------------------------------------------------------------

_embeddings = None


def get_embeddings() -> GoogleGenerativeAIEmbeddings:
    global _embeddings
    if _embeddings is None:
        if not GEMINI_API_KEY:
            raise RuntimeError("GEMINI_API_KEY not set in .env — required for the knowledge-base / RAG chat feature.")
        _embeddings = GoogleGenerativeAIEmbeddings(
            model=EMBEDDING_MODEL,
            google_api_key=GEMINI_API_KEY,
            output_dimensionality=EMBEDDING_DIMENSIONS,
        )
    return _embeddings


def get_vector_store(supabase: Client) -> SupabaseVectorStore:
    return SupabaseVectorStore(
        client=supabase,
        embedding=get_embeddings(),
        table_name=KNOWLEDGE_TABLE,
        query_name="match_documents",
    )


def _delete_indexed_docs(supabase: Client, source_type: str, source_id: str | None = None) -> None:
    q = supabase.table(KNOWLEDGE_TABLE).delete().eq("metadata->>source_type", source_type)
    if source_id is not None:
        q = q.eq("metadata->>source_id", source_id)
    q.execute()


def _job_document_text(job: dict) -> str:
    return "\n".join([
        f"Company: {job.get('company') or '—'}",
        f"Job Title: {job.get('job_title') or '—'}",
        f"Location: {job.get('location') or '—'}",
        f"Recruiter: {job.get('recruiter_name') or '—'} <{job.get('recruiter_email') or '—'}>",
        f"Priority: {job.get('priority') or '—'}",
        f"Match Score: {job.get('job_match_score') or 0}",
        f"Email Status: {job.get('email_status') or '—'}",
        f"Email Subject: {job.get('email_subject') or '—'}",
        f"Email Body: {job.get('email_body') or '—'}",
    ])


def index_job(supabase: Client, job: dict) -> None:
    """Upsert one job's embedding in the knowledge base. Best-effort — a
    failure here (e.g. GEMINI_API_KEY missing) must never break the caller."""
    if not job or job.get("id") is None:
        return
    try:
        job_id = str(job["id"])
        _delete_indexed_docs(supabase, "job", job_id)
        doc = Document(
            page_content=_job_document_text(job),
            metadata={
                "source_type": "job",
                "source_id": job_id,
                "company": job.get("company") or "",
                "job_title": job.get("job_title") or "",
            },
        )
        get_vector_store(supabase).add_documents([doc])
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Couldn't index job #{job.get('id')} into the knowledge base: {exc}")


def delete_job_from_index(supabase: Client, job_id: int) -> None:
    try:
        _delete_indexed_docs(supabase, "job", str(job_id))
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Couldn't remove job #{job_id} from the knowledge base: {exc}")


def extract_resume_text(filename: str, content: bytes) -> str:
    suffix = Path(filename or "").suffix.lower()
    try:
        if suffix == ".pdf":
            reader = PdfReader(BytesIO(content))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        if suffix == ".docx":
            d = docx_lib.Document(BytesIO(content))
            return "\n".join(p.text for p in d.paragraphs)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Couldn't extract text from resume ({filename}): {exc}")
        return ""
    # Legacy .doc (binary Word format) isn't parseable without extra system
    # tools — skip text extraction for it. PDF/DOCX cover the common case.
    return ""


def index_resume(supabase: Client, filename: str, content: bytes) -> None:
    try:
        _delete_indexed_docs(supabase, "resume")
        text = extract_resume_text(filename, content)
        if not text.strip():
            return
        doc = Document(page_content=text, metadata={"source_type": "resume", "source_id": filename})
        get_vector_store(supabase).add_documents([doc])
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Couldn't index resume into the knowledge base: {exc}")


def search_knowledge_base(supabase: Client, query: str, k: int = 6) -> list[Document]:
    return get_vector_store(supabase).similarity_search(query, k=k)


def reindex_all(supabase: Client) -> dict:
    """Bulk (re)index everything — used for the initial backfill of jobs that
    existed before the knowledge base was added, and as a manual fix-up."""
    counts = {"jobs": 0, "resume": 0, "errors": []}

    try:
        supabase.table(KNOWLEDGE_TABLE).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
    except Exception as exc:  # noqa: BLE001
        counts["errors"].append(f"clear existing index: {exc}")

    resp = supabase.table(SUPABASE_TABLE).select("*").execute()
    for job in resp.data or []:
        try:
            index_job(supabase, job)
            counts["jobs"] += 1
        except Exception as exc:  # noqa: BLE001
            counts["errors"].append(f"job #{job.get('id')}: {exc}")

    try:
        filename, raw = get_current_resume_bytes(supabase)
        if filename and raw:
            index_resume(supabase, filename, raw)
            counts["resume"] = 1
    except Exception as exc:  # noqa: BLE001
        counts["errors"].append(f"resume: {exc}")

    return counts


# --------------------------------------------------------------------------
# IMAGE PREPROCESSING + EXTRACTION (same approach as the CLI version, but
# works on in-memory bytes since images now come from Supabase Storage)
# --------------------------------------------------------------------------

def preprocess_image_bytes(raw_bytes: bytes) -> Image.Image:
    img = Image.open(BytesIO(raw_bytes))
    img = ImageOps.exif_transpose(img)
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    if CROP_WHITESPACE_MARGINS:
        gray = img.convert("L")
        diff = ImageOps.invert(gray) if gray.getpixel((0, 0)) > 200 else gray
        bbox = diff.getbbox()
        if bbox:
            pad = 8
            left = max(bbox[0] - pad, 0)
            top = max(bbox[1] - pad, 0)
            right = min(bbox[2] + pad, img.width)
            bottom = min(bbox[3] + pad, img.height)
            img = img.crop((left, top, right, bottom))

    longest_side = max(img.size)
    if longest_side > MAX_IMAGE_DIMENSION:
        scale = MAX_IMAGE_DIMENSION / longest_side
        new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
        img = img.resize(new_size, Image.LANCZOS)

    return img


def image_bytes_to_data_url(raw_bytes: bytes) -> str:
    img = preprocess_image_bytes(raw_bytes)
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    b64_data = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{b64_data}"


def is_quota_error(exc: Exception) -> bool:
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, APIStatusError):
        return exc.status_code == 429
    return "429" in str(exc) or "rate_limit" in str(exc).lower()


def is_extraction_mostly_empty(fields: dict) -> bool:
    core_fields = (
        fields.get("company", ""),
        fields.get("job_title", ""),
        fields.get("recruiter_name", ""),
        fields.get("recruiter_email", ""),
    )
    return all(not str(v).strip() for v in core_fields)


def extract_fields(client: Groq, raw_bytes: bytes) -> dict:
    data_url = image_bytes_to_data_url(raw_bytes)

    response = client.chat.completions.create(
        model=VISION_MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": EXTRACTION_PROMPT},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
        temperature=0.3,
        max_completion_tokens=2048,
        response_format={"type": "json_object"},
    )

    raw_text = response.choices[0].message.content or ""
    cleaned = re.sub(r"^```(?:json)?|```$", "", raw_text.strip(), flags=re.MULTILINE).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {
            "recruiter_email": "", "recruiter_name": "", "company": "",
            "job_title": "", "location": "", "job_match_score": 0,
            "priority": "", "email_subject": "", "email_body": "",
        }


def extract_with_retries(clients: list[Groq], raw_bytes: bytes, log) -> tuple[dict, bool]:
    """Returns (fields, flagged_low_confidence)."""
    max_retries = max(len(clients), 2)
    low_confidence_fallback = None
    fields = None

    for attempt in range(max_retries):
        client = clients[attempt % len(clients)]
        try:
            candidate = extract_fields(client, raw_bytes)
        except Exception as exc:  # noqa: BLE001
            if is_quota_error(exc):
                log(f"  ⚠️  quota hit (attempt {attempt + 1}/{max_retries}), trying another key...")
            else:
                log(f"  ⚠️  error on attempt {attempt + 1}: {exc.__class__.__name__}: {exc}")
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        if is_extraction_mostly_empty(candidate):
            low_confidence_fallback = candidate
            log(f"  ⚠️  extraction came back empty (attempt {attempt + 1}/{max_retries}), retrying...")
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        return candidate, False

    if low_confidence_fallback is not None:
        low_confidence_fallback["priority"] = "REVIEW - low OCR confidence"
        return low_confidence_fallback, True

    return None, False


# --------------------------------------------------------------------------
# LINKEDIN JOB SEARCH (HarvestAPI) — search by resume-derived keywords, keep
# only posts from the last LINKEDIN_MAX_POST_AGE_DAYS days, extract job
# fields + draft an email from the post text (no screenshot involved).
# --------------------------------------------------------------------------

LINKEDIN_EXTRACTION_PROMPT_TEMPLATE = f"""
You are looking at the text of a LinkedIn post, which may or may not be a
genuine job posting / recruiter outreach.

1. First decide if this is actually a job posting or recruiter outreach
   (not just someone commenting on hiring in general, a news article, etc).
2. Then decide if it is a DIRECT listing — posted by the actual hiring
   company, an employee of that company, or a recruiter working the role
   themselves — as opposed to a third-party job-aggregator or bot account
   (e.g. names like "RemoteYeah", "JobKash", "Singapore Jobs", generic
   "XYZ Jobs" pages, or accounts that just repost listings scraped from
   elsewhere with no personal involvement in the hiring). Aggregator/bot
   reposts are NOT direct listings even if the underlying job looks real.
3. If it is a genuine, direct listing, extract job/recruiter details and
   draft a short, personalized application email for the candidate below,
   tailored to this specific post.

Return ONLY a JSON object (no markdown fences, no commentary):

{{{{
  "is_job_post": true or false,
  "is_direct_listing": true or false,
  "recruiter_email": "an email address ONLY if one is actually visible verbatim in the post text — never guess, invent, or infer one from a name/company. Empty string if none is shown.",
  "recruiter_name": "the post author's name, else empty string",
  "company": "company name if identifiable, else empty string",
  "job_title": "job title / role if identifiable, else empty string",
  "location": "job location if mentioned, else empty string",
  "job_match_score": integer 0-100 rating how well this role matches the
      candidate profile below,
  "priority": one of "High", "Medium", "Low",
  "email_subject": "a short, specific subject line for the application email,
      e.g. 'Application for <Job Title> - {CANDIDATE_NAME}'",
  "email_body": "a concise (roughly 120-180 word) plain-text application
      email body, addressed to the recruiter by name if known. Reference the
      specific job title/company from this post. Highlight 2-3 of the
      candidate's most relevant skills for THIS role. End with a polite call
      to action and sign off with the candidate's name and contact details
      below. Plain text only, no markdown."
}}}}

Candidate profile to match against and to draft the email from:
\"\"\"{TARGET_PROFILE}\"\"\"

Candidate contact details to sign the email with:
Name: {CANDIDATE_NAME}
Email: {CANDIDATE_EMAIL}
Phone: {CANDIDATE_PHONE}
LinkedIn: {CANDIDATE_LINKEDIN}
GitHub: {CANDIDATE_GITHUB}

If this is not a genuine, direct job post — including aggregator/bot
reposts, or ones with no real contact info — set is_job_post and/or
is_direct_listing to false and leave the other fields as empty strings / 0.
Don't try to force a match or invent contact details.

LinkedIn post text:
\"\"\"{{post_text}}\"\"\"
"""


def harvestapi_search_posts(query: str, posted_limit: str = LINKEDIN_POSTED_LIMIT) -> list[dict]:
    if not HARVESTAPI_KEY:
        raise RuntimeError("HARVESTAPI_KEY not set in .env — required for LinkedIn job search.")
    resp = requests.get(
        f"{HARVESTAPI_BASE}/linkedin/post-search",
        params={
            "search": query,
            "postedLimit": posted_limit,
            "sortBy": "date",
            "page": 1,
        },
        headers={"X-API-Key": HARVESTAPI_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("elements", [])


def _post_posted_datetime(post: dict) -> datetime | None:
    ts = ((post.get("postedAt") or {}).get("timestamp"))
    if not ts:
        return None
    # HarvestAPI timestamps are milliseconds since epoch; be defensive in
    # case a given post ever comes back in seconds instead.
    if ts > 10_000_000_000:
        ts = ts / 1000
    try:
        return datetime.fromtimestamp(ts)
    except (ValueError, OSError):
        return None


def is_recent_post(post: dict, max_age_days: int = LINKEDIN_MAX_POST_AGE_DAYS) -> bool:
    posted_dt = _post_posted_datetime(post)
    if posted_dt is None:
        return False
    return (datetime.now() - posted_dt).days <= max_age_days


def extract_fields_from_linkedin_post(client: Groq, post_text: str) -> dict:
    prompt = LINKEDIN_EXTRACTION_PROMPT_TEMPLATE.format(post_text=post_text[:4000])
    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_completion_tokens=1024,
        response_format={"type": "json_object"},
    )
    raw_text = response.choices[0].message.content or ""
    cleaned = re.sub(r"^```(?:json)?|```$", "", raw_text.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {"is_job_post": False}


def _job_signature(company: str, job_title: str, recruiter_email: str) -> tuple[str, str, str]:
    """Normalized (company, job_title, recruiter_email) used to spot real
    duplicates — same company + same role + same contact — regardless of
    where the listing came from."""
    return (
        (company or "").strip().lower(),
        (job_title or "").strip().lower(),
        (recruiter_email or "").strip().lower(),
    )


linkedin_search_state = {"running": False}
linkedin_search_log_queue: "queue.Queue" = queue.Queue()


def run_linkedin_search():
    linkedin_search_state["running"] = True

    def log(msg: str):
        linkedin_search_log_queue.put({"type": "log", "message": msg})

    try:
        groq_keys = load_groq_keys()
        if not groq_keys:
            log("❌ No Groq API keys configured in .env — aborting.")
            linkedin_search_log_queue.put({"type": "done", "found": 0, "added": 0})
            return
        if not HARVESTAPI_KEY:
            log("❌ HARVESTAPI_KEY not set in .env — aborting.")
            linkedin_search_log_queue.put({"type": "done", "found": 0, "added": 0})
            return

        supabase = get_supabase()
        keywords = get_search_keywords(supabase)["keywords"]
        if not keywords:
            log("❌ No search keywords set. Upload a resume (auto-generates keywords) "
                "or set them manually first.")
            linkedin_search_log_queue.put({"type": "done", "found": 0, "added": 0})
            return

        client = Groq(api_key=groq_keys[0])
        log(f"Searching LinkedIn for {len(keywords)} keyword(s): {', '.join(keywords)}")

        # Gather + de-dupe posts across all keywords first, so overlapping
        # keywords don't process the same post twice.
        seen_ids = set()
        candidate_posts = []
        for kw in keywords:
            try:
                posts = harvestapi_search_posts(kw)
            except Exception as exc:  # noqa: BLE001
                log(f"  ⚠️  search for '{kw}' failed: {exc}")
                continue
            fresh = [p for p in posts if is_recent_post(p)]
            log(f"  '{kw}': {len(posts)} result(s), {len(fresh)} within the last "
                f"{LINKEDIN_MAX_POST_AGE_DAYS} day(s).")
            for p in fresh:
                pid = p.get("id") or p.get("linkedinUrl")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    candidate_posts.append(p)

        total = len(candidate_posts)
        linkedin_search_log_queue.put({"type": "start", "total": total})
        if total == 0:
            log("No fresh matching posts found.")
            linkedin_search_log_queue.put({"type": "done", "found": 0, "added": 0})
            return

        # Skip posts already saved (by linkedin_url) so re-running doesn't duplicate.
        existing = supabase.table(SUPABASE_TABLE).select("linkedin_url").eq("source", "linkedin").execute()
        already_saved = {r["linkedin_url"] for r in (existing.data or []) if r.get("linkedin_url")}

        # A "real" duplicate is the same company + job title + recruiter email,
        # regardless of which post or source it came from (e.g. the same
        # listing reposted under a different LinkedIn URL, or already added
        # via the screenshot pipeline). Built once up front, then updated as
        # we go so duplicates within this same run are also caught.
        all_rows = supabase.table(SUPABASE_TABLE).select("company,job_title,recruiter_email").execute()
        existing_signatures = {
            _job_signature(r.get("company"), r.get("job_title"), r.get("recruiter_email"))
            for r in (all_rows.data or [])
        }

        added = 0
        skipped_not_genuine = 0
        skipped_duplicate = 0
        for idx, post in enumerate(candidate_posts, start=1):
            post_url = post.get("linkedinUrl") or ""
            if post_url and post_url in already_saved:
                continue

            post_text = post.get("content") or ""
            author = (post.get("author") or {}).get("name") or ""
            log(f"[{idx}/{total}] Checking post by {author or 'unknown'} ...")

            try:
                fields = extract_fields_from_linkedin_post(client, post_text)
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ⚠️  extraction failed: {exc}. Skipping.")
                continue

            if not fields.get("is_job_post"):
                continue

            has_email = bool((fields.get("recruiter_email") or "").strip())
            is_direct = fields.get("is_direct_listing", True)  # tolerate older/odd responses
            if LINKEDIN_REQUIRE_GENUINE_CONTACT and (not has_email or not is_direct):
                reason = "no genuine contact email found" if not has_email else "looks like an aggregator/repost, not a direct listing"
                log(f"[{idx}/{total}] ⏭️  skipped ({reason}): {fields.get('company') or author or 'unknown'}")
                skipped_not_genuine += 1
                continue

            sig = _job_signature(fields.get("company"), fields.get("job_title"), fields.get("recruiter_email"))
            if sig in existing_signatures:
                log(f"[{idx}/{total}] ⏭️  skipped (duplicate — same company + title + email already saved): "
                    f"{fields.get('company') or '?'} - {fields.get('job_title') or '?'}")
                skipped_duplicate += 1
                continue

            posted_dt = _post_posted_datetime(post)
            row = {
                "recruiter_email": fields.get("recruiter_email", ""),
                "recruiter_name": fields.get("recruiter_name") or author,
                "company": fields.get("company", ""),
                "job_title": fields.get("job_title", ""),
                "location": fields.get("location", ""),
                "job_match_score": fields.get("job_match_score", 0) or 0,
                "priority": fields.get("priority", ""),
                "email_subject": fields.get("email_subject", ""),
                "email_body": fields.get("email_body", ""),
                "email_status": "Not Sent",
                "sent_date": None,
                "follow_up_date": None,
                "source": "linkedin",
                "linkedin_url": post_url,
                "posted_date": posted_dt.date().isoformat() if posted_dt else None,
            }
            try:
                insert_resp = supabase.table(SUPABASE_TABLE).insert(row).execute()
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ❌ couldn't save to the table ({exc}).")
                continue

            inserted_row = (insert_resp.data or [{}])[0]
            index_job(supabase, {**row, "id": inserted_row.get("id")})
            existing_signatures.add(sig)
            added += 1
            log(f"[{idx}/{total}] ✅ {row['company'] or '?'} | {row['job_title'] or '?'} | "
                f"score={row['job_match_score']} | posted {row['posted_date'] or '?'}")
            linkedin_search_log_queue.put({"type": "progress", "idx": idx, "total": total, "row": row})

        log(f"Done. {added} new job(s) added out of {total} fresh post(s) checked "
            f"({skipped_not_genuine} not-genuine, {skipped_duplicate} duplicate).")
        linkedin_search_log_queue.put({"type": "done", "found": total, "added": added})

    except Exception as exc:  # noqa: BLE001
        linkedin_search_log_queue.put({"type": "log", "message": f"❌ LinkedIn search crashed: {exc}"})
        linkedin_search_log_queue.put({"type": "done", "found": 0, "added": 0})
    finally:
        linkedin_search_state["running"] = False


# --------------------------------------------------------------------------
# PIPELINE: Supabase Storage -> Groq extraction -> Supabase table -> delete
# --------------------------------------------------------------------------

pipeline_state = {"running": False}
pipeline_log_queue: "queue.Queue" = queue.Queue()


def run_pipeline(auto_send: bool = False):
    pipeline_state["running"] = True

    def log(msg: str):
        pipeline_log_queue.put({"type": "log", "message": msg})

    mailed, mail_failed = 0, 0

    try:
        groq_keys = load_groq_keys()
        if not groq_keys:
            log("❌ No Groq API keys configured in .env — aborting.")
            pipeline_log_queue.put({"type": "done", "succeeded": 0, "failed": 0})
            return

        clients = [Groq(api_key=k) for k in groq_keys]
        supabase = get_supabase()

        files = supabase.storage.from_(SUPABASE_BUCKET).list()
        image_files = [
            f for f in files
            if Path(f["name"]).suffix.lower() in IMAGE_EXTENSIONS
        ]
        total = len(image_files)
        pipeline_log_queue.put({"type": "start", "total": total})

        if total == 0:
            log("No new screenshots found in the bucket.")
            pipeline_log_queue.put({"type": "done", "succeeded": 0, "failed": 0})
            return

        succeeded, failed = 0, 0

        for idx, file_info in enumerate(image_files, start=1):
            name = file_info["name"]
            log(f"[{idx}/{total}] Processing {name} ...")

            try:
                raw_bytes = supabase.storage.from_(SUPABASE_BUCKET).download(name)
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ❌ {name}: couldn't download from storage ({exc}). Skipping.")
                failed += 1
                pipeline_log_queue.put({"type": "progress", "idx": idx, "total": total, "status": "failed", "name": name})
                continue

            fields, flagged = extract_with_retries(clients, raw_bytes, log)

            if fields is None:
                log(f"[{idx}/{total}] ❌ {name}: failed on every attempt. Left in the bucket — re-run to retry.")
                failed += 1
                pipeline_log_queue.put({"type": "progress", "idx": idx, "total": total, "status": "failed", "name": name})
                continue

            row = {
                "recruiter_email": fields.get("recruiter_email", ""),
                "recruiter_name": fields.get("recruiter_name", ""),
                "company": fields.get("company", ""),
                "job_title": fields.get("job_title", ""),
                "location": fields.get("location", ""),
                "job_match_score": fields.get("job_match_score", 0) or 0,
                "priority": fields.get("priority", ""),
                "email_subject": fields.get("email_subject", ""),
                "email_body": fields.get("email_body", ""),
                "email_status": "Not Sent",
                "sent_date": None,
                "follow_up_date": None,
                "source_screenshot": name,
            }

            try:
                insert_resp = supabase.table(SUPABASE_TABLE).insert(row).execute()
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ❌ {name}: extracted fine, but couldn't save to the table "
                    f"({exc}). Left in the bucket so nothing is lost.")
                failed += 1
                pipeline_log_queue.put({"type": "progress", "idx": idx, "total": total, "status": "failed", "name": name})
                continue

            inserted_row = (insert_resp.data or [{}])[0]
            full_row = {**row, "id": inserted_row.get("id")}
            index_job(supabase, full_row)

            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([name])
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ⚠️  {name}: saved to the table, but couldn't delete from "
                    f"storage ({exc}). You may want to remove it manually.")

            status_icon = "⚠️ " if flagged else "✅"
            log(f"[{idx}/{total}] {status_icon} {name} -> {row['company'] or '?'} | "
                f"{row['job_title'] or '?'} | score={row['job_match_score']} | priority={row['priority']}")
            succeeded += 1

            if auto_send:
                if row.get("recruiter_email"):
                    try:
                        send_result = send_mail_for_job(supabase, full_row)
                        log(f"[{idx}/{total}]    ✉️ {send_result}")
                        mailed += 1
                    except Exception as exc:  # noqa: BLE001 - a failed send must not kill the run
                        log(f"[{idx}/{total}]    ❌ Couldn't send mail for {name}: {exc}")
                        mail_failed += 1
                else:
                    log(f"[{idx}/{total}]    ⚠️ No recruiter email extracted — skipped auto-send for {name}.")
                    mail_failed += 1

            pipeline_log_queue.put({
                "type": "progress", "idx": idx, "total": total,
                "status": "review" if flagged else "success", "name": name, "row": row,
            })

        summary = f"Done. {succeeded} succeeded, {failed} failed."
        if auto_send:
            summary += f" {mailed} email(s) sent, {mail_failed} not sent."
        log(summary)
        pipeline_log_queue.put({"type": "done", "succeeded": succeeded, "failed": failed})

    except Exception as exc:  # noqa: BLE001 - never let the background thread die silently
        pipeline_log_queue.put({"type": "log", "message": f"❌ Pipeline crashed: {exc}"})
        pipeline_log_queue.put({"type": "done", "succeeded": 0, "failed": 0})
    finally:
        pipeline_state["running"] = False


# --------------------------------------------------------------------------
# CHATBOT — Groq tool-calling over the jobs table + Gmail sending
# --------------------------------------------------------------------------

def find_job_matches(supabase: Client, query: str) -> list[dict]:
    """Used for sending mail — only considers jobs that haven't been sent yet."""
    query = query.strip().lower()
    resp = supabase.table(SUPABASE_TABLE).select("*").neq("email_status", "Sent").execute()
    rows = resp.data or []
    matches = [
        r for r in rows
        if query in (r.get("company") or "").lower()
        or query in (r.get("job_title") or "").lower()
        or query in (r.get("recruiter_name") or "").lower()
        or query in (r.get("recruiter_email") or "").lower()
    ]
    return matches


def find_any_job(supabase: Client, query: str = "", job_id: int | None = None) -> list[dict]:
    """General-purpose lookup across the WHOLE table (any status), for view/update/delete tools."""
    if job_id is not None:
        resp = supabase.table(SUPABASE_TABLE).select("*").eq("id", job_id).limit(1).execute()
        return resp.data or []

    query = (query or "").strip().lower()
    resp = supabase.table(SUPABASE_TABLE).select("*").execute()
    rows = resp.data or []
    if not query:
        return rows
    return [
        r for r in rows
        if query in (r.get("company") or "").lower()
        or query in (r.get("job_title") or "").lower()
        or query in (r.get("recruiter_name") or "").lower()
        or query in (r.get("recruiter_email") or "").lower()
        or query in (r.get("location") or "").lower()
        or query in (r.get("priority") or "").lower()
        or query in (r.get("email_status") or "").lower()
        or query in str(r.get("id"))
    ]


def find_bulk_send_matches(supabase: Client, query: str) -> list[dict]:
    """Used by send_mail_bulk. `query` is one of: 'today', 'this week', 'all'
    (pending only in every case), or a free-text company/title/recruiter
    substring — same matching as find_job_matches, but returns every match
    rather than requiring exactly one."""
    q = (query or "").strip().lower()
    resp = supabase.table(SUPABASE_TABLE).select("*").neq("email_status", "Sent").execute()
    rows = resp.data or []
    today_str = date.today().isoformat()
    week_ago_str = (date.today() - timedelta(days=7)).isoformat()

    if q in ("today", "created today"):
        return [r for r in rows if (r.get("created_at") or "")[:10] == today_str]
    if q in ("this week", "week", "last 7 days", "past week"):
        return [r for r in rows if (r.get("created_at") or "")[:10] >= week_ago_str]
    if q in ("all", "all pending", "everything", "*"):
        return rows
    return [
        r for r in rows
        if q in (r.get("company") or "").lower()
        or q in (r.get("job_title") or "").lower()
        or q in (r.get("recruiter_name") or "").lower()
    ]


def get_recent_sent_emails(supabase: Client, scope: str = "", limit: int = 20) -> list[dict]:
    """Compact {id, company, job_title, gmail_message_id, sent_date} rows for
    already-sent jobs — deliberately small so 'give me the mail ids' never
    blows the chat model's tiny token budget."""
    resp = supabase.table(SUPABASE_TABLE).select(
        "id,company,job_title,gmail_message_id,sent_date"
    ).eq("email_status", "Sent").order("sent_date", desc=True).execute()
    rows = resp.data or []

    scope = (scope or "").strip().lower()
    if scope in ("today", "sent today"):
        today_str = date.today().isoformat()
        rows = [r for r in rows if r.get("sent_date") == today_str]
    elif scope in ("this week", "week", "last 7 days"):
        week_ago_str = (date.today() - timedelta(days=7)).isoformat()
        rows = [r for r in rows if (r.get("sent_date") or "") >= week_ago_str]

    return rows[: max(1, min(int(limit or 20), 50))]


# Columns the chatbot is allowed to read/write on job_applications.
EDITABLE_JOB_COLUMNS = {
    "recruiter_email", "recruiter_name", "company", "job_title", "location",
    "job_match_score", "priority", "email_subject", "email_body",
    "email_status", "sent_date", "follow_up_date",
}


def send_mail_for_job(supabase: Client, row: dict) -> str:
    if not row.get("recruiter_email"):
        return f"'{row.get('company', 'that job')}' has no recruiter email on file, so I can't send it."

    resume_filename, resume_bytes = get_current_resume_bytes(supabase)

    gmail = get_gmail_service()
    message_id = gmail.send_mail(
        to_email=row["recruiter_email"],
        subject=row.get("email_subject") or f"Application - {CANDIDATE_NAME}",
        body=row.get("email_body") or "",
        attachment_bytes=resume_bytes,
        attachment_filename=resume_filename,
    )

    today = date.today()
    updates = {
        "email_status": "Sent",
        "sent_date": today.isoformat(),
        "follow_up_date": (today + timedelta(days=7)).isoformat(),
        "gmail_message_id": message_id or None,
    }
    supabase.table(SUPABASE_TABLE).update(updates).eq("id", row["id"]).execute()
    index_job(supabase, {**row, **updates})

    resume_note = f" (resume attached: {resume_filename})" if resume_filename else " (no resume on file — sent without an attachment)"
    return (f"Sent the application email to {row['recruiter_email']} for "
            f"{row.get('job_title', 'the role')} at {row.get('company', 'that company')}, "
            f"and marked it Sent.{resume_note}")


CHAT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "send_mail",
            "description": "Send the drafted application email for a specific job to its recruiter, and mark it as Sent. Use this whenever the user asks to send, email, or apply to a specific job/company/recruiter.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company name, job title, or recruiter name/email to identify which job to send. Use the words the user gave.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Get counts of jobs by priority and email status. Use for questions like 'how many jobs do I have' or 'how many are high priority'.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "start_processing",
            "description": "Start processing new screenshots from the Supabase bucket into the jobs table. Use when the user asks to process, extract, scan, or run the pipeline on new screenshots.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_jobs",
            "description": "Search or list rows from the job_applications table, across ALL statuses (sent or not). Use for any question about what jobs exist, e.g. 'show me jobs at TestCo', 'list high priority jobs', 'what's the status of the Google application'. Returns up to `limit` matching rows with all columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Free-text filter matched against company, job title, recruiter name/email, location, priority, email status, or id. Leave empty to list everything.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return. Defaults to 20.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_job",
            "description": "Get the full detail of one specific job row, including the full drafted email body. Use when the user wants to see everything about one job, e.g. 'show me the full email for the Infosys one'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company, job title, recruiter name/email, or id to identify the job.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_job",
            "description": (
                "Edit one or more fields on a job row in job_applications. Use whenever the user asks to change, "
                "correct, or update anything about a job — e.g. fix a recruiter's email, change the priority, "
                "rewrite the email subject/body, change match score, or manually mark a job's email_status. "
                "Editable columns: recruiter_email, recruiter_name, company, job_title, location, job_match_score "
                "(0-100 integer), priority (High/Medium/Low), email_subject, email_body, email_status "
                "(Not Sent/Sent), sent_date (YYYY-MM-DD), follow_up_date (YYYY-MM-DD)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company, job title, recruiter name/email, or id to identify the job to update.",
                    },
                    "updates": {
                        "type": "object",
                        "description": "Object mapping column name -> new value. Only include the fields being changed.",
                    },
                },
                "required": ["query", "updates"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_job",
            "description": (
                "Permanently delete a job row from job_applications. This is a TWO-STEP tool: "
                "1) First call it WITHOUT confirm (or confirm=false) to look up the job — you will "
                "get back its full details and must show them to the user and explicitly ask 'are "
                "you sure you want to delete this?'. 2) Only call it again WITH confirm=true after "
                "the user has clearly said yes in their next message. Never set confirm=true on the "
                "first call, even if the user's original request sounded certain — always get an "
                "explicit yes after seeing which job it is. This cannot be undone."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company, job title, recruiter name/email, or id to identify the job to delete.",
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to actually delete. Leave false/omitted for the initial lookup-and-confirm step.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge",
            "description": (
                "Semantic search across the FULL knowledge base: every job application "
                "(company, title, location, recruiter, priority, match score, status, "
                "and the full drafted email) AND the candidate's resume text. Use this "
                "for open-ended, fuzzy, or 'find similar' questions a simple keyword "
                "lookup can't answer — e.g. 'which applications are for backend/Python "
                "roles', 'does my resume mention AWS', 'summarize my high-priority "
                "applications', 'find recruiters who mentioned urgent hiring'. Prefer "
                "list_jobs/get_job for exact lookups by name; use this for anything "
                "broader or content-based, including anything about the resume."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The question or topic to search for."}
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_keywords",
            "description": "Get the current LinkedIn search keywords (extracted from the resume, but manually editable). Use when the user asks what keywords are set, or before suggesting a LinkedIn search.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_keywords",
            "description": "Replace the LinkedIn search keyword list with a new one the user has specified (add/remove/rewrite). Always send the FULL resulting list, not just the changed items. Keywords should be full search strings pairing a role/skill with hiring intent (e.g. \"AI Engineer Hiring\", \"Hiring Python Developer\") rather than bare single words, since LinkedIn's post search matches actual post text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "The full new list of search strings to save, e.g. 'Hiring Backend Engineer', not bare words like 'Backend'.",
                    }
                },
                "required": ["keywords"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_linkedin_jobs",
            "description": f"Search LinkedIn for job posts matching the saved keywords, keeping only posts from the last {LINKEDIN_MAX_POST_AGE_DAYS} days, and add any genuine job postings found to the jobs table with drafted emails. Runs in the background — use when the user asks to search/check LinkedIn for new jobs.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_sent_emails",
            "description": "Get a compact list of already-sent emails (id, company, job title, Gmail message id, sent date) — NOT the full job rows. Use this (not list_jobs) whenever the user asks for mail ids, how many were sent today/this week, or wants a short list of recent sends.",
            "parameters": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "description": "'today', 'this week', or empty for all time.",
                    },
                    "limit": {"type": "integer", "description": "Max rows, default 20, hard cap 50."},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_mail_bulk",
            "description": (
                "Send application emails to MULTIPLE unsent jobs at once, e.g. 'send all "
                "mail created today', 'send this week's applications', 'send all pending', "
                "or 'send everything at Acme'. This is a TWO-STEP tool just like delete_job: "
                "1) First call it WITHOUT confirm (or confirm=false) — you'll get back how many "
                "jobs match and their companies; show this to the user and ask them to confirm. "
                "2) Only call it again WITH confirm=true after the user clearly says yes. Never "
                "set confirm=true on the first call. This sends REAL emails and cannot be undone."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "'today', 'this week', 'all', or a company/title/recruiter substring identifying which unsent jobs to include.",
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to actually send. Leave false/omitted for the initial lookup-and-confirm step.",
                    },
                },
                "required": ["query"],
            },
        },
    },
]


def run_chat_tool(name: str, args: dict, supabase: Client) -> str:
    if name == "send_mail":
        matches = find_job_matches(supabase, args.get("query", ""))
        if not matches:
            return f"I couldn't find an unsent job matching '{args.get('query')}'."
        if len(matches) > 1:
            options = ", ".join(f"{m.get('company')} - {m.get('job_title')}" for m in matches[:5])
            return f"That matches more than one job: {options}. Can you be more specific?"
        return send_mail_for_job(supabase, matches[0])

    if name == "get_stats":
        resp = supabase.table(SUPABASE_TABLE).select("priority,email_status,sent_date").execute()
        rows = resp.data or []
        by_priority = {}
        by_status = {}
        today_str = date.today().isoformat()
        week_ago_str = (date.today() - timedelta(days=7)).isoformat()
        sent_today = 0
        sent_this_week = 0
        for r in rows:
            by_priority[r.get("priority") or "Unknown"] = by_priority.get(r.get("priority") or "Unknown", 0) + 1
            by_status[r.get("email_status") or "Unknown"] = by_status.get(r.get("email_status") or "Unknown", 0) + 1
            sent_date = r.get("sent_date")
            if sent_date == today_str:
                sent_today += 1
            if sent_date and sent_date >= week_ago_str:
                sent_this_week += 1
        return json.dumps({
            "total": len(rows),
            "by_priority": by_priority,
            "by_status": by_status,
            "sent_today": sent_today,
            "sent_last_7_days": sent_this_week,
        })

    if name == "start_processing":
        if pipeline_state["running"]:
            return "The pipeline is already running — check the console panel for progress."
        threading.Thread(target=run_pipeline, daemon=True).start()
        return "Started processing new screenshots. Watch the console panel for live progress."

    if name == "list_jobs":
        limit = args.get("limit") or 20
        rows = find_any_job(supabase, args.get("query", ""))
        rows = rows[: max(1, min(int(limit), 100))]
        if not rows:
            return "No jobs matched that."
        summary = [
            {
                "id": r.get("id"),
                "company": r.get("company"),
                "job_title": r.get("job_title"),
                "location": r.get("location"),
                "priority": r.get("priority"),
                "job_match_score": r.get("job_match_score"),
                "email_status": r.get("email_status"),
                "recruiter_email": r.get("recruiter_email"),
            }
            for r in rows
        ]
        return json.dumps(summary)

    if name in ("get_job", "update_job", "delete_job"):
        query = args.get("query", "")
        job_id = None
        if query.strip().isdigit():
            job_id = int(query.strip())
        matches = find_any_job(supabase, query="" if job_id is not None else query, job_id=job_id)
        if not matches:
            return f"I couldn't find any job matching '{query}'."
        if len(matches) > 1:
            options = ", ".join(f"#{m.get('id')} {m.get('company')} - {m.get('job_title')}" for m in matches[:8])
            return f"That matches more than one job: {options}. Can you be more specific, or give the id?"
        row = matches[0]

        if name == "get_job":
            return json.dumps(row)

        if name == "update_job":
            updates = args.get("updates") or {}
            clean_updates = {k: v for k, v in updates.items() if k in EDITABLE_JOB_COLUMNS}
            rejected = [k for k in updates if k not in EDITABLE_JOB_COLUMNS]
            if not clean_updates:
                return f"None of the fields you gave are editable. Editable fields: {', '.join(sorted(EDITABLE_JOB_COLUMNS))}."
            supabase.table(SUPABASE_TABLE).update(clean_updates).eq("id", row["id"]).execute()
            index_job(supabase, {**row, **clean_updates})
            note = f" (ignored non-editable field(s): {', '.join(rejected)})" if rejected else ""
            return (f"Updated job #{row['id']} ({row.get('company')} - {row.get('job_title')}): "
                    f"set {clean_updates}.{note}")

        if name == "delete_job":
            if args.get("confirm") is not True:
                return (f"Found job #{row['id']}: {row.get('company')} - {row.get('job_title')} "
                        f"({row.get('recruiter_email') or 'no email on file'}, status: {row.get('email_status')}). "
                        f"This is not deleted yet — ask the user to confirm, then call delete_job again "
                        f"with confirm=true only if they say yes.")
            supabase.table(SUPABASE_TABLE).delete().eq("id", row["id"]).execute()
            delete_job_from_index(supabase, row["id"])
            return f"Deleted job #{row['id']} ({row.get('company')} - {row.get('job_title')})."

    if name == "get_keywords":
        data = get_search_keywords(supabase)
        if not data["keywords"]:
            return "No search keywords set yet. Upload a resume to auto-generate some, or set them manually."
        return json.dumps(data)

    if name == "update_keywords":
        keywords = args.get("keywords") or []
        if not keywords:
            return "Give me at least one keyword to save."
        data = save_search_keywords(supabase, keywords)
        return f"Saved {len(data['keywords'])} search keyword(s): {', '.join(data['keywords'])}."

    if name == "search_linkedin_jobs":
        if linkedin_search_state["running"]:
            return "A LinkedIn search is already running — check the console panel for progress."
        keywords = get_search_keywords(supabase)["keywords"]
        if not keywords:
            return "No search keywords set yet — upload a resume or set keywords first."
        threading.Thread(target=run_linkedin_search, daemon=True).start()
        return (f"Started searching LinkedIn for posts matching your {len(keywords)} keyword(s), "
                f"keeping only ones posted in the last {LINKEDIN_MAX_POST_AGE_DAYS} days. "
                f"Watch the console panel for progress.")

    if name == "get_sent_emails":
        rows = get_recent_sent_emails(supabase, args.get("scope", ""), args.get("limit", 20))
        if not rows:
            return "No sent emails matched that."
        return json.dumps(rows)

    if name == "send_mail_bulk":
        matches = find_bulk_send_matches(supabase, args.get("query", ""))
        if not matches:
            return f"No unsent jobs matched '{args.get('query')}'."
        if args.get("confirm") is not True:
            companies = ", ".join(f"{m.get('company') or '?'} ({m.get('job_title') or '?'})" for m in matches[:10])
            more = f" and {len(matches) - 10} more" if len(matches) > 10 else ""
            return (f"Found {len(matches)} unsent job(s) matching '{args.get('query')}': "
                    f"{companies}{more}. This will send {len(matches)} real email(s) and cannot be "
                    f"undone — ask the user to confirm, then call send_mail_bulk again with "
                    f"confirm=true only if they say yes.")
        sent, failed = [], []
        for row in matches:
            try:
                send_mail_for_job(supabase, row)
                sent.append(row.get("company") or f"#{row.get('id')}")
            except Exception as exc:  # noqa: BLE001 - one failure shouldn't stop the batch
                failed.append(f"{row.get('company') or row.get('id')}: {exc}")
        summary = f"Sent {len(sent)}/{len(matches)} email(s)."
        if sent:
            shown = ", ".join(sent[:10])
            summary += f" Sent to: {shown}{' and ' + str(len(sent) - 10) + ' more' if len(sent) > 10 else ''}."
        if failed:
            summary += f" Failed ({len(failed)}): {'; '.join(failed[:5])}."
        return summary

    if name == "search_knowledge":
        query = args.get("query", "")
        try:
            docs = search_knowledge_base(supabase, query, k=6)
        except Exception as exc:  # noqa: BLE001
            return (f"Knowledge-base search failed ({exc}). Make sure GEMINI_API_KEY is set "
                    f"and the 'documents' table / match_documents function exist in Supabase.")
        if not docs:
            return "Nothing relevant found in the knowledge base."
        results = [
            {"source": d.metadata.get("source_type"), "ref": d.metadata.get("source_id"), "content": d.page_content}
            for d in docs
        ]
        return json.dumps(results)

    return f"Unknown tool: {name}"


CHAT_SYSTEM_PROMPT = f"""
You are the assistant embedded in {CANDIDATE_NAME}'s job-application pipeline
dashboard. You have full read and write access to the job_applications table
via your tools: you can list/search every job regardless of status, view a
job's full detail (including the drafted email body), edit any editable field
on a job, delete a job outright, report aggregate stats, kick off screenshot
processing, and send application emails (with the candidate's resume
auto-attached if one is on file) on the user's behalf.

You also have search_knowledge — a semantic (RAG) search over EVERYTHING:
every job's full content and the candidate's resume text. Use list_jobs/
get_job for exact lookups by name or id, and search_knowledge for open-ended,
fuzzy, "find similar", or resume-content questions that keyword matching
can't answer well.

For any question about counts — how many total, how many sent, how many
today/this week, breakdowns by priority or status — always use get_stats
first; it already includes sent_today and sent_last_7_days. Only fall back
to list_jobs if get_stats genuinely doesn't cover what was asked.

For anything about mail IDs, or a short list of what was recently sent, ALWAYS
use get_sent_emails — never list_jobs or search_knowledge for this, they
return far more data than needed and can fail with a "request too large"
error. Only call one tool per question when possible; don't chain multiple
broad tools together.

You can also help with the LinkedIn sourcing flow:
- get_keywords / update_keywords manage the LinkedIn search keywords, which
  are auto-extracted from the candidate's resume but always user-editable.
- search_linkedin_jobs kicks off a background search of LinkedIn for posts
  matching those keywords, keeping only ones posted recently, and adds any
  genuine job postings found (with a drafted email) to the jobs table.

Be concise and direct. When you change something (send mail, edit a field,
delete a row, start processing), confirm exactly what happened in plain
language. If a request is ambiguous — which job to act on, or which field to
change — ask one short clarifying question instead of guessing.

DELETING AND BULK-SENDING ARE ALWAYS TWO STEPS: never call delete_job or
send_mail_bulk with confirm=true on the first attempt, no matter how sure the
user sounds. First look the match(es) up (confirm omitted/false), show the
user exactly what you found (which job, or how many jobs and which
companies), and ask them to confirm. Only call the tool again with
confirm=true after they clearly say yes in their next message. If they say no
or don't confirm, don't act.
"""


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []


# --------------------------------------------------------------------------
# FASTAPI APP
# --------------------------------------------------------------------------

app = FastAPI(title="Job Application Pipeline")


@app.post("/api/screenshots/upload")
async def api_upload_screenshots(files: list[UploadFile] = File(...)):
    supabase = get_supabase()
    uploaded, skipped, errors = [], [], []

    for f in files:
        suffix = Path(f.filename or "").suffix.lower()
        if suffix not in IMAGE_EXTENSIONS:
            skipped.append(f.filename)
            continue

        content = await f.read()
        mime_type = f.content_type or mimetypes.guess_type(f.filename or "")[0] or "application/octet-stream"
        try:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                f.filename,
                content,
                file_options={"content-type": mime_type, "upsert": "true"},
            )
            uploaded.append(f.filename)
        except Exception as exc:  # noqa: BLE001
            errors.append({"name": f.filename, "error": str(exc)})

    return {"uploaded": uploaded, "skipped": skipped, "errors": errors}


@app.get("/api/resume")
def api_get_resume():
    supabase = get_supabase()
    try:
        info = get_current_resume_info(supabase)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail=f"Couldn't reach the '{SUPABASE_RESUME_BUCKET}' bucket — has it been created in Supabase Storage yet? ({exc})",
        )
    return info or {}


@app.post("/api/resume/upload")
async def api_upload_resume(file: UploadFile = File(...)):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in RESUME_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Resume must be a .pdf, .doc, or .docx file")

    content = await file.read()
    safe_name = sanitize_storage_filename(file.filename or "resume.pdf")
    mime_type = file.content_type or mimetypes.guess_type(safe_name)[0] or "application/octet-stream"

    supabase = get_supabase()
    try:
        # Only one resume is ever kept — clear whatever was there before uploading the new one.
        clear_resume_bucket(supabase)
        supabase.storage.from_(SUPABASE_RESUME_BUCKET).upload(
            safe_name,
            content,
            file_options={"content-type": mime_type, "upsert": "true"},
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail=f"Couldn't upload to the '{SUPABASE_RESUME_BUCKET}' bucket — has it been created in Supabase Storage yet? ({exc})",
        )
    index_resume(supabase, safe_name, content)

    keywords_result = None
    try:
        keywords_result = regenerate_keywords_from_current_resume(supabase)
    except Exception as exc:  # noqa: BLE001 - keyword generation must never break the upload
        print(f"⚠️  Couldn't auto-generate search keywords from the new resume: {exc}")

    return {"name": safe_name, "keywords": (keywords_result or {}).get("keywords", [])}


@app.delete("/api/resume")
def api_delete_resume():
    supabase = get_supabase()
    try:
        clear_resume_bucket(supabase)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Couldn't clear the resume bucket: {exc}")
    _delete_indexed_docs(supabase, "resume")
    return {"status": "deleted"}


@app.post("/api/knowledge/reindex-all")
def api_reindex_all():
    supabase = get_supabase()
    try:
        counts = reindex_all(supabase)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Reindex failed: {exc}")
    return counts


class KeywordsRequest(BaseModel):
    keywords: list[str]


@app.get("/api/keywords")
def api_get_keywords():
    supabase = get_supabase()
    return get_search_keywords(supabase)


@app.post("/api/keywords")
def api_set_keywords(req: KeywordsRequest):
    supabase = get_supabase()
    return save_search_keywords(supabase, req.keywords)


@app.post("/api/keywords/regenerate")
def api_regenerate_keywords():
    supabase = get_supabase()
    try:
        return regenerate_keywords_from_current_resume(supabase)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/linkedin/search/start")
def api_start_linkedin_search():
    if linkedin_search_state["running"]:
        return {"status": "already_running"}
    if not HARVESTAPI_KEY:
        raise HTTPException(status_code=400, detail="HARVESTAPI_KEY not set in .env")
    while not linkedin_search_log_queue.empty():
        linkedin_search_log_queue.get_nowait()
    threading.Thread(target=run_linkedin_search, daemon=True).start()
    return {"status": "started"}


@app.get("/api/linkedin/search/stream")
async def api_linkedin_search_stream():
    def event_gen():
        while True:
            try:
                item = linkedin_search_log_queue.get(timeout=1)
                yield f"data: {json.dumps(item)}\n\n"
                if item["type"] == "done":
                    break
            except queue.Empty:
                if not linkedin_search_state["running"]:
                    break
                yield ": keep-alive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.get("/api/chat/provider")
def api_chat_provider():
    """Lets you confirm which provider/model the chatbot is actually using
    right now — handy after editing .env, since a running server won't pick
    up changes until restarted."""
    if CHAT_PROVIDER == "openrouter":
        return {
            "provider": "openrouter",
            "model": OPENROUTER_MODEL,
            "openrouter_api_key_set": bool(OPENROUTER_API_KEY),
        }
    return {
        "provider": "groq",
        "model": CHAT_MODEL,
        "groq_api_key_set": bool(load_groq_keys()),
    }


@app.get("/api/jobs")
def api_get_jobs():
    supabase = get_supabase()
    resp = supabase.table(SUPABASE_TABLE).select("*").order("id", desc=True).execute()
    return resp.data or []


@app.post("/api/jobs/{job_id}/send")
def api_send_job(job_id: int):
    supabase = get_supabase()
    resp = supabase.table(SUPABASE_TABLE).select("*").eq("id", job_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        raise HTTPException(status_code=404, detail="Job not found")
    try:
        message = send_mail_for_job(supabase, rows[0])
    except Exception as exc:  # noqa: BLE001 - never let this crash unhandled
        raise HTTPException(status_code=500, detail=f"Failed to send: {exc}")
    return {"message": message}


class ProcessStartRequest(BaseModel):
    auto_send: bool = False


@app.post("/api/process/start")
def api_start_process(payload: ProcessStartRequest | None = None):
    if pipeline_state["running"]:
        return {"status": "already_running"}
    auto_send = payload.auto_send if payload else False
    # Drain any stale messages from a previous run before starting fresh.
    while not pipeline_log_queue.empty():
        pipeline_log_queue.get_nowait()
    threading.Thread(target=run_pipeline, args=(auto_send,), daemon=True).start()
    return {"status": "started", "auto_send": auto_send}


@app.get("/api/process/stream")
async def api_process_stream():
    def event_gen():
        while True:
            try:
                item = pipeline_log_queue.get(timeout=1)
                yield f"data: {json.dumps(item)}\n\n"
                if item["type"] == "done":
                    break
            except queue.Empty:
                if not pipeline_state["running"]:
                    break
                yield ": keep-alive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.post("/api/chat")
def api_chat(req: ChatRequest):
    try:
        client, chat_model = get_chat_client_and_model()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
    supabase = get_supabase()

    messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}]
    messages.extend(req.history[-CHAT_HISTORY_TURNS:])
    messages.append({"role": "user", "content": req.message})

    # Groq's newest chat completions API wants max_completion_tokens; OpenRouter
    # (and most OpenAI-compatible APIs) expect the classic max_tokens.
    output_token_kwarg = "max_tokens" if CHAT_PROVIDER == "openrouter" else "max_completion_tokens"

    try:
        reply = None
        for _ in range(5):  # hard cap so a confused model can't loop forever
            response = client.chat.completions.create(
                model=chat_model,
                messages=messages,
                tools=CHAT_TOOLS,
                tool_choice="auto",
                temperature=0.4,
                **{output_token_kwarg: CHAT_MAX_OUTPUT_TOKENS},
            )
            choice = response.choices[0].message

            if not choice.tool_calls:
                reply = choice.content
                break

            messages.append({
                "role": "assistant",
                "content": choice.content,
                "tool_calls": [tc.model_dump() for tc in choice.tool_calls],
            })
            for tool_call in choice.tool_calls:
                try:
                    args = json.loads(tool_call.function.arguments or "{}")
                    result = run_chat_tool(tool_call.function.name, args, supabase)
                except Exception as tool_exc:  # noqa: BLE001 - one bad tool call shouldn't crash the whole reply
                    result = f"That action failed: {tool_exc}"
                if result and len(result) > TOOL_RESULT_CHAR_LIMIT:
                    result = result[:TOOL_RESULT_CHAR_LIMIT] + \
                        f"... [truncated — {len(result)} chars total, narrow your query for more]"
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result,
                })
        else:
            reply = "That took more steps than expected — could you rephrase or narrow the question?"
    except Exception as exc:  # noqa: BLE001 - never let this crash unhandled
        exc_str = str(exc)
        if "rate_limit_exceeded" in exc_str or "429" in exc_str:
            provider_name = "OpenRouter" if CHAT_PROVIDER == "openrouter" else "Groq"
            billing_url = "openrouter.ai/settings/credits" if CHAT_PROVIDER == "openrouter" else "console.groq.com/settings/billing"
            raise HTTPException(
                status_code=429,
                detail=(
                    f"{provider_name}'s rate limit for `{chat_model}` was hit (its free/"
                    f"on-demand tier allows very little traffic per minute). I've already "
                    f"capped responses at {CHAT_MAX_OUTPUT_TOKENS} output tokens — try a "
                    f"shorter/narrower question, wait a few seconds and retry, or check "
                    f"{billing_url} for more headroom."
                ),
            )
        raise HTTPException(status_code=500, detail=f"Chat failed [{CHAT_PROVIDER}]: {exc}")

    return {"reply": reply}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
