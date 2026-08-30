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
from datetime import date, timedelta

from dotenv import load_dotenv
from PIL import Image, ImageOps
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

from groq import Groq, RateLimitError, APIStatusError
from supabase import create_client, Client

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

VISION_MODEL = os.environ.get("VISION_MODEL", "qwen/qwen3.8-27b")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "llama-3.3-70b-versatile")

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
# PIPELINE: Supabase Storage -> Groq extraction -> Supabase table -> delete
# --------------------------------------------------------------------------

pipeline_state = {"running": False}
pipeline_log_queue: "queue.Queue" = queue.Queue()


def run_pipeline():
    pipeline_state["running"] = True

    def log(msg: str):
        pipeline_log_queue.put({"type": "log", "message": msg})

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
                supabase.table(SUPABASE_TABLE).insert(row).execute()
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ❌ {name}: extracted fine, but couldn't save to the table "
                    f"({exc}). Left in the bucket so nothing is lost.")
                failed += 1
                pipeline_log_queue.put({"type": "progress", "idx": idx, "total": total, "status": "failed", "name": name})
                continue

            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([name])
            except Exception as exc:  # noqa: BLE001
                log(f"[{idx}/{total}] ⚠️  {name}: saved to the table, but couldn't delete from "
                    f"storage ({exc}). You may want to remove it manually.")

            status_icon = "⚠️ " if flagged else "✅"
            log(f"[{idx}/{total}] {status_icon} {name} -> {row['company'] or '?'} | "
                f"{row['job_title'] or '?'} | score={row['job_match_score']} | priority={row['priority']}")
            succeeded += 1
            pipeline_log_queue.put({
                "type": "progress", "idx": idx, "total": total,
                "status": "review" if flagged else "success", "name": name, "row": row,
            })

        log(f"Done. {succeeded} succeeded, {failed} failed.")
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


def send_mail_for_job(supabase: Client, row: dict) -> str:
    if not row.get("recruiter_email"):
        return f"'{row.get('company', 'that job')}' has no recruiter email on file, so I can't send it."

    gmail = get_gmail_service()
    gmail.send_mail(
        to_email=row["recruiter_email"],
        subject=row.get("email_subject") or f"Application - {CANDIDATE_NAME}",
        body=row.get("email_body") or "",
    )

    today = date.today()
    supabase.table(SUPABASE_TABLE).update({
        "email_status": "Sent",
        "sent_date": today.isoformat(),
        "follow_up_date": (today + timedelta(days=7)).isoformat(),
    }).eq("id", row["id"]).execute()

    return f"Sent the application email to {row['recruiter_email']} for {row.get('job_title', 'the role')} at {row.get('company', 'that company')}, and marked it Sent."


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
        resp = supabase.table(SUPABASE_TABLE).select("priority,email_status").execute()
        rows = resp.data or []
        by_priority = {}
        by_status = {}
        for r in rows:
            by_priority[r.get("priority") or "Unknown"] = by_priority.get(r.get("priority") or "Unknown", 0) + 1
            by_status[r.get("email_status") or "Unknown"] = by_status.get(r.get("email_status") or "Unknown", 0) + 1
        return json.dumps({"total": len(rows), "by_priority": by_priority, "by_status": by_status})

    if name == "start_processing":
        if pipeline_state["running"]:
            return "The pipeline is already running — check the console panel for progress."
        threading.Thread(target=run_pipeline, daemon=True).start()
        return "Started processing new screenshots. Watch the console panel for live progress."

    return f"Unknown tool: {name}"


CHAT_SYSTEM_PROMPT = f"""
You are the assistant embedded in {CANDIDATE_NAME}'s job-application pipeline
dashboard. You can look up jobs, report stats, kick off screenshot processing,
and send application emails on the user's behalf via the available tools.
Be concise and direct. When you send an email or start processing, confirm
what happened in plain language. If a request is ambiguous (e.g. which job to
email), ask a short clarifying question instead of guessing.
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
    message = send_mail_for_job(supabase, rows[0])
    return {"message": message}


@app.post("/api/process/start")
def api_start_process():
    if pipeline_state["running"]:
        return {"status": "already_running"}
    # Drain any stale messages from a previous run before starting fresh.
    while not pipeline_log_queue.empty():
        pipeline_log_queue.get_nowait()
    threading.Thread(target=run_pipeline, daemon=True).start()
    return {"status": "started"}


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
    groq_keys = load_groq_keys()
    if not groq_keys:
        raise HTTPException(status_code=500, detail="No Groq API key configured")
    client = Groq(api_key=groq_keys[0])
    supabase = get_supabase()

    messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}]
    messages.extend(req.history)
    messages.append({"role": "user", "content": req.message})

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=messages,
        tools=CHAT_TOOLS,
        tool_choice="auto",
        temperature=0.4,
    )

    choice = response.choices[0].message

    if choice.tool_calls:
        messages.append(choice.model_dump())
        for tool_call in choice.tool_calls:
            args = json.loads(tool_call.function.arguments or "{}")
            result = run_chat_tool(tool_call.function.name, args, supabase)
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result,
            })

        follow_up = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=messages,
            temperature=0.4,
        )
        reply = follow_up.choices[0].message.content
    else:
        reply = choice.content

    return {"reply": reply}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
