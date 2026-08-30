# Job Application Pipeline — Dashboard + Chatbot

Everything from the old CLI script, moved to Supabase, with a web dashboard
and a chatbot on top.

## What changed vs. the old script

| Old | New |
|---|---|
| Screenshots in local `screenshots/` folder | Screenshots in a Supabase Storage bucket |
| Results in `job_applications.xlsx` | Results in a Supabase table |
| Processed screenshot moved to `screenshots/output/` | Processed screenshot is **deleted** from Storage |
| Run from terminal, watch console text | Web dashboard with a live console panel showing the same progress |
| No UI | Jobs table (priority/status badges, per-row "Send" button) |
| No chatbot | Chatbot that can send mail, report stats, and kick off processing, using tool calling |
| — | Sending mail uses your exact `GmailService` class, unchanged |

## Setup

1. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

2. **Create the Supabase table** — open your Supabase project → SQL Editor →
   paste and run `schema.sql`.

3. **Create the Storage bucket** — Supabase project → Storage → New bucket →
   name it `screenshots` (or pick your own name and set `SUPABASE_BUCKET` in
   `.env` to match). You don't need to upload anything manually — the
   dashboard has an "Add Screenshots" upload box (drag-and-drop or click to
   pick files) that uploads straight into this bucket.

4. **Configure environment** — copy `.env.example` to `.env` and fill in:
   ```
   SUPABASE_URL=...
   SUPABASE_KEY=...        # service_role key (needed to delete files from Storage)
   GROQ_API_KEY=...
   ```
   Use the **service_role** key, not `anon` — the `anon` key can't delete
   Storage files, and the pipeline needs to delete each screenshot after
   it's processed.

5. **Gmail** — put your existing `credentials.json` in this same folder.
   `gmail_service.py` is your class, unchanged. The first time the chatbot
   or a "Send" button actually sends mail, a browser window will open for
   you to log in once; after that `token.json` is reused automatically.

6. **Run it**
   ```
   uvicorn app:app --reload
   ```
   Open **http://127.0.0.1:8000** in a browser.

## Using it

- **Add Screenshots** — drag files into the box (or click it to browse) and
  hit Upload. They go straight into the Supabase bucket, ready for the
  pipeline to pick up.
- **Run Pipeline** — pulls every image from the Supabase bucket, extracts
  fields + drafts an email with Groq, saves the row to the table, deletes
  the screenshot from Storage. Progress streams live into the console panel
  (same `[12/56] ✅ ...` style as the old script, just in the browser).
- **Jobs table** — updates automatically as the pipeline runs. Each unsent
  row has a **Send** button that emails that recruiter directly and marks
  the row `Sent`.
- **Chatbot** (bottom-right 💬) — ask things like:
  - "send mail to TestCo" / "email the recruiter at Acme"
  - "how many high priority jobs do I have"
  - "process new screenshots"
  If a request is ambiguous (matches more than one job), it'll ask you to
  be more specific instead of guessing.

## Notes

- The pipeline retries a failed screenshot across however many `GROQ_API_KEY*`
  keys you've set, same as before. A screenshot that fails on every attempt
  is left in the bucket (not deleted) so the next run retries it.
- If an extraction comes back with company/title/recruiter all blank after
  every retry, it's saved with `priority = "REVIEW - low OCR confidence"`
  instead of a silent blank row.
- `CHAT_MODEL` defaults to `llama-3.3-70b-versatile`. Groq's model lineup
  changes fairly often — if you get a "model decommissioned" error, check
  `console.groq.com/docs/models` and update `CHAT_MODEL` / `VISION_MODEL`
  in `.env`.
