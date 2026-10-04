"""
scheduler.py — DB-backed schedules triggered from outside the app.

A Render Free service sleeps after 15 minutes idle, so an in-process
scheduler cannot fire. GitHub Actions pings /api/cron/tick, which wakes
the service, and the service asks the database what is due right now.
"""

import os
import secrets
import threading
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator

TZ = ZoneInfo(os.environ.get("APP_TZ", "Asia/Kolkata"))
GRACE = timedelta(minutes=int(os.environ.get("SCHEDULE_GRACE_MIN", "90")))

router = APIRouter()

_runner = None
_get_supabase = None
_thread: threading.Thread | None = None
_lock = threading.Lock()


def configure(runner, get_supabase):
    global _runner, _get_supabase
    _runner, _get_supabase = runner, get_supabase


def _secret() -> str:
    # read lazily so it works whether .env loads before or after import
    return os.environ.get("CRON_SECRET", "")


def _sb():
    if _get_supabase is None:
        raise RuntimeError("scheduler.configure() was never called")
    return _get_supabase()


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


class ScheduleIn(BaseModel):
    label: str
    run_at: str
    enabled: bool = True

    @field_validator("run_at")
    @classmethod
    def _check(cls, v):
        p = str(v).split(":")
        h, m = int(p[0]), int(p[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError("run_at must be HH:MM")
        return f"{h:02d}:{m:02d}"


def _worker(trigger: str):
    sb = _sb()
    run = None
    try:
        run = sb.table("pipeline_runs").insert(
            {"trigger": trigger, "status": "running"}).execute().data[0]
    except Exception as exc:
        print(f"[scheduler] could not log run start: {exc}")

    try:
        _runner()
        status, detail = "done", None
    except Exception:
        status, detail = "error", traceback.format_exc()[-4000:]
        print(f"[scheduler] run failed:\n{detail}")

    if run:
        try:
            sb.table("pipeline_runs").update({
                "status": status,
                "detail": detail,
                "finished_at": datetime.now(TZ).isoformat(),
            }).eq("id", run["id"]).execute()
        except Exception as exc:
            print(f"[scheduler] could not log run end: {exc}")


def start_run(trigger: str) -> bool:
    global _thread
    with _lock:
        if is_running():
            return False
        _thread = threading.Thread(target=_worker, args=(trigger,), daemon=True)
        _thread.start()
        return True


def _due(now: datetime) -> list[dict]:
    rows = _sb().table("schedules").select("*").eq("enabled", True).execute().data or []
    out = []
    for row in rows:
        p = str(row["run_at"]).split(":")
        slot = now.replace(hour=int(p[0]), minute=int(p[1]), second=0, microsecond=0)
        if now < slot or now - slot > GRACE:
            continue
        last = row.get("last_run_at")
        if last:
            last_dt = datetime.fromisoformat(
                str(last).replace("Z", "+00:00")).astimezone(TZ)
            if last_dt >= slot:
                continue
        out.append(row)
    return out


@router.get("/api/healthz")
def healthz():
    return {"ok": True, "running": is_running(), "time": datetime.now(TZ).isoformat()}


@router.get("/api/schedules")
def list_schedules():
    rows = _sb().table("schedules").select("*").order("run_at").execute().data
    return {"timezone": str(TZ), "running": is_running(), "schedules": rows or []}


@router.post("/api/schedules")
def create_schedule(body: ScheduleIn):
    return _sb().table("schedules").insert(body.model_dump()).execute().data[0]


@router.patch("/api/schedules/{sid}")
def update_schedule(sid: int, body: ScheduleIn):
    rows = _sb().table("schedules").update(
        body.model_dump()).eq("id", sid).execute().data
    if not rows:
        raise HTTPException(404, "no such schedule")
    return rows[0]


@router.delete("/api/schedules/{sid}")
def delete_schedule(sid: int):
    _sb().table("schedules").delete().eq("id", sid).execute()
    return {"deleted": sid}


@router.post("/api/full-run")
def full_run_manual():
    if not start_run("manual"):
        raise HTTPException(409, "a run is already in progress")
    return {"started": True}


@router.get("/api/runs")
def recent_runs():
    rows = _sb().table("pipeline_runs").select("*").order(
        "started_at", desc=True).limit(10).execute().data
    return {"running": is_running(), "runs": rows or []}


@router.post("/api/cron/tick")
def cron_tick(x_cron_secret: str = Header(default="")):
    s = _secret()
    if not s or not secrets.compare_digest(x_cron_secret, s):
        raise HTTPException(401, "bad cron secret")

    now = datetime.now(TZ)
    due = _due(now)
    if not due:
        return {"woken": True, "now": now.isoformat(), "fired": []}
    if not start_run("schedule"):
        return {"woken": True, "fired": [], "note": "already running"}

    stamp = now.isoformat()
    for row in due:
        _sb().table("schedules").update(
            {"last_run_at": stamp}).eq("id", row["id"]).execute()
    return {"woken": True, "now": stamp, "fired": [r["label"] for r in due]}


PAGE = """<!doctype html>
<meta charset="utf-8"><title>Schedule</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 body{font-family:system-ui,sans-serif;background:#111;color:#eee;margin:0;padding:24px}
 .wrap{max-width:720px;margin:0 auto}
 .row{display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap}
 input,button{font:inherit;padding:6px 10px;border-radius:6px;border:1px solid #444;
   background:#1b1b1b;color:#eee}
 button{cursor:pointer}button:hover{background:#2a2a2a}
 .muted{opacity:.6;font-size:13px}
 .card{border:1px solid #333;border-radius:10px;padding:16px;margin-bottom:16px}
 a{color:#8ab4f8}
</style>
<div class="wrap">
 <h2>Pipeline schedule</h2>
 <p class="muted">Each enabled time runs the full job once a day:
   LinkedIn search, then process and send. <a href="/">Back to dashboard</a></p>
 <div class="card">
   <div class="row"><strong>Times</strong><span id="tz" class="muted"></span>
     <span style="flex:1"></span>
     <button onclick="add()">+ Add time</button>
     <button onclick="runNow()">Run now</button></div>
   <div id="rows"></div>
   <div id="status" class="muted"></div>
 </div>
 <div class="card"><strong>Recent runs</strong><div id="runs" class="muted"></div></div>
</div>
<script>
async function load(){
 const d=await (await fetch('/api/schedules')).json();
 tz.textContent='times in '+d.timezone;
 rows.innerHTML=(d.schedules||[]).map(s=>`
  <div class="row" data-sid="${s.id}">
   <input class="l" value="${s.label}" style="width:150px">
   <input class="t" type="time" value="${String(s.run_at).slice(0,5)}">
   <label><input class="o" type="checkbox" ${s.enabled?'checked':''}> on</label>
   <button onclick="save(${s.id})">Save</button>
   <button onclick="del(${s.id})">Delete</button>
   <span class="muted">last: ${s.last_run_at?new Date(s.last_run_at).toLocaleString():'never'}</span>
  </div>`).join('');
 status.textContent=d.running?'A run is in progress...':'';
 const r=await (await fetch('/api/runs')).json();
 runs.innerHTML=(r.runs||[]).map(x=>
  `<div>${new Date(x.started_at).toLocaleString()} — ${x.trigger} — ${x.status}</div>`
 ).join('')||'none yet';
}
function body(id){const r=document.querySelector(`[data-sid="${id}"]`);
 return{label:r.querySelector('.l').value.trim()||'Run',
        run_at:r.querySelector('.t').value,
        enabled:r.querySelector('.o').checked};}
async function save(id){await fetch('/api/schedules/'+id,{method:'PATCH',
 headers:{'Content-Type':'application/json'},body:JSON.stringify(body(id))});load();}
async function del(id){if(!confirm('Delete?'))return;
 await fetch('/api/schedules/'+id,{method:'DELETE'});load();}
async function add(){await fetch('/api/schedules',{method:'POST',
 headers:{'Content-Type':'application/json'},
 body:JSON.stringify({label:'New run',run_at:'10:00',enabled:true})});load();}
async function runNow(){const r=await fetch('/api/full-run',{method:'POST'});
 status.textContent=r.ok?'Started — watch the dashboard console.':'Already running.';
 setTimeout(load,1500);}
load();setInterval(load,15000);
</script>"""


@router.get("/schedule", response_class=HTMLResponse)
def schedule_page():
    return PAGE
