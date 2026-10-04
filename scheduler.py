"""
scheduler.py — DB-backed schedules triggered from outside the app.

A Render Free service sleeps after 15 minutes idle, so an in-process
scheduler cannot fire. GitHub Actions pings /api/cron/tick, which wakes
the service, and the service asks the database what is due right now.

Each schedule has a mode:
    search → find new LinkedIn jobs only
    send   → send mail for pending rows only
    both   → search, then send
"""

import os
import secrets
import threading
import traceback
from datetime import datetime, timedelta
from typing import Literal
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
    return os.environ.get("CRON_SECRET", "")


def _sb():
    if _get_supabase is None:
        raise HTTPException(500, "scheduler.configure() was never called")
    try:
        return _get_supabase()
    except Exception as exc:
        raise HTTPException(500, f"Supabase client error: {exc}")


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


class ScheduleIn(BaseModel):
    label: str = "Run"
    run_at: str
    enabled: bool = True
    mode: Literal["search", "send", "both"] = "both"

    @field_validator("run_at")
    @classmethod
    def _check(cls, v):
        p = str(v).split(":")
        try:
            h, m = int(p[0]), int(p[1])
        except (ValueError, IndexError):
            raise ValueError("run_at must look like HH:MM")
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError("run_at out of range")
        return f"{h:02d}:{m:02d}"


def _worker(trigger: str, mode: str):
    sb = _get_supabase()
    run = None
    try:
        run = sb.table("pipeline_runs").insert(
            {"trigger": f"{trigger}:{mode}", "status": "running"}).execute().data[0]
    except Exception as exc:
        print(f"[scheduler] could not log run start: {exc}")

    try:
        _runner(mode)
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


def start_run(trigger: str, mode: str = "both") -> bool:
    global _thread
    with _lock:
        if is_running():
            return False
        _thread = threading.Thread(target=_worker, args=(trigger, mode), daemon=True)
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


def _merge_modes(rows: list[dict]) -> str:
    """Two slots firing in the same tick: do the union, never the work twice."""
    modes = {r.get("mode") or "both" for r in rows}
    if "both" in modes or {"search", "send"} <= modes:
        return "both"
    return modes.pop()


@router.get("/api/healthz")
def healthz():
    return {"ok": True, "running": is_running(), "time": datetime.now(TZ).isoformat()}


@router.get("/api/schedules")
def list_schedules():
    try:
        rows = _sb().table("schedules").select("*").order("run_at").execute().data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"read failed: {exc}")
    return {"timezone": str(TZ), "running": is_running(), "schedules": rows or []}


@router.post("/api/schedules")
def create_schedule(body: ScheduleIn):
    sb = _sb()
    payload = body.model_dump()
    try:
        resp = sb.table("schedules").insert(payload).execute()
    except Exception as exc:
        # Usual causes: table missing, or SUPABASE_KEY is the anon key and
        # row-level security is blocking the write.
        raise HTTPException(500, f"insert failed: {exc}")

    rows = resp.data or []
    if rows:
        return rows[0]

    try:
        check = sb.table("schedules").select("*").eq(
            "run_at", payload["run_at"]).execute().data or []
    except Exception as exc:
        raise HTTPException(500, f"insert returned no row and re-read failed: {exc}")
    if check:
        return check[-1]
    raise HTTPException(
        500,
        "insert silently returned no row — usually row-level security. "
        "Check that SUPABASE_KEY in Render is the service_role key.",
    )


@router.patch("/api/schedules/{sid}")
def update_schedule(sid: int, body: ScheduleIn):
    try:
        rows = _sb().table("schedules").update(
            body.model_dump()).eq("id", sid).execute().data
    except Exception as exc:
        raise HTTPException(500, f"update failed: {exc}")
    if not rows:
        raise HTTPException(404, "no such schedule (or RLS blocked the update)")
    return rows[0]


@router.delete("/api/schedules/{sid}")
def delete_schedule(sid: int):
    try:
        _sb().table("schedules").delete().eq("id", sid).execute()
    except Exception as exc:
        raise HTTPException(500, f"delete failed: {exc}")
    return {"deleted": sid}


@router.post("/api/full-run")
def full_run_manual(mode: str = "both"):
    if mode not in ("search", "send", "both"):
        raise HTTPException(422, "mode must be search, send or both")
    if not start_run("manual", mode):
        raise HTTPException(409, "a run is already in progress")
    return {"started": True, "mode": mode}


@router.get("/api/runs")
def recent_runs():
    try:
        rows = _sb().table("pipeline_runs").select("*").order(
            "started_at", desc=True).limit(10).execute().data
    except Exception as exc:
        raise HTTPException(500, f"read failed: {exc}")
    return {"running": is_running(), "runs": rows or []}


@router.get("/api/schedules/diagnose")
def diagnose():
    out = {"timezone": str(TZ), "cron_secret_set": bool(_secret())}
    sb = _sb()
    for table in ("schedules", "pipeline_runs"):
        try:
            rows = sb.table(table).select("*").limit(1).execute().data
            out[table] = {"ok": True, "rows_visible": len(rows or [])}
        except Exception as exc:
            out[table] = {"ok": False, "error": str(exc)}
    return out


@router.post("/api/cron/tick")
def cron_tick(x_cron_secret: str = Header(default="")):
    s = _secret()
    if not s or not secrets.compare_digest(x_cron_secret, s):
        raise HTTPException(401, "bad cron secret")

    now = datetime.now(TZ)
    due = _due(now)
    if not due:
        return {"woken": True, "now": now.isoformat(), "fired": []}

    mode = _merge_modes(due)
    if not start_run("schedule", mode):
        return {"woken": True, "fired": [], "note": "already running"}

    stamp = now.isoformat()
    for row in due:
        _sb().table("schedules").update(
            {"last_run_at": stamp}).eq("id", row["id"]).execute()
    return {"woken": True, "now": stamp, "mode": mode,
            "fired": [r["label"] for r in due]}


PAGE = """<!doctype html>
<meta charset="utf-8"><title>Schedule</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 body{font-family:ui-monospace,Menlo,Consolas,monospace;background:#0f1115;color:#ddd;
  margin:0;padding:20px}
 .wrap{max-width:660px;margin:0 auto}
 .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;border:1px solid #23262b;
  border-radius:8px;padding:10px;margin:8px 0;background:#171a1f}
 input,button,select{font:inherit;padding:7px 10px;border-radius:6px;border:1px solid #3a3f46;
  background:#1e2127;color:#eee}
 button{cursor:pointer}
 .muted{opacity:.6;font-size:12px}
 h2{color:#f5a524;letter-spacing:.05em}
 a{color:#f5a524}
 @media(max-width:560px){.row{flex-direction:column;align-items:stretch}
  input,select{width:100%;box-sizing:border-box}}
</style>
<div class="wrap">
 <h2>SCHEDULE</h2>
 <p class="muted">search = find new LinkedIn jobs &middot; send = mail the pending rows
 &middot; both = search then send. <a href="/">Back to dashboard</a></p>
 <div id="rows"></div>
 <p><button onclick="add()">+ ADD TIME</button>
    <button onclick="runNow('search')">SEARCH NOW</button>
    <button onclick="runNow('send')">SEND NOW</button></p>
 <div id="status" class="muted"></div>
 <h3>Recent runs</h3><div id="runs" class="muted"></div>
</div>
<script>
async function call(u,o){const r=await fetch(u,o);
 if(!r.ok){const t=await r.text();document.getElementById('status').textContent=
  'Error '+r.status+': '+t;throw new Error(t);} return r.json();}
function opt(v,s){return '<option value="'+v+'"'+(s===v?' selected':'')+'>'+v+'</option>';}
async function load(){
 try{const d=await call('/api/schedules');
  rows.innerHTML=(d.schedules||[]).map(s=>`
   <div class="row" data-sid="${s.id}">
    <input class="l" value="${s.label}">
    <input class="t" type="time" value="${String(s.run_at).slice(0,5)}">
    <select class="m">${opt('search',s.mode)}${opt('send',s.mode)}${opt('both',s.mode)}</select>
    <label><input class="o" type="checkbox" ${s.enabled?'checked':''}> on</label>
    <button onclick="save(${s.id})">SAVE</button>
    <button onclick="del(${s.id})">DELETE</button>
    <div class="muted">${d.timezone} &middot; last: ${s.last_run_at?new Date(s.last_run_at).toLocaleString():'never'}</div>
   </div>`).join('')||'<p class="muted">No times set yet.</p>';
  status.textContent=d.running?'A run is in progress...':'';
 }catch(e){}
 try{const r=await call('/api/runs');
  runs.innerHTML=(r.runs||[]).map(x=>
   new Date(x.started_at).toLocaleString()+' - '+x.trigger+' - '+x.status).join('<br>')||'none yet';
 }catch(e){}}
function body(id){const r=document.querySelector(`[data-sid="${id}"]`);
 return{label:r.querySelector('.l').value.trim()||'Run',
        run_at:r.querySelector('.t').value,mode:r.querySelector('.m').value,
        enabled:r.querySelector('.o').checked};}
async function save(id){await call('/api/schedules/'+id,{method:'PATCH',
 headers:{'Content-Type':'application/json'},body:JSON.stringify(body(id))});load();}
async function del(id){if(!confirm('Delete?'))return;
 await call('/api/schedules/'+id,{method:'DELETE'});load();}
async function add(){await call('/api/schedules',{method:'POST',
 headers:{'Content-Type':'application/json'},
 body:JSON.stringify({label:'New run',run_at:'10:00',mode:'search',enabled:true})});load();}
async function runNow(m){try{await call('/api/full-run?mode='+m,{method:'POST'});
 status.textContent='Started ('+m+') - watch the Console tab.';setTimeout(load,1500);}catch(e){}}
load();setInterval(load,20000);
</script>"""


@router.get("/schedule", response_class=HTMLResponse)
def schedule_page():
    return PAGE
