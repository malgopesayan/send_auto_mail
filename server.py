"""
server.py — wraps the existing app without modifying app.py.

Start command on Render:
    uvicorn server:app --host 0.0.0.0 --port $PORT
"""

import os
import shutil
from pathlib import Path
from starlette.routing import Mount

# --- Gmail secret files -----------------------------------------------------
# Render Secret Files live at /etc/secrets and are READ-ONLY. google-auth
# rewrites token.json every time it refreshes the hourly access token, so a
# writable copy must exist in the working directory before GmailService loads.
for _name in ("credentials.json", "token.json"):
    _src = Path("/etc/secrets") / _name
    if _src.exists():
        _dst = Path.cwd() / _name
        try:
            if _dst.exists() or _dst.is_symlink():
                _dst.unlink()
            shutil.copy(_src, _dst)
            os.chmod(_dst, 0o600)
            print(f"[server] {_name} ready (writable copy)")
        except Exception as exc:
            print(f"[server] could not prepare {_name}: {exc}")

import app as app_module          # noqa: E402  (must come after the copy above)
import scheduler                  # noqa: E402

app = app_module.app


def run_full_job():
    """One scheduled slot: find new LinkedIn jobs, then process and send."""
    # Nobody is attached to the SSE stream during a cron run, so drain the
    # log queues first or they grow with every line and are never read.
    for q in (app_module.linkedin_search_log_queue, app_module.pipeline_log_queue):
        while not q.empty():
            q.get_nowait()

    app_module.run_linkedin_search()
    app_module.run_pipeline(auto_send=True)


scheduler.configure(run_full_job, app_module.get_supabase)
app.include_router(scheduler.router)

# app.py mounts StaticFiles at "/", which matches every path. Routes added
# after a mount are shadowed by it, so push all mounts to the end.
_mounts = [r for r in app.router.routes if isinstance(r, Mount)]
if _mounts:
    app.router.routes = [r for r in app.router.routes if not isinstance(r, Mount)] + _mounts

print("[server] scheduler attached: /schedule, /api/healthz, /api/cron/tick")
