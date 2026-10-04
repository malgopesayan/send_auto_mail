"""
server.py — wraps the existing app without modifying app.py or index.html.

Start command on Render:
    uvicorn server:app --host 0.0.0.0 --port $PORT
"""

import os
import shutil
from pathlib import Path

from fastapi.responses import HTMLResponse
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


# --- schedule panel injected into the existing dashboard --------------------
PANEL = """
<div id="sched-fab" title="Schedule">&#128337;</div>
<div id="sched-panel">
  <div class="sp-head">
    <strong>Schedule</strong>
    <span id="sp-tz"></span>
    <span style="flex:1"></span>
    <span id="sp-close" title="Close">&times;</span>
  </div>
  <div id="sp-rows"></div>
  <div class="sp-actions">
    <button type="button" id="sp-add">+ Add time</button>
    <button type="button" id="sp-run">Run now</button>
  </div>
  <div id="sp-status"></div>
  <div id="sp-runs"></div>
</div>
<style>
#sched-fab{position:fixed;left:20px;bottom:20px;width:48px;height:48px;border-radius:50%;
 background:#2563eb;color:#fff;display:flex;align-items:center;justify-content:center;
 font-size:22px;cursor:pointer;z-index:9998;box-shadow:0 4px 14px rgba(0,0,0,.4)}
#sched-panel{position:fixed;left:20px;bottom:80px;width:390px;max-width:calc(100vw - 40px);
 max-height:70vh;overflow:auto;background:#15171c;color:#e8e8e8;border:1px solid #333;
 border-radius:12px;padding:14px;z-index:9999;display:none;
 font-family:system-ui,sans-serif;font-size:14px;box-shadow:0 8px 28px rgba(0,0,0,.5)}
#sched-panel.open{display:block}
#sched-panel .sp-head{display:flex;align-items:center;gap:8px;margin-bottom:10px}
#sched-panel #sp-close{cursor:pointer;font-size:20px;opacity:.7;padding:0 4px}
#sched-panel .sp-row{display:flex;gap:6px;align-items:center;margin:6px 0;flex-wrap:wrap}
#sched-panel input[type=text]{width:108px}
#sched-panel input,#sched-panel button{font:inherit;padding:5px 8px;border-radius:6px;
 border:1px solid #444;background:#1e2026;color:#e8e8e8}
#sched-panel button{cursor:pointer}
#sched-panel button:hover{background:#2a2d35}
#sched-panel .sp-actions{margin-top:10px;display:flex;gap:8px}
#sched-panel #sp-tz,#sched-panel #sp-status,#sched-panel #sp-runs,
#sched-panel .sp-last{opacity:.6;font-size:12px}
#sched-panel #sp-runs{margin-top:10px;border-top:1px solid #2c2c2c;padding-top:8px}
</style>
<script>
(function(){
 var P=document.getElementById('sched-panel');
 document.getElementById('sched-fab').onclick=function(){
   P.classList.toggle('open'); if(P.classList.contains('open')) spLoad();};
 document.getElementById('sp-close').onclick=function(){P.classList.remove('open')};
 document.getElementById('sp-add').onclick=function(){
   fetch('/api/schedules',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({label:'New run',run_at:'10:00',enabled:true})}).then(spLoad)};
 document.getElementById('sp-run').onclick=function(){
   fetch('/api/full-run',{method:'POST'}).then(function(r){
     document.getElementById('sp-status').textContent =
       r.ok?'Started - watch the console panel.':'A run is already in progress.';
     setTimeout(spLoad,1500);})};

 window.spSave=function(id){
   var r=document.querySelector('[data-sid="'+id+'"]');
   fetch('/api/schedules/'+id,{method:'PATCH',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({label:r.querySelector('.sp-l').value.trim()||'Run',
                         run_at:r.querySelector('.sp-t').value,
                         enabled:r.querySelector('.sp-o').checked})}).then(spLoad)};
 window.spDel=function(id){ if(!confirm('Delete this schedule?'))return;
   fetch('/api/schedules/'+id,{method:'DELETE'}).then(spLoad)};

 function spLoad(){
  fetch('/api/schedules').then(function(r){return r.json()}).then(function(d){
   document.getElementById('sp-tz').textContent='('+d.timezone+')';
   document.getElementById('sp-rows').innerHTML=(d.schedules||[]).map(function(s){
    return '<div class="sp-row" data-sid="'+s.id+'">'+
     '<input type="text" class="sp-l" value="'+s.label+'">'+
     '<input type="time" class="sp-t" value="'+String(s.run_at).slice(0,5)+'">'+
     '<label><input type="checkbox" class="sp-o" '+(s.enabled?'checked':'')+'> on</label>'+
     '<button onclick="spSave('+s.id+')">Save</button>'+
     '<button onclick="spDel('+s.id+')">Del</button>'+
     '<div class="sp-last">last: '+(s.last_run_at?new Date(s.last_run_at).toLocaleString():'never')+'</div>'+
     '</div>';}).join('');
   document.getElementById('sp-status').textContent=d.running?'A run is in progress...':'';
  }).catch(function(e){
   document.getElementById('sp-rows').textContent='Could not load schedules: '+e;});

  fetch('/api/runs').then(function(r){return r.json()}).then(function(d){
   document.getElementById('sp-runs').innerHTML='<b>Recent runs</b><br>'+
    ((d.runs||[]).map(function(x){
      return new Date(x.started_at).toLocaleString()+' - '+x.trigger+' - '+x.status;
    }).join('<br>')||'none yet');}).catch(function(){});
 }
})();
</script>
"""

_INDEX = Path(__file__).parent / "static" / "index.html"


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve the existing dashboard with the schedule panel appended."""
    html = _INDEX.read_text(encoding="utf-8")
    if "sched-fab" not in html:
        if "</body>" in html:
            html = html.replace("</body>", PANEL + "</body>", 1)
        else:
            html += PANEL
    return HTMLResponse(html)


# app.py mounts StaticFiles at "/", which matches every path. Routes added
# after a mount are shadowed by it, so push all mounts to the end.
_mounts = [r for r in app.router.routes if isinstance(r, Mount)]
if _mounts:
    app.router.routes = [r for r in app.router.routes if not isinstance(r, Mount)] + _mounts

print("[server] scheduler attached: /, /schedule, /api/healthz, /api/cron/tick")
