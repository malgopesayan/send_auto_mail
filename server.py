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
# rewrites token.json whenever it refreshes the hourly access token, so a
# writable copy must exist before GmailService loads.
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
<div id="sched-fab" title="Schedule">&#9200;</div>
<div id="sched-back"></div>
<div id="sched-panel">
  <div class="sp-head">
    <strong>SCHEDULE</strong><span id="sp-tz"></span>
    <span style="flex:1"></span><span id="sp-close">&times;</span>
  </div>
  <div id="sp-rows">loading...</div>
  <div class="sp-actions">
    <button type="button" id="sp-add">+ ADD TIME</button>
    <button type="button" id="sp-run">RUN NOW</button>
  </div>
  <div id="sp-status"></div>
  <div id="sp-runs"></div>
</div>
<style>
#sched-fab{position:fixed;left:16px;bottom:76px;width:46px;height:46px;border-radius:50%;
 background:#f5a524;color:#1a1a1a;display:flex;align-items:center;justify-content:center;
 font-size:22px;cursor:pointer;z-index:9998;box-shadow:0 4px 14px rgba(0,0,0,.5)}
#sched-back{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9998}
#sched-back.open{display:block}
#sched-panel{position:fixed;left:16px;bottom:134px;width:380px;max-height:68vh;overflow:auto;
 background:#14161a;color:#d8d8d8;border:1px solid #2e3238;border-radius:10px;padding:14px;
 z-index:9999;display:none;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px;
 box-shadow:0 10px 30px rgba(0,0,0,.6)}
#sched-panel.open{display:block}
#sched-panel .sp-head{display:flex;align-items:center;gap:8px;margin-bottom:10px;
 letter-spacing:.06em;color:#f5a524}
#sched-panel #sp-close{cursor:pointer;font-size:22px;opacity:.7;padding:0 6px;color:#d8d8d8}
#sched-panel .sp-row{display:flex;gap:6px;align-items:center;flex-wrap:wrap;
 border:1px solid #23262b;border-radius:8px;padding:8px;margin:8px 0;background:#181b20}
#sched-panel input,#sched-panel button{font:inherit;padding:6px 8px;border-radius:6px;
 border:1px solid #3a3f46;background:#1e2127;color:#e8e8e8}
#sched-panel input[type=text]{width:104px}
#sched-panel button{cursor:pointer}
#sched-panel button:hover{background:#2a2e35}
#sched-panel .sp-actions{display:flex;gap:8px;margin-top:4px}
#sched-panel #sp-tz,#sched-panel #sp-status,#sched-panel #sp-runs,#sched-panel .sp-last{
 opacity:.6;font-size:11px}
#sched-panel #sp-status{margin-top:8px;color:#f5a524;opacity:.9;word-break:break-word}
#sched-panel .sp-last{width:100%}
#sched-panel #sp-runs{margin-top:10px;border-top:1px solid #23262b;padding-top:8px;
 line-height:1.6}
@media (max-width:560px){
 #sched-panel{left:8px;right:8px;width:auto;bottom:70px;max-height:78vh}
 #sched-fab{bottom:72px;left:12px}
 #sched-panel .sp-row{flex-direction:column;align-items:stretch}
 #sched-panel input[type=text],#sched-panel input[type=time]{width:100%}
 #sched-panel .sp-actions button{flex:1}
}
</style>
<script>
(function(){
 var P=document.getElementById('sched-panel'),B=document.getElementById('sched-back');
 function open(){P.classList.add('open');B.classList.add('open');spLoad();}
 function shut(){P.classList.remove('open');B.classList.remove('open');}
 document.getElementById('sched-fab').onclick=function(){
   P.classList.contains('open')?shut():open();};
 document.getElementById('sp-close').onclick=shut; B.onclick=shut;

 function say(m){document.getElementById('sp-status').textContent=m;}

 function call(u,o){return fetch(u,o).then(function(r){
   if(!r.ok){return r.text().then(function(t){
     say('Error '+r.status+': '+t); throw new Error(t);});}
   return r.json();});}

 document.getElementById('sp-add').onclick=function(){
   say('');
   call('/api/schedules',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({label:'New run',run_at:'10:00',enabled:true})})
    .then(spLoad).catch(function(){});};

 document.getElementById('sp-run').onclick=function(){
   say('');
   call('/api/full-run',{method:'POST'}).then(function(){
     say('Started - open the Console tab to watch.');
     setTimeout(spLoad,1500);}).catch(function(){});};

 window.spSave=function(id){
  var r=document.querySelector('[data-sid="'+id+'"]');
  say('');
  call('/api/schedules/'+id,{method:'PATCH',
   headers:{'Content-Type':'application/json'},
   body:JSON.stringify({label:r.querySelector('.sp-l').value.trim()||'Run',
                        run_at:r.querySelector('.sp-t').value,
                        enabled:r.querySelector('.sp-o').checked})})
   .then(function(){say('Saved.');spLoad();}).catch(function(){});};

 window.spDel=function(id){
  if(!confirm('Delete this schedule?'))return;
  say('');
  call('/api/schedules/'+id,{method:'DELETE'}).then(spLoad).catch(function(){});};

 function spLoad(){
  call('/api/schedules').then(function(d){
   document.getElementById('sp-tz').textContent='('+d.timezone+')';
   document.getElementById('sp-rows').innerHTML=(d.schedules||[]).map(function(s){
    return '<div class="sp-row" data-sid="'+s.id+'">'+
      '<input type="text" class="sp-l" value="'+s.label+'">'+
      '<input type="time" class="sp-t" value="'+String(s.run_at).slice(0,5)+'">'+
      '<label><input type="checkbox" class="sp-o" '+(s.enabled?'checked':'')+'> on</label>'+
      '<button onclick="spSave('+s.id+')">SAVE</button>'+
      '<button onclick="spDel('+s.id+')">DEL</button>'+
      '<div class="sp-last">last run: '+
        (s.last_run_at?new Date(s.last_run_at).toLocaleString():'never')+'</div></div>';
   }).join('')||'No times set yet.';
   if(d.running) say('A run is in progress...');
  }).catch(function(){});

  call('/api/runs').then(function(d){
   document.getElementById('sp-runs').innerHTML='RECENT RUNS<br>'+
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
        html = (html.replace("</body>", PANEL + "</body>", 1)
                if "</body>" in html else html + PANEL)
    return HTMLResponse(html)


# app.py mounts StaticFiles at "/", which matches every path. Routes added
# after a mount are shadowed by it, so push all mounts to the end.
_mounts = [r for r in app.router.routes if isinstance(r, Mount)]
if _mounts:
    app.router.routes = [r for r in app.router.routes if not isinstance(r, Mount)] + _mounts

print("[server] scheduler attached: /, /schedule, /api/healthz, /api/cron/tick")
