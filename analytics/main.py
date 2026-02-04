# analytics/main.py
import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from redis.asyncio import Redis
import redis.asyncio as redis

REDIS_URL = os.getenv("ANTICIP8_ANALYTICS_REDIS_URL", "redis://redis_analytics:6379/0")
app = FastAPI(title="Anticip8 Analytics")
r: Redis = redis.from_url(REDIS_URL, decode_responses=True)

def _k_top2_global() -> str: return "anticip8:chain:top2"
def _k_top3_global() -> str: return "anticip8:chain:top3"
def _k_top2_user(u: str) -> str: return f"anticip8:chain:u:{u}:top2"
def _k_top3_user(u: str) -> str: return f"anticip8:chain:u:{u}:top3"

async def _top_zset(key: str, limit: int) -> List[Dict[str, Any]]:
    # WITHSCORES
    raw = await r.zrevrange(key, 0, max(0, limit - 1), withscores=True)
    return [{"seq": m, "count": int(s)} for (m, s) in raw]

@app.get("/api/top/bigrams")
async def top_bigrams(limit: int = 50):
    return {"key": _k_top2_global(), "items": await _top_zset(_k_top2_global(), limit)}

@app.get("/api/top/trigrams")
async def top_trigrams(limit: int = 50):
    return {"key": _k_top3_global(), "items": await _top_zset(_k_top3_global(), limit)}

@app.get("/api/users/{user}/bigrams")
async def user_bigrams(user: str, limit: int = 50):
    return {"key": _k_top2_user(user), "items": await _top_zset(_k_top2_user(user), limit)}

@app.get("/api/users/{user}/trigrams")
async def user_trigrams(user: str, limit: int = 50):
    return {"key": _k_top3_user(user), "items": await _top_zset(_k_top3_user(user), limit)}

# --------- ultra-simple frontend ----------
INDEX_HTML = r"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Anticip8 Analytics</title>
  <style>
    :root{
      --bg: #0b1020;
      --panel: rgba(255,255,255,.06);
      --panel2: rgba(255,255,255,.08);
      --border: rgba(255,255,255,.10);
      --text: rgba(255,255,255,.92);
      --muted: rgba(255,255,255,.64);
      --accent: #6ee7ff;
      --accent2:#a78bfa;
      --good:#34d399;
      --bad:#fb7185;
      --warn:#fbbf24;
      --shadow: 0 18px 60px rgba(0,0,0,.45);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      --radius: 16px;
    }
    *{ box-sizing:border-box; }
    html,body{ height:100%; }
    body{
      margin:0;
      font-family: var(--sans);
      background:
        radial-gradient(1200px 600px at 15% 10%, rgba(110,231,255,.12), transparent 60%),
        radial-gradient(900px 500px at 85% 15%, rgba(167,139,250,.10), transparent 55%),
        radial-gradient(900px 500px at 40% 90%, rgba(52,211,153,.08), transparent 55%),
        var(--bg);
      color: var(--text);
    }

    /* layout */
    .wrap{ max-width: 1200px; margin: 0 auto; padding: 22px; }
    .topbar{
      display:flex; align-items:center; justify-content:space-between; gap:14px;
      padding: 18px 18px;
      border: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.05));
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .brand{
      display:flex; flex-direction:column; gap:2px;
    }
    .brand h1{ margin:0; font-size: 18px; letter-spacing:.2px; }
    .brand .sub{ font-size: 12px; color: var(--muted); }

    .controls{
      display:flex; gap:10px; align-items:center; flex-wrap:wrap;
    }

    .field{
      display:flex; flex-direction:column; gap:6px;
    }
    label{ font-size: 12px; color: var(--muted); }
    input, select{
      background: rgba(0,0,0,.28);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 9px 10px;
      border-radius: 12px;
      outline: none;
      min-width: 160px;
    }
    input:focus, select:focus{
      border-color: rgba(110,231,255,.55);
      box-shadow: 0 0 0 4px rgba(110,231,255,.10);
    }

    .btn{
      border: 1px solid rgba(110,231,255,.35);
      background: linear-gradient(180deg, rgba(110,231,255,.18), rgba(110,231,255,.08));
      color: var(--text);
      padding: 10px 14px;
      border-radius: 12px;
      cursor:pointer;
      font-weight: 600;
      transition: transform .08s ease, background .2s ease, border-color .2s ease;
      user-select:none;
    }
    .btn:hover{
      transform: translateY(-1px);
      border-color: rgba(110,231,255,.60);
      background: linear-gradient(180deg, rgba(110,231,255,.22), rgba(110,231,255,.10));
    }
    .btn:active{ transform: translateY(0); }
    .btn.secondary{
      border: 1px solid rgba(167,139,250,.35);
      background: linear-gradient(180deg, rgba(167,139,250,.18), rgba(167,139,250,.08));
    }
    .btn.secondary:hover{
      border-color: rgba(167,139,250,.60);
      background: linear-gradient(180deg, rgba(167,139,250,.22), rgba(167,139,250,.10));
    }

    /* stats strip */
    .strip{
      margin-top: 14px;
      display:grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
    }
    @media (max-width: 980px){ .strip{ grid-template-columns: repeat(2,1fr); } }
    @media (max-width: 520px){ .strip{ grid-template-columns: 1fr; } }
    .stat{
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: rgba(255,255,255,.05);
      display:flex; gap:10px; align-items:center; justify-content:space-between;
    }
    .stat .k{ font-size: 12px; color: var(--muted); }
    .stat .v{ font-family: var(--mono); font-size: 13px; color: var(--text); }

    /* cards grid */
    .grid{
      margin-top: 16px;
      display:grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }
    @media (max-width: 980px){ .grid{ grid-template-columns: 1fr; } }

    .card{
      border-radius: var(--radius);
      border: 1px solid var(--border);
      background: rgba(255,255,255,.05);
      box-shadow: 0 18px 60px rgba(0,0,0,.30);
      overflow:hidden;
    }
    .card header{
      padding: 12px 14px;
      display:flex; align-items:center; justify-content:space-between; gap:10px;
      border-bottom: 1px solid var(--border);
      background: rgba(255,255,255,.04);
    }
    .title{
      display:flex; flex-direction:column; gap:2px;
    }
    .title h3{ margin:0; font-size: 14px; }
    .title .hint{ margin:0; font-size: 12px; color: var(--muted); }

    .mini{
      display:flex; align-items:center; gap:10px;
    }
    .mini input{
      min-width: 180px;
      padding: 8px 10px;
      border-radius: 12px;
      font-size: 13px;
    }
    .pill{
      font-family: var(--mono);
      font-size: 12px;
      color: var(--muted);
      border: 1px solid var(--border);
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(0,0,0,.20);
    }

    /* table */
    .table-wrap{ max-height: 420px; overflow:auto; }
    table{ width:100%; border-collapse: collapse; }
    th, td{
      padding: 10px 10px;
      border-bottom: 1px solid rgba(255,255,255,.08);
      font-size: 13px;
      vertical-align: top;
    }
    th{
      position: sticky;
      top: 0;
      z-index: 1;
      background: rgba(14,18,35,.92);
      backdrop-filter: blur(8px);
      text-align:left;
      color: var(--muted);
      font-weight: 600;
    }
    tr:hover td{ background: rgba(110,231,255,.05); }
    td.num{ width: 56px; color: var(--muted); font-family: var(--mono); }
    td.count{ width: 90px; font-family: var(--mono); }
    td.seq{
      font-family: var(--mono);
      white-space: pre-wrap;
      word-break: break-word;
      line-height: 1.35;
    }

    .seqline{
      display:flex; align-items:center; justify-content:space-between; gap:10px;
    }
    .copy{
      border: 1px solid var(--border);
      background: rgba(0,0,0,.20);
      color: var(--muted);
      padding: 6px 10px;
      border-radius: 10px;
      cursor:pointer;
      font-size: 12px;
      flex: 0 0 auto;
      transition: background .15s ease, color .15s ease, border-color .15s ease;
    }
    .copy:hover{
      border-color: rgba(110,231,255,.5);
      color: var(--text);
      background: rgba(110,231,255,.10);
    }

    /* toast */
    .toast{
      position: fixed;
      right: 18px;
      bottom: 18px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: rgba(0,0,0,.55);
      color: var(--text);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
      opacity: 0;
      transform: translateY(8px);
      transition: opacity .18s ease, transform .18s ease;
      max-width: 420px;
      pointer-events: none;
    }
    .toast.show{ opacity: 1; transform: translateY(0); }
    .toast .t{ font-size: 12px; color: var(--muted); margin-bottom: 6px; }
    .toast .m{ font-family: var(--mono); font-size: 12px; white-space: pre-wrap; word-break: break-word; }

    /* loading shimmer */
    .skeleton{
      height: 12px;
      border-radius: 999px;
      background: linear-gradient(90deg, rgba(255,255,255,.04), rgba(255,255,255,.10), rgba(255,255,255,.04));
      background-size: 200% 100%;
      animation: shimmer 1.0s linear infinite;
    }
    @keyframes shimmer { 0%{ background-position: 0% 0; } 100%{ background-position: 200% 0; } }
    .sk-row{ padding: 12px 10px; border-bottom: 1px solid rgba(255,255,255,.08); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <h1>Anticip8 Analytics</h1>
        <div class="sub">Топ цепочек (bi/tri-grams) — global + per user</div>
      </div>

      <div class="controls">
        <div class="field">
          <label for="user">User</label>
          <input id="user" placeholder="u254 / anon" />
        </div>
        <div class="field">
          <label for="limit">Limit</label>
          <input id="limit" value="30" size="4" />
        </div>
        <button class="btn" id="refreshBtn">Refresh</button>
        <button class="btn secondary" id="autoBtn" title="Автообновление каждые 2с">Auto: OFF</button>
      </div>
    </div>

    <div class="strip">
      <div class="stat"><div class="k">Status</div><div class="v" id="status">idle</div></div>
      <div class="stat"><div class="k">User</div><div class="v" id="statUser">anon</div></div>
      <div class="stat"><div class="k">Limit</div><div class="v" id="statLimit">30</div></div>
      <div class="stat"><div class="k">Updated</div><div class="v" id="updated">—</div></div>
    </div>

    <div class="grid">
      <section class="card">
        <header>
          <div class="title">
            <h3>Global bigrams</h3>
            <p class="hint">Самые частые пары</p>
          </div>
          <div class="mini">
            <input id="f_g2" placeholder="filter…" />
            <span class="pill" id="cnt_g2">—</span>
          </div>
        </header>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:56px;">#</th><th>seq</th><th style="width:90px;">count</th></tr></thead>
            <tbody id="tb_g2"></tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <header>
          <div class="title">
            <h3>Global trigrams</h3>
            <p class="hint">Самые частые тройки</p>
          </div>
          <div class="mini">
            <input id="f_g3" placeholder="filter…" />
            <span class="pill" id="cnt_g3">—</span>
          </div>
        </header>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:56px;">#</th><th>seq</th><th style="width:90px;">count</th></tr></thead>
            <tbody id="tb_g3"></tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <header>
          <div class="title">
            <h3>User bigrams</h3>
            <p class="hint">Пары для выбранного user</p>
          </div>
          <div class="mini">
            <input id="f_u2" placeholder="filter…" />
            <span class="pill" id="cnt_u2">—</span>
          </div>
        </header>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:56px;">#</th><th>seq</th><th style="width:90px;">count</th></tr></thead>
            <tbody id="tb_u2"></tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <header>
          <div class="title">
            <h3>User trigrams</h3>
            <p class="hint">Тройки для выбранного user</p>
          </div>
          <div class="mini">
            <input id="f_u3" placeholder="filter…" />
            <span class="pill" id="cnt_u3">—</span>
          </div>
        </header>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:56px;">#</th><th>seq</th><th style="width:90px;">count</th></tr></thead>
            <tbody id="tb_u3"></tbody>
          </table>
        </div>
      </section>
    </div>
  </div>

  <div class="toast" id="toast">
    <div class="t" id="toastTitle">info</div>
    <div class="m" id="toastMsg">—</div>
  </div>

<script>
let AUTO = false;
let AUTO_TIMER = null;

function $(id){ return document.getElementById(id); }

function toast(title, msg, isErr=false){
  $("toastTitle").textContent = title;
  $("toastMsg").textContent = msg;
  const t = $("toast");
  t.style.borderColor = isErr ? "rgba(251,113,133,.45)" : "rgba(110,231,255,.35)";
  t.classList.add("show");
  clearTimeout(t._tm);
  t._tm = setTimeout(()=>t.classList.remove("show"), 2400);
}

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok){
    const text = await r.text();
    throw new Error(text || ("HTTP " + r.status));
  }
  return await r.json();
}

function setSkeleton(tbId){
  const tb = $(tbId);
  tb.innerHTML = "";
  for(let i=0;i<12;i++){
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="num"><div class="skeleton" style="width:24px;"></div></td>
      <td><div class="sk-row"><div class="skeleton" style="width:${40+Math.random()*50}%"></div></div></td>
      <td class="count"><div class="skeleton" style="width:48px;"></div></td>
    `;
    tb.appendChild(tr);
  }
}

function escapeHtml(s){
  return (s||"").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function render(tbId, cntId, items, filterText){
  const tb = $(tbId);
  const q = (filterText||"").trim().toLowerCase();
  const out = q ? items.filter(it => (it.seq||"").toLowerCase().includes(q)) : items;
  $(cntId).textContent = `${out.length}/${items.length}`;

  tb.innerHTML = "";
  out.forEach((it, i) => {
    const seq = it.seq || "";
    const cnt = it.count ?? 0;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="num">${i+1}</td>
      <td class="seq">
        <div class="seqline">
          <div>${escapeHtml(seq)}</div>
          <button class="copy" data-seq="${escapeHtml(seq)}">copy</button>
        </div>
      </td>
      <td class="count">${cnt}</td>
    `;
    tb.appendChild(tr);
  });

  tb.querySelectorAll("button.copy").forEach(btn => {
    btn.addEventListener("click", async () => {
      const s = btn.getAttribute("data-seq") || "";
      try{
        await navigator.clipboard.writeText(s);
        toast("copied", s);
      }catch(e){
        toast("copy failed", String(e), true);
      }
    });
  });
}

async function loadAll(){
  const user = ($("user").value || "anon").trim() || "anon";
  const limit = parseInt(($("limit").value || "30").trim(), 10) || 30;

  $("statUser").textContent = user;
  $("statLimit").textContent = String(limit);

  $("status").textContent = "loading…";
  setSkeleton("tb_g2"); setSkeleton("tb_g3"); setSkeleton("tb_u2"); setSkeleton("tb_u3");

  const t0 = performance.now();
  try{
    const [g2, g3, u2, u3] = await Promise.all([
      fetchJSON(`/api/top/bigrams?limit=${limit}`),
      fetchJSON(`/api/top/trigrams?limit=${limit}`),
      fetchJSON(`/api/users/${encodeURIComponent(user)}/bigrams?limit=${limit}`),
      fetchJSON(`/api/users/${encodeURIComponent(user)}/trigrams?limit=${limit}`)
    ]);

    window.__DATA = { g2: g2.items||[], g3: g3.items||[], u2: u2.items||[], u3: u3.items||[] };

    render("tb_g2", "cnt_g2", window.__DATA.g2, $("f_g2").value);
    render("tb_g3", "cnt_g3", window.__DATA.g3, $("f_g3").value);
    render("tb_u2", "cnt_u2", window.__DATA.u2, $("f_u2").value);
    render("tb_u3", "cnt_u3", window.__DATA.u3, $("f_u3").value);

    const dt = Math.round(performance.now() - t0);
    $("status").textContent = `ok (${dt}ms)`;
    $("updated").textContent = new Date().toLocaleTimeString();
  }catch(e){
    $("status").textContent = "error";
    toast("load error", String(e), true);
  }
}

function wireFilters(){
  const pairs = [
    ["f_g2","tb_g2","cnt_g2","g2"],
    ["f_g3","tb_g3","cnt_g3","g3"],
    ["f_u2","tb_u2","cnt_u2","u2"],
    ["f_u3","tb_u3","cnt_u3","u3"],
  ];
  for(const [fid,tbid,cid,key] of pairs){
    $(fid).addEventListener("input", () => {
      const data = (window.__DATA && window.__DATA[key]) ? window.__DATA[key] : [];
      render(tbid, cid, data, $(fid).value);
    });
  }
}

function setAuto(on){
  AUTO = on;
  $("autoBtn").textContent = on ? "Auto: ON" : "Auto: OFF";
  $("autoBtn").title = on ? "Автообновление каждые 2с" : "Автообновление выключено";
  if(AUTO_TIMER) clearInterval(AUTO_TIMER);
  if(on){
    AUTO_TIMER = setInterval(loadAll, 2000);
  }else{
    AUTO_TIMER = null;
  }
}

$("refreshBtn").addEventListener("click", loadAll);
$("autoBtn").addEventListener("click", () => setAuto(!AUTO));
$("limit").addEventListener("keydown", (e)=>{ if(e.key==="Enter") loadAll(); });
$("user").addEventListener("keydown", (e)=>{ if(e.key==="Enter") loadAll(); });

wireFilters();
loadAll();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML
