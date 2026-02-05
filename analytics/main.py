# analytics/main.py
import os
import re
from typing import Optional, List, Dict, Any, Tuple

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from redis.asyncio import Redis
import redis.asyncio as redis

REDIS_URL = os.getenv("ANTICIP8_ANALYTICS_REDIS_URL", "redis://redis_analytics:6379/0")

app = FastAPI(title="Anticip8 Analytics")
r: Redis = redis.from_url(REDIS_URL, decode_responses=True)

def _k_top2_global() -> str: return "anticip8:chain:top2"
def _k_top3_global() -> str: return "anticip8:chain:top3"
def _k_top2_user(u: str) -> str: return f"anticip8:chain:u:{u}:top2"
def _k_top3_user(u: str) -> str: return f"anticip8:chain:u:{u}:top3"

# --- seq parsing (bigrams) ---
# we accept common formats:
# - "A|B"
# - "A -> B"
# - "A→B"
# - "A,B"
# - "['A','B']" (best-effort)
_SPLIT_RE = re.compile(r"\s*(\|\|\||\|\||\||->|→|,|;|\t)\s*")

def _parse_bigram(seq: str) -> Optional[Tuple[str, str]]:
    s = (seq or "").strip()
    if not s:
        return None

    # try JSON-ish list: ["a","b"]
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        inner = s[1:-1].strip()
        # naive split by comma, strip quotes
        parts = [p.strip().strip("'\"") for p in inner.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]

    parts = [p.strip() for p in _SPLIT_RE.split(s) if p.strip() and not _SPLIT_RE.fullmatch(p.strip())]
    # The regex split returns delimiters too depending; easier: fallback manual
    # Let's do a simpler robust approach:
    for delim in ["|||", "||", "|", "->", "→", ",", ";", "\t"]:
        if delim in s:
            p = [x.strip() for x in s.split(delim)]
            if len(p) >= 2 and p[0] and p[1]:
                return p[0], p[1]

    # last resort: if it looks like "A B" with double spaces? nope.
    return None

async def _top_zset(key: str, limit: int) -> List[Dict[str, Any]]:
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

# ---------------- GRAPH / HEATMAP ----------------

def _build_graph_from_bigrams(items: List[Dict[str, Any]], max_nodes: int = 30, q: str = "") -> Dict[str, Any]:
    """
    items: [{"seq": "...", "count": 123}, ...] from zset top2
    returns:
      nodes: [{"id": "path", "w": sum_weight}, ...]
      links: [{"s": "from", "t": "to", "w": weight}, ...]
      labels: [node_id...]
      matrix: [[w..], ...] (NxN)
    """
    q = (q or "").strip().lower()
    edges: Dict[Tuple[str, str], int] = {}
    node_w: Dict[str, int] = {}

    for it in items:
        seq = it.get("seq") or ""
        cnt = int(it.get("count") or 0)
        parsed = _parse_bigram(seq)
        if not parsed:
            continue
        a, b = parsed
        if q and (q not in a.lower()) and (q not in b.lower()):
            continue

        edges[(a, b)] = edges.get((a, b), 0) + cnt
        node_w[a] = node_w.get(a, 0) + cnt
        node_w[b] = node_w.get(b, 0) + cnt

    # limit nodes by weight
    top_nodes = sorted(node_w.items(), key=lambda kv: kv[1], reverse=True)[:max_nodes]
    keep = {n for (n, _) in top_nodes}

    links = []
    for (a, b), w in sorted(edges.items(), key=lambda kv: kv[1], reverse=True):
        if a in keep and b in keep:
            links.append({"s": a, "t": b, "w": int(w)})

    labels = [n for (n, _) in top_nodes]
    idx = {n: i for i, n in enumerate(labels)}
    N = len(labels)
    matrix = [[0 for _ in range(N)] for __ in range(N)]
    max_w = 0
    for ln in links:
        i = idx[ln["s"]]
        j = idx[ln["t"]]
        matrix[i][j] = int(ln["w"])
        if ln["w"] > max_w:
            max_w = ln["w"]

    nodes = [{"id": n, "w": int(w)} for n, w in top_nodes]

    return {
        "nodes": nodes,
        "links": links,
        "labels": labels,
        "matrix": matrix,
        "max_w": int(max_w),
        "stats": {
            "edges_total": len(edges),
            "edges_kept": len(links),
            "nodes_total": len(node_w),
            "nodes_kept": len(nodes),
        },
    }

@app.get("/api/graph/bigrams")
async def graph_bigrams(limit: int = 200, max_nodes: int = 30, q: str = ""):
    items = await _top_zset(_k_top2_global(), limit)
    return JSONResponse({"key": _k_top2_global(), "graph": _build_graph_from_bigrams(items, max_nodes=max_nodes, q=q)})

@app.get("/api/users/{user}/graph/bigrams")
async def graph_bigrams_user(user: str, limit: int = 200, max_nodes: int = 30, q: str = ""):
    items = await _top_zset(_k_top2_user(user), limit)
    return JSONResponse({"key": _k_top2_user(user), "graph": _build_graph_from_bigrams(items, max_nodes=max_nodes, q=q)})

# --------- ultra-simple frontend (+heatmap +graph) ----------
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

    .wrap{ max-width: 1280px; margin: 0 auto; padding: 22px; }
    .topbar{
      display:flex; align-items:center; justify-content:space-between; gap:14px;
      padding: 18px 18px;
      border: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.05));
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .brand{ display:flex; flex-direction:column; gap:2px; }
    .brand h1{ margin:0; font-size: 18px; letter-spacing:.2px; }
    .brand .sub{ font-size: 12px; color: var(--muted); }

    .controls{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .field{ display:flex; flex-direction:column; gap:6px; }
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

    .strip{
      margin-top: 14px;
      display:grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 12px;
    }
    @media (max-width: 1120px){ .strip{ grid-template-columns: repeat(3,1fr); } }
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
    .title{ display:flex; flex-direction:column; gap:2px; }
    .title h3{ margin:0; font-size: 14px; }
    .title .hint{ margin:0; font-size: 12px; color: var(--muted); }

    .mini{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
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

    .seqline{ display:flex; align-items:center; justify-content:space-between; gap:10px; }
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
      max-width: 520px;
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

    /* viz canvases */
    .viz-wrap{
      padding: 12px 14px 14px;
    }
    .canvas-box{
      border: 1px solid rgba(255,255,255,.10);
      border-radius: 14px;
      background: rgba(0,0,0,.18);
      overflow:hidden;
    }
    canvas{
      display:block;
      width:100%;
      height:420px;
    }
    .viz-meta{
      margin-top: 10px;
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      align-items:center;
      justify-content:space-between;
    }
    .mono{ font-family: var(--mono); color: var(--muted); font-size: 12px; }
    .legend{
      display:flex; gap:10px; align-items:center; flex-wrap:wrap;
    }
    .dot{
      width:10px; height:10px; border-radius:999px;
      border:1px solid rgba(255,255,255,.18);
      background: rgba(110,231,255,.25);
    }
    .dot2{ background: rgba(167,139,250,.22); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <h1>Anticip8 Analytics</h1>
        <div class="sub">Топ цепочек (bi/tri-grams) — + Heatmap/Graph по bigrams</div>
      </div>

      <div class="controls">
        <div class="field">
          <label for="user">User</label>
          <input id="user" placeholder="u254 / anon" />
        </div>
        <div class="field">
          <label for="limit">Limit (tables)</label>
          <input id="limit" value="30" size="4" />
        </div>
        <div class="field">
          <label for="glimit">Limit (graph src)</label>
          <input id="glimit" value="200" size="4" />
        </div>
        <div class="field">
          <label for="maxNodes">Max nodes</label>
          <input id="maxNodes" value="28" size="4" />
        </div>
        <div class="field">
          <label for="gq">Graph filter</label>
          <input id="gq" placeholder="contains path…" />
        </div>

        <button class="btn" id="refreshBtn">Refresh</button>
        <button class="btn secondary" id="autoBtn" title="Автообновление каждые 2с">Auto: OFF</button>
      </div>
    </div>

    <div class="strip">
      <div class="stat"><div class="k">Status</div><div class="v" id="status">idle</div></div>
      <div class="stat"><div class="k">User</div><div class="v" id="statUser">anon</div></div>
      <div class="stat"><div class="k">Updated</div><div class="v" id="updated">—</div></div>
      <div class="stat"><div class="k">Edges kept</div><div class="v" id="g_edges">—</div></div>
      <div class="stat"><div class="k">Nodes kept</div><div class="v" id="g_nodes">—</div></div>
      <div class="stat"><div class="k">Max weight</div><div class="v" id="g_maxw">—</div></div>
    </div>

    <div class="grid">
      <!-- Heatmap -->
      <section class="card">
        <header>
          <div class="title">
            <h3>Heatmap (from → to)</h3>
            <p class="hint">Bigrams: интенсивность перехода</p>
          </div>
          <div class="mini">
            <span class="pill" id="hm_info">click cell</span>
          </div>
        </header>
        <div class="viz-wrap">
          <div class="canvas-box">
            <canvas id="heatmap"></canvas>
          </div>
          <div class="viz-meta">
            <div class="legend">
              <div class="dot"></div><div class="mono">weight ↑</div>
              <div class="dot dot2"></div><div class="mono">diagonal ignored</div>
            </div>
            <div class="mono" id="hm_meta">—</div>
          </div>
        </div>
      </section>

      <!-- Graph -->
      <section class="card">
        <header>
          <div class="title">
            <h3>Graph (from → to)</h3>
            <p class="hint">Bigrams: линии = связи, толщина = count</p>
          </div>
          <div class="mini">
            <span class="pill" id="gr_info">drag: nope, click: node</span>
          </div>
        </header>
        <div class="viz-wrap">
          <div class="canvas-box">
            <canvas id="graph"></canvas>
          </div>
          <div class="viz-meta">
            <div class="mono" id="gr_meta">—</div>
            <div class="mono" id="gr_sel">—</div>
          </div>
        </div>
      </section>

      <!-- tables (existing) -->
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

function renderTable(tbId, cntId, items, filterText){
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

/* ---------- Heatmap ---------- */
function fitCanvas(canvas){
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  return dpr;
}

function heatColor(t){
  // t in [0..1] -> rgba(110,231,255, alpha-ish) but via manual:
  // We'll interpolate between dark and bright cyan.
  const a = 0.08 + 0.70 * t;
  return `rgba(110,231,255,${a.toFixed(3)})`;
}

function drawHeatmap(canvas, labels, matrix, maxW){
  const ctx = canvas.getContext("2d");
  const dpr = fitCanvas(canvas);
  const W = canvas.width, H = canvas.height;

  ctx.clearRect(0,0,W,H);

  const N = labels.length;
  if(!N){
    ctx.fillStyle = "rgba(255,255,255,.55)";
    ctx.font = `${12*dpr}px ui-monospace`;
    ctx.fillText("no data", 14*dpr, 24*dpr);
    return {cell:null};
  }

  const pad = 14 * dpr;
  const top = 30 * dpr;
  const left = 30 * dpr;

  const gridW = W - left - pad;
  const gridH = H - top - pad;

  const cell = Math.max(2*dpr, Math.floor(Math.min(gridW, gridH) / N));
  const gW = cell * N;
  const gH = cell * N;

  // background grid
  ctx.strokeStyle = "rgba(255,255,255,.06)";
  ctx.lineWidth = 1*dpr;

  for(let i=0;i<=N;i++){
    const x = left + i*cell;
    const y = top + i*cell;
    ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, top+gH); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(left+gW, y); ctx.stroke();
  }

  // cells
  const denom = Math.max(1, maxW||1);
  for(let i=0;i<N;i++){
    for(let j=0;j<N;j++){
      const w = matrix[i][j] || 0;
      if(i===j){
        // diagonal: draw subtle purple
        ctx.fillStyle = "rgba(167,139,250,.10)";
      }else if(w<=0){
        ctx.fillStyle = "rgba(255,255,255,.02)";
      }else{
        const t = Math.min(1, Math.log1p(w) / Math.log1p(denom));
        ctx.fillStyle = heatColor(t);
      }
      ctx.fillRect(left + j*cell, top + i*cell, cell, cell);
    }
  }

  // small title
  ctx.fillStyle = "rgba(255,255,255,.65)";
  ctx.font = `${12*dpr}px ui-monospace`;
  ctx.fillText(`N=${N} maxW=${maxW||0} (log scale)`, left, 18*dpr);

  // save geometry for click mapping
  return {cell, left, top, N, dpr, gW, gH};
}

function heatmapPick(geom, x, y){
  if(!geom || !geom.cell) return null;
  const {cell, left, top, N} = geom;
  const i = Math.floor((y - top) / cell);
  const j = Math.floor((x - left) / cell);
  if(i<0 || j<0 || i>=N || j>=N) return null;
  return {i, j};
}

/* ---------- Graph ---------- */
function drawGraph(canvas, labels, links, nodeWeights){
  const ctx = canvas.getContext("2d");
  const dpr = fitCanvas(canvas);
  const W = canvas.width, H = canvas.height;

  ctx.clearRect(0,0,W,H);

  const N = labels.length;
  if(!N){
    ctx.fillStyle = "rgba(255,255,255,.55)";
    ctx.font = `${12*dpr}px ui-monospace`;
    ctx.fillText("no data", 14*dpr, 24*dpr);
    return {nodes:[]};
  }

  // place nodes on circle (cheap and readable)
  const cx = W/2, cy = H/2;
  const R = Math.min(W,H)*0.34;

  const nodes = labels.map((id, idx) => {
    const ang = (idx / N) * Math.PI * 2;
    const x = cx + Math.cos(ang) * R;
    const y = cy + Math.sin(ang) * R;
    const w = nodeWeights[id] || 0;
    return {id, x, y, w};
  });

  const pos = Object.fromEntries(nodes.map(n => [n.id, n]));
  let maxLink = 0;
  for(const l of links) maxLink = Math.max(maxLink, l.w||0);
  const denom = Math.max(1, maxLink);

  // links
  ctx.lineCap = "round";
  for(const l of links){
    const a = pos[l.s], b = pos[l.t];
    if(!a || !b) continue;
    const t = Math.min(1, Math.log1p(l.w||0)/Math.log1p(denom));
    ctx.strokeStyle = `rgba(110,231,255,${(0.10 + 0.55*t).toFixed(3)})`;
    ctx.lineWidth = (1 + 6*t) * dpr;

    // arrow-ish: just line with a tiny head
    ctx.beginPath();
    ctx.moveTo(a.x, a.y);
    ctx.lineTo(b.x, b.y);
    ctx.stroke();

    // arrow head
    const dx = b.x - a.x, dy = b.y - a.y;
    const L = Math.hypot(dx,dy) || 1;
    const ux = dx/L, uy = dy/L;
    const hx = b.x - ux*(10*dpr), hy = b.y - uy*(10*dpr);
    ctx.fillStyle = `rgba(167,139,250,${(0.10 + 0.45*t).toFixed(3)})`;
    ctx.beginPath();
    ctx.moveTo(b.x, b.y);
    ctx.lineTo(hx - uy*(4*dpr), hy + ux*(4*dpr));
    ctx.lineTo(hx + uy*(4*dpr), hy - ux*(4*dpr));
    ctx.closePath();
    ctx.fill();
  }

  // nodes
  let maxNodeW = 0;
  for(const n of nodes) maxNodeW = Math.max(maxNodeW, n.w||0);
  const nDen = Math.max(1, maxNodeW);

  for(const n of nodes){
    const t = Math.min(1, Math.log1p(n.w||0)/Math.log1p(nDen));
    const rad = (6 + 14*t) * dpr;
    ctx.fillStyle = `rgba(255,255,255,${(0.06 + 0.25*t).toFixed(3)})`;
    ctx.strokeStyle = `rgba(110,231,255,${(0.20 + 0.50*t).toFixed(3)})`;
    ctx.lineWidth = 2*dpr;
    ctx.beginPath();
    ctx.arc(n.x, n.y, rad, 0, Math.PI*2);
    ctx.fill(); ctx.stroke();
  }

  // labels
  ctx.fillStyle = "rgba(255,255,255,.70)";
  ctx.font = `${11*dpr}px ui-monospace`;
  for(const n of nodes){
    const txt = n.id.length > 26 ? (n.id.slice(0,23) + "…") : n.id;
    ctx.fillText(txt, n.x + 10*dpr, n.y - 10*dpr);
  }

  // title
  ctx.fillStyle = "rgba(255,255,255,.65)";
  ctx.font = `${12*dpr}px ui-monospace`;
  ctx.fillText(`N=${N} E=${links.length} maxE=${maxLink} (log scale)`, 14*dpr, 20*dpr);

  return {nodes, dpr};
}

function pickNode(graphGeom, x, y){
  if(!graphGeom || !graphGeom.nodes) return null;
  // radius guess based on weight not stored here; use fixed pick radius.
  const R = 18 * (graphGeom.dpr || 1);
  for(const n of graphGeom.nodes){
    const dx = x - n.x, dy = y - n.y;
    if(dx*dx + dy*dy <= R*R) return n;
  }
  return null;
}

/* ---------- Load everything ---------- */
function buildNodeWeightMap(nodes){
  const m = {};
  for(const n of (nodes||[])) m[n.id] = n.w||0;
  return m;
}

async function loadGraph(){
  const user = ($("user").value || "anon").trim() || "anon";
  const glimit = parseInt(($("glimit").value || "200").trim(), 10) || 200;
  const maxNodes = parseInt(($("maxNodes").value || "28").trim(), 10) || 28;
  const q = ($("gq").value || "").trim();

  // prefer user graph if user != anon? We'll load BOTH and show user graph if user filled.
  // For minimal: if user field is non-empty -> show user graph, else global.
  const useUser = user && user !== "anon";

  const url = useUser
    ? `/api/users/${encodeURIComponent(user)}/graph/bigrams?limit=${glimit}&max_nodes=${maxNodes}&q=${encodeURIComponent(q)}`
    : `/api/graph/bigrams?limit=${glimit}&max_nodes=${maxNodes}&q=${encodeURIComponent(q)}`;

  const res = await fetchJSON(url);
  const g = res.graph || {};
  const labels = g.labels || [];
  const matrix = g.matrix || [];
  const links = g.links || [];
  const nodes = g.nodes || [];
  const maxW = g.max_w || 0;
  const st = g.stats || {};

  $("g_edges").textContent = `${st.edges_kept ?? "-"} (of ${st.edges_total ?? "-"})`;
  $("g_nodes").textContent = `${st.nodes_kept ?? "-"} (of ${st.nodes_total ?? "-"})`;
  $("g_maxw").textContent = String(maxW || 0);

  $("hm_meta").textContent = `${useUser ? ("user=" + user) : "global"} | key=${res.key} | q=${q||"—"}`;
  $("gr_meta").textContent = `${useUser ? ("user=" + user) : "global"} | key=${res.key} | q=${q||"—"}`;

  // draw heatmap
  const hmCanvas = $("heatmap");
  window.__HM = window.__HM || {};
  window.__HM.data = {labels, matrix, maxW};
  window.__HM.geom = drawHeatmap(hmCanvas, labels, matrix, maxW);

  // draw graph
  const grCanvas = $("graph");
  const nodeW = buildNodeWeightMap(nodes);
  window.__GR = window.__GR || {};
  window.__GR.data = {labels, links, nodeW};
  window.__GR.geom = drawGraph(grCanvas, labels, links, nodeW);

  $("hm_info").textContent = `cells: ${labels.length}×${labels.length}`;
  $("gr_info").textContent = `nodes: ${labels.length}, edges: ${links.length}`;
}

async function loadTables(){
  const user = ($("user").value || "anon").trim() || "anon";
  const limit = parseInt(($("limit").value || "30").trim(), 10) || 30;

  $("statUser").textContent = user;
  $("status").textContent = "loading…";

  setSkeleton("tb_g2"); setSkeleton("tb_g3"); setSkeleton("tb_u2"); setSkeleton("tb_u3");

  const t0 = performance.now();
  const [g2, g3, u2, u3] = await Promise.all([
    fetchJSON(`/api/top/bigrams?limit=${limit}`),
    fetchJSON(`/api/top/trigrams?limit=${limit}`),
    fetchJSON(`/api/users/${encodeURIComponent(user)}/bigrams?limit=${limit}`),
    fetchJSON(`/api/users/${encodeURIComponent(user)}/trigrams?limit=${limit}`)
  ]);

  window.__DATA = { g2: g2.items||[], g3: g3.items||[], u2: u2.items||[], u3: u3.items||[] };

  renderTable("tb_g2", "cnt_g2", window.__DATA.g2, $("f_g2").value);
  renderTable("tb_g3", "cnt_g3", window.__DATA.g3, $("f_g3").value);
  renderTable("tb_u2", "cnt_u2", window.__DATA.u2, $("f_u2").value);
  renderTable("tb_u3", "cnt_u3", window.__DATA.u3, $("f_u3").value);

  const dt = Math.round(performance.now() - t0);
  $("status").textContent = `ok (${dt}ms)`;
  $("updated").textContent = new Date().toLocaleTimeString();
}

async function loadAll(){
  try{
    await Promise.all([loadTables(), loadGraph()]);
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
      renderTable(tbid, cid, data, $(fid).value);
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

/* clicks */
function canvasClickPos(canvas, ev){
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const x = (ev.clientX - rect.left) * dpr;
  const y = (ev.clientY - rect.top) * dpr;
  return {x,y};
}

$("heatmap").addEventListener("click", (ev) => {
  const {x,y} = canvasClickPos($("heatmap"), ev);
  const geom = window.__HM && window.__HM.geom;
  const data = window.__HM && window.__HM.data;
  const hit = heatmapPick(geom, x, y);
  if(!hit || !data) return;

  const {i,j} = hit;
  const from = data.labels[i];
  const to = data.labels[j];
  const w = (data.matrix[i] && data.matrix[i][j]) ? data.matrix[i][j] : 0;
  $("hm_info").textContent = `from="${from}" → to="${to}" : ${w}`;
  toast("heatmap", `${from} -> ${to}\ncount=${w}`);
});

$("graph").addEventListener("click", (ev) => {
  const {x,y} = canvasClickPos($("graph"), ev);
  const geom = window.__GR && window.__GR.geom;
  const data = window.__GR && window.__GR.data;
  const n = pickNode(geom, x, y);
  if(!n || !data) return;

  // show outgoing links from this node
  const out = (data.links || []).filter(l => l.s === n.id).sort((a,b)=> (b.w||0)-(a.w||0)).slice(0, 10);
  const lines = out.map(l => `${l.s} -> ${l.t} : ${l.w}`).join("\n") || "(no outgoing)";
  $("gr_sel").textContent = `selected: ${n.id} (w=${n.w||0})`;
  toast("node", `${n.id}\n\n${lines}`);
});

window.addEventListener("resize", () => {
  // redraw from cached data
  if(window.__HM && window.__HM.data){
    const {labels, matrix, maxW} = window.__HM.data;
    window.__HM.geom = drawHeatmap($("heatmap"), labels, matrix, maxW);
  }
  if(window.__GR && window.__GR.data){
    const {labels, links, nodeW} = window.__GR.data;
    window.__GR.geom = drawGraph($("graph"), labels, links, nodeW);
  }
});

$("refreshBtn").addEventListener("click", loadAll);
$("autoBtn").addEventListener("click", () => setAuto(!AUTO));

["limit","glimit","maxNodes","user","gq"].forEach(id=>{
  $(id).addEventListener("keydown", (e)=>{ if(e.key==="Enter") loadAll(); });
});
$("gq").addEventListener("input", () => {
  // don't DDOS: small debounce
  clearTimeout(window.__GQ_T);
  window.__GQ_T = setTimeout(loadGraph, 220);
});

wireFilters();
loadAll();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML
