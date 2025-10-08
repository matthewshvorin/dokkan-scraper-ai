# cards_site.py
import json
import os
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from flask import Flask, send_from_directory, abort, jsonify, render_template_string, request

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
CARDS_ROOT = OUTPUT_DIR / "cards"
ASSETS_ROOT = OUTPUT_DIR / "assets"  # expects 'dokkaninfo.com/...' under here

app = Flask(__name__)

print(f"Serving cards from: {CARDS_ROOT}")
print(f"Serving ASSETS from: {ASSETS_ROOT}  (expects dokkaninfo.com/... under here)")

# ---------- Helpers: assets ----------

def safe_join_assets(rel: str) -> Optional[Path]:
    rel = (rel or "").replace("\\", "/").lstrip("/")
    full = (ASSETS_ROOT / rel).resolve()
    try:
        full.relative_to(ASSETS_ROOT.resolve())
    except Exception:
        return None
    return full

def url_for_asset(rel: Optional[str]) -> str:
    if not rel:
        return ""
    rel = rel.replace("\\", "/")
    return f"/assets/{rel}"

def asset_rel_exists(rel: Optional[str]) -> bool:
    if not rel:
        return False
    p = safe_join_assets(rel)
    return bool(p and p.exists())

# ---------- Helpers: picking best image per unit ----------

def _first_variant(meta: Dict[str, Any]) -> Dict[str, Any]:
    variants = meta.get("variants") or []
    if isinstance(variants, list) and variants:
        v = variants[0]
        if isinstance(v, dict):
            return v
    return meta

def _assets_index(meta: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    v0 = _first_variant(meta)
    idx = v0.get("assets_index")
    return idx if isinstance(idx, dict) else {}

def find_card_art_full(meta: Dict[str, Any]) -> Optional[str]:
    idx = _assets_index(meta)
    arts = idx.get("card_art") or []
    for item in arts:
        if isinstance(item, dict) and item.get("subtype") == "full_card":
            rel = item.get("path")
            if asset_rel_exists(rel):
                return rel
    return None

def find_unit_thumb(meta: Dict[str, Any]) -> Optional[str]:
    v0 = _first_variant(meta)
    unit_id = v0.get("unit_id") or meta.get("unit_id")
    idx = _assets_index(meta)
    thumbs = idx.get("thumbnail") or []

    for item in thumbs:
        if not isinstance(item, dict):
            continue
        if str(item.get("card_id")) == str(unit_id):
            rel = item.get("path")
            if asset_rel_exists(rel):
                return rel

    for item in thumbs:
        if not isinstance(item, dict):
            continue
        rel = item.get("path")
        if asset_rel_exists(rel):
            return rel
    return None

def pick_best_family_image(meta: Dict[str, Any]) -> Tuple[str, str]:
    rel = find_card_art_full(meta)
    if rel:
        print(f"[THUMB] full-card used for {meta.get('display_name') or meta.get('unit_id')} -> {rel}")
        return ("card_art_full", rel)

    rel = find_unit_thumb(meta)
    if rel:
        print(f"[THUMB] thumbnail used for {meta.get('display_name') or meta.get('unit_id')} -> {rel}")
        return ("thumbnail", rel)

    unit_id = (_first_variant(meta).get("unit_id") or meta.get("unit_id") or "????")
    print(f"[THUMB] placeholder used for {meta.get('display_name')} ({unit_id})")
    return ("placeholder", f"/placeholder/{unit_id}")

def choose_family_thumb(meta: Dict[str, Any]) -> str:
    kind, rel = pick_best_family_image(meta)
    print(f"[THUMB] picked kind={kind} rel={rel}")
    return rel

# ---------- Robust metadata discovery ----------

META_CANDIDATES = ["meta.json", "card.json", "index.json"]

def discover_meta_file(folder: Path) -> Optional[Path]:
    # 1) preferred names
    for name in META_CANDIDATES:
        p = folder / name
        if p.exists():
            return p

    # 2) any *meta*.json
    metas = sorted(folder.glob("*meta*.json"))
    if metas:
        return metas[0]

    # 3) any JSON -> pick the largest (often the real payload)
    jsons = list(folder.glob("*.json"))
    if jsons:
        jsons.sort(key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
        return jsons[0]

    return None

def load_meta(folder: Path) -> Optional[Dict[str, Any]]:
    meta_path = discover_meta_file(folder)
    if not meta_path:
        print(f"[META] No JSON found in: {folder}")
        return None
    try:
        with meta_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                print(f"[META] Top-level JSON is not an object in {meta_path.name} (type={type(data).__name__})")
                return None
            data.setdefault("__meta_file__", meta_path.name)
            return data
    except Exception as e:
        print(f"[META] Failed to parse {meta_path}: {e}")
        return None

def build_family_summary(folder: Path, meta: Dict[str, Any]) -> Dict[str, Any]:
    v0 = _first_variant(meta)
    unit_id = v0.get("unit_id") or meta.get("unit_id") or folder.name
    display_name = meta.get("display_name") or v0.get("display_name") or folder.name
    rarity = v0.get("rarity") or meta.get("rarity") or "?"
    ctype = v0.get("type") or meta.get("type") or "?"
    source_url = v0.get("source_base_url") or meta.get("source_base_url")
    eza = bool(v0.get("eza"))
    thumb_rel = choose_family_thumb(meta)

    variants = meta.get("variants")
    num_variants = len(variants) if isinstance(variants, list) else 1
    eza_steps = 0
    if isinstance(v0, dict) and isinstance(v0.get("eza_steps"), list):
        eza_steps = len(v0["eza_steps"])

    return {
        "folder_name": folder.name,
        "unit_id": str(unit_id),
        "display_name": display_name,
        "rarity": rarity,
        "type": ctype,
        "source_url": source_url,
        "thumb_rel": thumb_rel,
        "eza": eza,
        "num_variants": num_variants,
        "eza_steps": eza_steps,
        "__meta_file__": meta.get("__meta_file__", "?"),
    }

def load_families() -> List[Dict[str, Any]]:
    families: List[Dict[str, Any]] = []
    if not CARDS_ROOT.exists():
        print(f"[LOAD] Cards root missing: {CARDS_ROOT}")
        return families

    count_dirs = 0
    for child in sorted(CARDS_ROOT.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        count_dirs += 1
        meta = load_meta(child)
        if not meta:
            print(f"[LOAD] Skipped (no meta): {child.name}")
            continue
        try:
            fam = build_family_summary(child, meta)
            families.append(fam)
        except Exception as e:
            print(f"[LOAD] Skipped {child.name}: {e}")

    print(f"[LOAD] scanned_dirs={count_dirs} loaded_units={len(families)}")
    return families

# ---------- Routes: assets & tools ----------

@app.get("/assets/<path:rel>")
def serve_asset(rel: str):
    rel_norm = (rel or "").replace("\\", "/").lstrip("/")
    full = safe_join_assets(rel_norm)
    print(f"[ASSETS] exact: rel='{rel_norm}' -> full='{full}' exists={full.exists() if full else None}")
    if not full or not full.exists():
        print(f"[ASSETS] 404: rel='{rel_norm}' (no match)")
        abort(404)
    return send_from_directory(str(full.parent), full.name)

@app.get("/__exists")
def probe_exists():
    rel = request.args.get("rel", "", type=str)
    full = safe_join_assets(rel) if rel else None
    exists = bool(full and full.exists())
    print(f"[EXISTS] rel='{rel}' -> '{full}' exists={exists}")
    return jsonify({"rel": rel, "full": str(full) if full else None, "exists": exists})

@app.get("/__scan")
def scan_page():
    rows = []
    if CARDS_ROOT.exists():
        for child in sorted(CARDS_ROOT.iterdir(), key=lambda p: p.name):
            if not child.is_dir(): continue
            meta_path = discover_meta_file(child)
            rows.append((child.name, meta_path.name if meta_path else "—", str(meta_path) if meta_path else ""))
    html = ["<h1>Cards scan</h1><table border='1' cellspacing='0' cellpadding='6'>",
            "<tr><th>Folder</th><th>Chosen JSON</th><th>Full Path</th></tr>"]
    for folder, name, full in rows:
        html.append(f"<tr><td>{folder}</td><td>{name}</td><td><code>{full}</code></td></tr>")
    html.append("</table>")
    html.append(f"<p>Root: <code>{CARDS_ROOT}</code></p>")
    return "\n".join(html)

# ---------- Placeholder art (SVG) ----------

@app.get("/placeholder/<unit_id>")
def placeholder(unit_id: str):
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="300" height="420">
  <defs>
    <linearGradient id="g" x1="0" x2="1">
      <stop offset="0" stop-color="#1f2937"/>
      <stop offset="1" stop-color="#374151"/>
    </linearGradient>
  </defs>
  <rect width="100%" height="100%" fill="url(#g)"/>
  <rect x="10" y="10" width="280" height="400" rx="18" fill="none" stroke="#4b5563" stroke-width="2"/>
  <text x="50%" y="45%" dominant-baseline="middle" text-anchor="middle" fill="#9ca3af" font-family="Segoe UI, Arial" font-size="16">No art found</text>
  <text x="50%" y="55%" dominant-baseline="middle" text-anchor="middle" fill="#e5e7eb" font-family="Segoe UI, Arial" font-size="22">Unit {unit_id}</text>
</svg>'''
    return app.response_class(svg, mimetype="image/svg+xml")

# ---------- UI ----------

INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Dokkan Cards — Local Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {
      --bg: #0b0f18; --card: #131a26; --muted: #9aa4b2; --text: #e9eef7; --chip: #1f2937; --chip-br: #2b3646;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: linear-gradient(180deg, #0b0f18, #0f1623 40%, #0b0f18); color: var(--text); font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    header { position: sticky; top: 0; z-index: 10; backdrop-filter: blur(8px); background: rgba(11,15,24,0.75); border-bottom: 1px solid #1d2533; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 18px 20px; }
    h1 { margin: 0; font-size: 22px; font-weight: 700; letter-spacing: .2px; }
    .toolbar { display: grid; grid-template-columns: 1fr auto auto; gap: 10px; margin-top: 12px; }
    .search { display: flex; gap: 8px; align-items: center; background: var(--card); border: 1px solid #1d2533; border-radius: 12px; padding: 10px 12px; }
    .search input { flex: 1; background: transparent; border: none; outline: none; color: var(--text); font-size: 14px; }
    .chip, select, button { background: var(--chip); border: 1px solid var(--chip-br); color: var(--text); border-radius: 12px; padding: 10px 12px; font-size: 14px; }
    .grid { display: grid; gap: 14px; padding: 16px 20px 60px; max-width: 1200px; margin: 0 auto; grid-template-columns: repeat( auto-fill, minmax(240px, 1fr) ); }
    .card { background: var(--card); border: 1px solid #1d2533; border-radius: 16px; overflow: hidden; display: flex; flex-direction: column; min-height: 420px; transition: transform .15s ease, box-shadow .2s, border-color .2s; }
    .card:hover { transform: translateY(-2px); border-color: #2b3646; box-shadow: 0 6px 24px rgba(0,0,0,.35); }
    .thumb { position: relative; background: #0d1320; aspect-ratio: 300 / 420; display: grid; place-items: center; overflow: hidden; }
    .thumb img { width: 100%; height: 100%; object-fit: cover; display: block; }
    .badge { position: absolute; top: 10px; left: 10px; display: flex; gap: 6px; }
    .pill { background: rgba(0,0,0,.35); border: 1px solid rgba(255,255,255,.12); padding: 4px 8px; border-radius: 999px; font-size: 12px; backdrop-filter: blur(4px); }
    .meta { padding: 12px; display: grid; gap: 6px; }
    .name { font-weight: 700; font-size: 15px; line-height: 1.25; }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .tag { background: var(--chip); border: 1px solid var(--chip-br); padding: 4px 8px; border-radius: 8px; font-size: 12px; color: #9aa4b2; }
    .actions { display: flex; gap: 8px; padding: 0 12px 12px; }
    .actions a, .actions button { text-decoration: none; cursor: pointer; background: #142032; border: 1px solid #223148; color: var(--text); padding: 8px 10px; border-radius: 10px; font-size: 13px; }
    .muted { color: #9aa4b2; }
    .footer { text-align: center; color: #9aa4b2; font-size: 12px; padding: 14px 20px 30px; }
    .hide { display: none !important; }
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <h1>Dokkan Cards — Local Viewer</h1>
      <div class="toolbar">
        <div class="search">
          <span class="muted">🔎</span>
          <input id="q" placeholder="Search name or unit id…" autocomplete="off"/>
        </div>
        <select id="rarity">
          <option value="">All Rarities</option>
          <option>LR</option><option>UR</option><option>SSR</option><option>SR</option><option>R</option><option>N</option>
        </select>
        <select id="type">
          <option value="">All Types</option>
          <option>TEQ</option><option>AGL</option><option>STR</option><option>PHY</option><option>INT</option>
          <option>Super TEQ</option><option>Extreme TEQ</option>
          <option>Super AGL</option><option>Extreme AGL</option>
          <option>Super STR</option><option>Extreme STR</option>
          <option>Super PHY</option><option>Extreme PHY</option>
          <option>Super INT</option><option>Extreme INT</option>
        </select>
      </div>
    </div>
  </header>

  <main class="grid" id="grid">
    {% for f in families %}
    {% set is_placeholder = f.thumb_rel and f.thumb_rel.startswith('/') %}
    <article class="card" data-name="{{ f.display_name|e }}" data-id="{{ f.unit_id|e }}" data-rarity="{{ f.rarity|e }}" data-type="{{ f.type|e }}">
      <div class="thumb">
        <img
          loading="lazy"
          src="{{ f.thumb_rel if is_placeholder else url_for_asset(f.thumb_rel) }}"
          alt="{{ f.display_name|e }}"
          data-rel="{{ f.thumb_rel }}"
          onerror="console.warn('[IMG-404]', this.dataset.rel, '->', this.src);
                   if(!this.dataset.rel.startsWith('/')){
                     fetch('/__exists?rel='+encodeURIComponent(this.dataset.rel))
                      .then(r=>r.json()).then(j=>console.log('[IMG-EXISTS]', j));
                   }"
        />
        <div class="badge">
          <span class="pill">{{ f.rarity }}</span>
          <span class="pill">{{ f.type }}</span>
          {% if f.eza %}<span class="pill" title="Has EZA">EZA</span>{% endif %}
        </div>
      </div>
      <div class="meta">
        <div class="name">{{ f.display_name }}</div>
        <div class="row">
          <span class="tag">Unit {{ f.unit_id }}</span>
          <span class="tag muted">JSON: {{ f.__meta_file__ }}</span>
          {% if f.num_variants > 1 %}<span class="tag">{{ f.num_variants }} variants</span>{% endif %}
        </div>
      </div>
      <div class="actions">
        {% if f.source_url %}
          <a href="{{ f.source_url }}" target="_blank" rel="noreferrer">Open on DokkanInfo</a>
        {% endif %}
        <button onclick="copyText('{{ f.unit_id }}')">Copy ID</button>
        <button onclick="peekAssets('{{ f.unit_id|e }}', '{{ f.thumb_rel|e }}')">Peek</button>
      </div>
    </article>
    {% endfor %}
  </main>

  <div class="footer muted">Showing {{ families|length }} units • Assets served from <code>output/dokkaninfo.com/…</code> • <a href="/__scan" style="color:#7dd3fc">scan</a></div>

  <script>
    const q = document.getElementById('q');
    const rarity = document.getElementById('rarity');
    const typeSel = document.getElementById('type');
    const grid = document.getElementById('grid');

    function applyFilter(){
      const term = (q.value || '').trim().toLowerCase();
      const r = rarity.value;
      const t = typeSel.value;
      let shown = 0;

      for (const card of grid.children) {
        const name = (card.dataset.name || '').toLowerCase();
        const id = (card.dataset.id || '').toLowerCase();
        const cr = card.dataset.rarity || '';
        const ct = card.dataset.type || '';

        const matchesTerm = !term || name.includes(term) || id.includes(term);
        const matchesR = !r || cr === r;
        const matchesT = !t || ct === t;

        const ok = matchesTerm && matchesR && matchesT;
        card.classList.toggle('hide', !ok);
        if (ok) shown++;
      }
      console.log('[FILTER] shown=', shown);
    }

    q.addEventListener('input', applyFilter);
    rarity.addEventListener('change', applyFilter);
    typeSel.addEventListener('change', applyFilter);

    function copyText(text){
      navigator.clipboard.writeText(text).then(()=> console.log('[COPY]', text));
    }
    function peekAssets(unitId, rel){
      console.log('[PEEK]', { unitId, rel });
      if (!rel || rel.startsWith('/')) { alert('Using placeholder — no local art found.'); return; }
      fetch('/__exists?rel='+encodeURIComponent(rel))
        .then(r=>r.json())
        .then(j=>{ alert((j.exists ? '✅ Found:\n' : '❌ Missing:\n') + j.full); })
        .catch(()=>alert('Probe failed.'));
    }
    applyFilter();
  </script>
</body>
</html>
"""

@app.get("/")
def home():
    fams = load_families()
    try:
        fams.sort(key=lambda f: int(''.join(ch for ch in f["unit_id"] if ch.isdigit())))
    except Exception:
        pass
    return render_template_string(INDEX_HTML, families=fams, url_for_asset=url_for_asset)

# ---------- Main ----------
if __name__ == "__main__":
    app.run(debug=True)
