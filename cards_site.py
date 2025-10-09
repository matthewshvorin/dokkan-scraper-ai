import json
import re
from pathlib import Path
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, abort, jsonify, render_template_string, request, send_file

# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
CARDS_DIR = OUTPUT_DIR / "cards"
ASSETS_ROOT = OUTPUT_DIR / "assets"  # expects 'dokkaninfo.com/...' under here

# --------------------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------------------
app = Flask(__name__)

print(f"Serving cards from: {CARDS_DIR}")
print(f"Serving ASSETS from: {ASSETS_ROOT}  (expects dokkaninfo.com/... under here)")

# --------------------------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------------------------
def norm_rel(rel: str) -> str:
    return (rel or "").replace("\\", "/").lstrip("/")

def safe_get(d: Any, path: str, default=None):
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur

def parse_dt(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    for f in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, f)
        except Exception:
            pass
    return None

def asset_candidates(index_obj: Any, category: str, subtype: Optional[str] = None, card_id: Optional[str] = None) -> List[Dict]:
    recs: List[Dict] = []
    if isinstance(index_obj, dict):
        recs = list(index_obj.get(category, []))
    elif isinstance(index_obj, list):
        recs = [x for x in index_obj if isinstance(x, dict) and x.get("category") == category]

    def score(it: Dict) -> Tuple[int, int]:
        s1 = 1 if (subtype and it.get("subtype") == subtype) else 0
        s2 = 1 if (card_id and str(it.get("card_id")) == str(card_id)) else 0
        return (s1, s2)
    recs.sort(key=score, reverse=True)
    return recs

# canonical art finders
FULL_CARD_RE = re.compile(r"[\\/](\d{7})[\\/]\1\.png$")
CHAR_CARD_RE = re.compile(r"/character/card/(\d{7})/card_\1_character\.png$")
THUMB_CANON = "dokkaninfo.com/assets/global/en/character/thumb/card_{cid}_thumb/card_{cid}_thumb.png"

def extract_card_id(variant: Dict, full: Optional[str], char: Optional[str]) -> Optional[str]:
    vid = str(variant.get("form_id") or variant.get("unit_id") or "")
    if vid.isdigit() and len(vid) == 7:
        return vid
    if full:
        m = FULL_CARD_RE.search(full)
        if m: return m.group(1)
    if char:
        m = CHAR_CARD_RE.search(char)
        if m: return m.group(1)
    return None

def choose_variant_art(variant: Dict) -> Dict[str, Optional[str]]:
    """Return canonical art URLs: full, character, thumb (from card id), and the card id."""
    idx = variant.get("assets_index") or {}
    v_id = variant.get("form_id") or variant.get("unit_id")

    full = char = None
    ca_full = asset_candidates(idx, "card_art", subtype="full_card", card_id=v_id)
    if ca_full: full = norm_rel(ca_full[0].get("path", ""))

    ca_char = asset_candidates(idx, "card_art", subtype="character", card_id=v_id)
    if ca_char: char = norm_rel(ca_char[0].get("path", ""))

    for a in (variant.get("assets") or []):
        a_rel = norm_rel(a)
        if not full and FULL_CARD_RE.search(a_rel):
            full = a_rel
        if not char and CHAR_CARD_RE.search(a_rel):
            char = a_rel

    cid = extract_card_id(variant, full, char)
    thumb = f"/assets/{THUMB_CANON.format(cid=cid)}" if cid else None

    return {
        "full": f"/assets/{full}" if full else None,
        "character": f"/assets/{char}" if char else None,
        "thumb": thumb,
        "cid": cid,
    }

def choose_images_for_list(variant: Optional[Dict]) -> Dict[str, Optional[str]]:
    if not variant:
        return {"grid": None, "full": None, "thumb": None, "cid": None, "character": None}
    art = choose_variant_art(variant)
    return {
        "grid": art["full"] or art["character"] or art["thumb"],
        "full": art["full"],
        "thumb": art["thumb"],
        "cid": art["cid"],
        "character": art["character"],
    }

def best_variant_for_display(variants: List[Dict]) -> Dict:
    if not variants:
        return {}
    eza_vs = [v for v in variants if v.get("eza") and not v.get("is_super_eza")]
    seza_vs = [v for v in variants if v.get("eza") and v.get("is_super_eza")]
    def key(v):
        try: return int(v.get("step") or 0)
        except Exception: return 0
    if seza_vs:
        return sorted(seza_vs, key=key)[-1]
    if eza_vs:
        return sorted(eza_vs, key=key)[-1]
    base = next((v for v in variants if v.get("key") == "base"), None)
    return base or variants[0]

def _nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())

def _has_lines(obj: Any) -> bool:
    if not isinstance(obj, dict): return False
    lines = obj.get("lines")
    if not isinstance(lines, list): return False
    for ln in lines:
        if isinstance(ln, dict) and (_nonempty_str(ln.get("text")) or _nonempty_str(ln.get("context"))):
            return True
    return False

def _has_active_skill(kit: Dict) -> bool:
    a = kit.get("active_skill")
    if not isinstance(a, dict):
        return False
    return (
        _nonempty_str(a.get("name")) or
        _nonempty_str(a.get("effect")) or
        _nonempty_str(a.get("activation_conditions")) or
        _has_lines(a)
    )

def _has_standby_skill(kit: Dict) -> bool:
    s = kit.get("standby_skill")
    if not isinstance(s, dict):
        return False
    return (
        _nonempty_str(s.get("name")) or
        _nonempty_str(s.get("effect")) or
        _nonempty_str(s.get("activation_conditions")) or
        _has_lines(s)
    )

def _has_giant_form(kit: Dict) -> bool:
    """Only exact 'Giant Form' (case-insensitive). Avoids 'Giant Ape Power'."""
    cats = [c.strip().lower() for c in (kit.get("categories") or [])]
    if "giant form" in cats:
        return True
    if safe_get(kit, "giant_form.can_transform") or kit.get("giant_form"):
        return True
    return False

def _has_revival(kit: Dict) -> bool:
    return bool(kit.get("revival") or kit.get("revival_skill") or safe_get(kit, "revival.can_revive"))

def mechanics_flags(variants: List[Dict]) -> List[str]:
    flags = set()
    for v in variants:
        kit = v.get("kit") or {}
        if v.get("eza"):
            if v.get("is_super_eza"):
                flags.add("SEZA")
            else:
                flags.add("EZA")
        if safe_get(kit, "transformation.can_transform"):
            flags.add("Transforms")
        if safe_get(kit, "reversible_exchange.can_exchange"):
            flags.add("Exchange")
        if _has_standby_skill(kit):
            flags.add("Standby")
        if _has_active_skill(kit):
            flags.add("Active")
        if _has_giant_form(kit):
            flags.add("Giant Form")
        if _has_revival(kit):
            flags.add("Revival")
    order = ["SEZA","EZA","Transforms","Exchange","Standby","Active","Giant Form","Revival"]
    return sorted(flags, key=lambda s: order.index(s) if s in order else 99)

def compact_passive_lines(passive: Dict) -> List[str]:
    lines = []
    if not passive:
        return lines
    raw = passive.get("lines")
    if isinstance(raw, list) and raw:
        for ln in raw:
            t = (ln.get("text") or "").strip()
            ctx = (ln.get("context") or "").strip()
            if not t and not ctx:
                continue
            lines.append(f"{t} — {ctx}" if t and ctx else (t or ctx))
    elif passive.get("effect"):
        lines.append(passive["effect"])
    return [s for s in lines if s]

def primary_stats_block(kit: Dict) -> Dict[str, Dict[str, Optional[int]]]:
    stats = kit.get("stats") or {}
    def pick(group: str, field: str) -> Optional[int]:
        g = stats.get(group) or {}
        return g.get(field)
    return {
        "HP": {"Base": pick("HP", "Base Max"), "55%": pick("HP", "55%"), "100%": pick("HP", "100%")},
        "ATK": {"Base": pick("ATK", "Base Max"), "55%": pick("ATK", "55%"), "100%": pick("ATK", "100%")},
        "DEF": {"Base": pick("DEF", "Base Max"), "55%": pick("DEF", "55%"), "100%": pick("DEF", "100%")},
    }

# --- EZA step parsing ---
STEP_RE = re.compile(r"_eza_step_(\d+)$")
def get_step(v: Dict) -> int:
    if isinstance(v.get("step"), int):
        return v["step"]
    key = v.get("key") or ""
    m = STEP_RE.search(key)
    if m:
        try: return int(m.group(1))
        except: return 0
    return 0

def highest_eza_variant(variants: List[Dict]) -> Optional[Dict]:
    eza_vs = [v for v in variants if v.get("eza") and not v.get("is_super_eza")]
    if not eza_vs: return None
    return sorted(eza_vs, key=get_step)[-1]

def highest_seza_variant(variants: List[Dict]) -> Optional[Dict]:
    seza_vs = [v for v in variants if v.get("eza") and v.get("is_super_eza")]
    if not seza_vs: return None
    return sorted(seza_vs, key=get_step)[-1]

# ---------- Forms / grouping ----------
FORM_ROOT_RE = re.compile(r"^(form_\d+)")

def form_root(key: str) -> Optional[str]:
    if not key or not key.startswith("form_"):
        return None
    root = re.sub(r"_eza_step_\d+$", "", key)
    root = re.sub(r"_base$", "", root)
    m = FORM_ROOT_RE.match(root)
    return m.group(1) if m else root

def compute_variant_kind(variant: Dict) -> str:
    kit = variant.get("kit") or {}
    if safe_get(kit, "reversible_exchange.can_exchange"): return "Exchange"
    if safe_get(kit, "transformation.can_transform"): return "Transformation"
    if _has_giant_form(kit): return "Giant Form"
    if _has_standby_skill(kit): return "Standby"
    return "Form"

def pack_variant_detail(variant: Optional[Dict]) -> Optional[Dict]:
    if not variant:
        return None
    kit = variant.get("kit") or {}
    art = choose_variant_art(variant)
    vname = variant.get("display_name") or safe_get(variant, "kit.display_name") or None
    return {
        "display_name": vname,
        "rarity": variant.get("rarity"),
        "type": variant.get("type"),
        "obtain": variant.get("obtain_type"),
        "release": variant.get("release_date"),
        "images": {"full": art["full"], "character": art["character"], "thumb": art["thumb"]},
        "leader_skill": kit.get("leader_skill"),
        "super_attack": kit.get("super_attack"),
        "ultra_super_attack": kit.get("ultra_super_attack"),
        "passive_skill": kit.get("passive_skill"),
        "active_skill": kit.get("active_skill") if _has_active_skill(kit) else None,
        "standby_skill": kit.get("standby_skill") if _has_standby_skill(kit) else None,
        "links": kit.get("link_skills") or [],
        "categories": kit.get("categories") or [],
        "stats": primary_stats_block(kit),
        "step": get_step(variant),
        "is_super_eza": bool(variant.get("is_super_eza")),
    }

def group_forms(meta: Dict) -> List[Dict]:
    variants: List[Dict] = meta.get("variants") or []
    by_root: Dict[str, Dict] = {}

    base_v = next((v for v in variants if v.get("key") == "base"), None)
    if base_v:
        by_root["base"] = {"root": "base", "regular": base_v, "eza_steps": [], "seza_steps": [], "kind": "Base"}

    for v in variants:
        key = v.get("key") or ""
        if not key: continue
        if key == "base":
            continue

        if key.startswith("eza_step_") and v.get("eza") and not v.get("is_super_eza"):
            g = by_root.setdefault("base", {"root":"base","regular":None,"eza_steps":[], "seza_steps":[], "kind":"Base"})
            g["eza_steps"].append(v); continue
        if key.startswith("eza_step_") and v.get("eza") and v.get("is_super_eza"):
            g = by_root.setdefault("base", {"root":"base","regular":None,"eza_steps":[], "seza_steps":[], "kind":"Base"})
            g["seza_steps"].append(v); continue

        if key.startswith("form_"):
            root = form_root(key)
            if not root: continue
            g = by_root.setdefault(root, {"root":root, "regular":None, "eza_steps":[], "seza_steps":[], "kind":"Form"})
            if "_eza_step_" in key and v.get("eza"):
                (g["seza_steps"] if v.get("is_super_eza") else g["eza_steps"]).append(v)
            else:
                if g["regular"] is None or key.endswith("_base"):
                    g["regular"] = v
                    g["kind"] = compute_variant_kind(v)

    out: List[Dict] = []
    for root, g in by_root.items():
        reg = pack_variant_detail(g.get("regular"))
        eza_steps = [pack_variant_detail(v) for v in sorted(g["eza_steps"], key=get_step)]
        seza_steps = [pack_variant_detail(v) for v in sorted(g["seza_steps"], key=get_step)]
        thumb = None
        for cand in [reg] + (eza_steps[::-1] if eza_steps else []) + (seza_steps[::-1] if seza_steps else []):
            if cand and (cand["images"]["full"] or cand["images"]["character"]):
                thumb = cand["images"]["full"] or cand["images"]["character"]
                break

        if root != "base" and not thumb:
            continue

        out.append({
            "root": root,
            "title": "Base Form" if root == "base" else g.get("kind") or "Form",
            "kind": "Base" if root == "base" else g.get("kind") or "Form",
            "thumb": thumb,
            "regular": reg,
            "eza_steps": eza_steps,
            "seza_steps": seza_steps,
            "has_eza": bool(eza_steps),
            "has_seza": bool(seza_steps),
        })

    out.sort(key=lambda f: (0 if f["root"] == "base" else 1, f["title"]))
    return out

# ---------- Summaries ----------
def to_unit_summary(meta: Dict) -> Dict:
    unit_id = str(meta.get("unit_id") or meta.get("form_id") or "")
    name = meta.get("display_name") or f"Unit {unit_id}"
    variants = meta.get("variants") or []
    base = next((v for v in variants if v.get("key") == "base"), variants[0] if variants else {})
    reg_art = choose_images_for_list(base)

    best_eza = highest_eza_variant(variants)
    best_seza = highest_seza_variant(variants)
    eza_art = choose_images_for_list(best_eza) if best_eza else None
    seza_art = choose_images_for_list(best_seza) if best_seza else None

    chosen = best_variant_for_display(variants)
    chosen_art = choose_images_for_list(chosen)

    kit = (chosen.get("kit") or {})
    leader = (kit.get("leader_skill") or "").strip()
    super1_name = safe_get(kit, "super_attack.name")
    super1_eff  = safe_get(kit, "super_attack.effect")
    ultra_name  = safe_get(kit, "ultra_super_attack.name")
    ultra_eff   = safe_get(kit, "ultra_super_attack.effect")
    passive     = kit.get("passive_skill") or {}
    passive_lines = compact_passive_lines(passive)
    links = kit.get("link_skills") or []
    cats  = kit.get("categories") or []

    rel_dt = parse_dt(chosen.get("release_date"))
    rel_ts = int(rel_dt.timestamp()*1000) if rel_dt else None

    return {
        "unit_id": unit_id,
        "name": name,
        "rarity": chosen.get("rarity") or meta.get("rarity"),
        "type": chosen.get("type") or meta.get("type"),
        "obtain": chosen.get("obtain_type"),
        "release": chosen.get("release_date"),
        "release_ts": rel_ts,
        "source": meta.get("source_base_url"),
        "flags": mechanics_flags(variants),

        "img_auto": chosen_art["grid"],
        "img_regular": reg_art["grid"],
        "img_eza": (eza_art or {}).get("grid"),
        "img_seza": (seza_art or {}).get("grid"),

        "leader": leader,
        "super1": {"name": super1_name, "effect": super1_eff},
        "ultra": {"name": ultra_name, "effect": ultra_eff} if (ultra_name or ultra_eff) else None,
        "passive_lines": passive_lines[:3],

        "links": links[:8],          # summary-limited for UI
        "categories": cats[:12],     # summary-limited for UI
    }

def to_unit_detail(meta: Dict) -> Dict:
    unit_id = str(meta.get("unit_id") or meta.get("form_id") or "")
    name = meta.get("display_name") or f"Unit {unit_id}"
    variants = meta.get("variants") or []
    chosen = best_variant_for_display(variants)
    images = choose_variant_art(chosen)
    flags = mechanics_flags(variants)
    forms = group_forms(meta)

    def last_or_none(lst): return lst[-1] if lst else None
    base_form = next((f for f in forms if f["root"] == "base"), forms[0] if forms else None)
    default_variant = None
    if base_form:
        default_variant = base_form["regular"] or last_or_none(base_form["seza_steps"]) or last_or_none(base_form["eza_steps"])

    detail = {
        "unit_id": unit_id,
        "name": name,
        "rarity": chosen.get("rarity") or meta.get("rarity"),
        "type": chosen.get("type") or meta.get("type"),
        "obtain": chosen.get("obtain_type"),
        "release": chosen.get("release_date"),
        "timezone": chosen.get("timezone"),
        "source": meta.get("source_base_url"),
        "images": images,
        "flags": flags,

        "leader_skill": (default_variant or {}).get("leader_skill"),
        "super_attack": (default_variant or {}).get("super_attack"),
        "ultra_super_attack": (default_variant or {}).get("ultra_super_attack"),
        "passive_skill": (default_variant or {}).get("passive_skill"),
        "active_skill": (default_variant or {}).get("active_skill"),
        "standby_skill": (default_variant or {}).get("standby_skill"),
        "links": (default_variant or {}).get("links") or [],
        "categories": (default_variant or {}).get("categories") or [],
        "stats": (default_variant or {}).get("stats") or {"HP":{},"ATK":{},"DEF":{}},

        "forms": forms,
    }
    return detail

# ---------- Light records for Team Builder / Finder ----------
def to_light_unit(meta: Dict) -> Dict:
    unit_id = str(meta.get("unit_id") or meta.get("form_id") or "")
    name = meta.get("display_name") or f"Unit {unit_id}"
    variants = meta.get("variants") or []
    chosen = best_variant_for_display(variants)
    art = choose_images_for_list(chosen)
    kit = chosen.get("kit") or {}
    cats = kit.get("categories") or []
    links = kit.get("link_skills") or []
    rarity = chosen.get("rarity") or meta.get("rarity") or ""
    r_rank = {"LR":0,"UR":1,"SSR":2}.get((rarity or "").upper(), 3)
    return {
        "id": unit_id,
        "name": name,
        "rarity": rarity,
        "rarity_rank": r_rank,
        "type": chosen.get("type") or meta.get("type") or "",
        "categories": cats,  # full list
        "links": links,      # NEW: include links for synergy
        "leader_skill": kit.get("leader_skill") or "",
        "img": art["grid"] or art["full"] or art["character"] or art["thumb"],
        "source": meta.get("source_base_url"),
    }

# --------------------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------------------
def iter_metadata_files() -> List[Path]:
    if not CARDS_DIR.exists():
        return []
    metas = []
    for folder in sorted(CARDS_DIR.iterdir()):
        if folder.is_dir():
            p = folder / "METADATA.json"
            if p.exists():
                metas.append(p)
    return metas

@lru_cache(maxsize=1)
def load_all_units() -> List[Dict]:
    units = []
    for p in iter_metadata_files():
        try:
            meta = json.loads(p.read_text(encoding="utf-8"))
            units.append(meta)
        except Exception as e:
            print(f"[WARN] failed reading {p}: {e}")
    def sort_key(meta):
        variants = meta.get("variants") or []
        chosen = best_variant_for_display(variants)
        dt = parse_dt(chosen.get("release_date"))
        return dt or datetime.fromtimestamp(0)
    units.sort(key=sort_key, reverse=True)
    return units

def clear_cache_if_requested():
    if request.args.get("reload") == "1":
        load_all_units.cache_clear()

# --------------------------------------------------------------------------------------
# Assets
# --------------------------------------------------------------------------------------
def secure_path_under(root: Path, rel: str) -> Optional[Path]:
    rel = norm_rel(rel)
    target = root.joinpath(*rel.split("/")).resolve()
    try:
        root_resolved = root.resolve()
    except Exception:
        root_resolved = root
    if not str(target).startswith(str(root_resolved)):
        return None
    if not target.exists():
        return None
    return target

@app.route("/assets/<path:relpath>")
def serve_asset(relpath: str):
    full = secure_path_under(ASSETS_ROOT, relpath)
    if not full:
        abort(404)
    # Add light caching to assets
    resp = send_file(str(full))
    resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return resp

@app.route("/__exists")
def exists_probe():
    rel = request.args.get("rel", "")
    full = secure_path_under(ASSETS_ROOT, rel)
    ok = bool(full)
    print(f"[EXISTS] rel='{rel}' -> '{full}' exists={ok}")
    return jsonify({"exists": ok})

# --------------------------------------------------------------------------------------
# Helpers for Home facets
# --------------------------------------------------------------------------------------
def compute_facets(cards: List[Dict]) -> Tuple[List[Tuple[str,int]], List[Tuple[str,int]]]:
    from collections import Counter
    c_counter, l_counter = Counter(), Counter()
    for c in cards:
        for cat in (c.get("categories") or []):
            c_counter[cat] += 1
        for ln in (c.get("links") or []):
            l_counter[ln] += 1
    top_cats = c_counter.most_common(24)
    top_links = l_counter.most_common(18)
    return top_cats, top_links

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@app.route("/")
def home():
    clear_cache_if_requested()
    metas = load_all_units()
    cards = [to_unit_summary(m) for m in metas]
    top_cats, top_links = compute_facets(cards)
    return render_template_string(INDEX_HTML, cards=cards, total=len(cards), top_cats=top_cats, top_links=top_links)

@app.route("/unit/<unit_id>")
def unit_detail(unit_id: str):
    clear_cache_if_requested()
    metas = load_all_units()
    meta = next((m for m in metas if str(m.get("unit_id")) == str(unit_id)), None)
    if not meta:
        abort(404)
    detail = to_unit_detail(meta)
    return render_template_string(DETAIL_HTML, u=detail)

@app.route("/team")
def team_builder():
    clear_cache_if_requested()
    metas = load_all_units()
    light = [to_light_unit(m) for m in metas]
    return render_template_string(TEAM_HTML, units=light)

@app.route("/finder")
def leader_finder():
    clear_cache_if_requested()
    metas = load_all_units()
    light = [to_light_unit(m) for m in metas]
    return render_template_string(FINDER_HTML, units=light)

# -------- JSON APIs (handy for tooling) --------
@app.route("/api/units")
def api_units():
    metas = load_all_units()
    rows = [to_light_unit(m) for m in metas]
    return jsonify(rows)

@app.route("/api/unit/<unit_id>")
def api_unit(unit_id: str):
    metas = load_all_units()
    meta = next((m for m in metas if str(m.get("unit_id")) == str(unit_id)), None)
    if not meta: abort(404)
    return jsonify(to_unit_detail(meta))

# --------------------------------------------------------------------------------------
# Templates
# --------------------------------------------------------------------------------------
INDEX_HTML = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<title>Dokkan Unit Browser</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root{
  --bg:#0b0f14; --card:#101720; --ink:#e9f2ff; --muted:#9fb0c7; --accent:#5bd1ff; --accent-2:#9bffdd;
  --chip:#182330; --chip-border:#223144; --cardw: 360px;
  --agl:#3b82f6; --teq:#10b981; --int:#8b5cf6; --str:#ef4444; --phy:#f59e0b;
}
:root[data-theme="light"]{
  --bg:#f7faff; --card:#ffffff; --ink:#0b1623; --muted:#3e526d; --accent:#0d7bd0; --accent-2:#0ab39b;
  --chip:#eef3fb; --chip-border:#d9e6f8;
}
*{box-sizing:border-box}
body{
  margin:0; background:
    radial-gradient(1200px 800px at 15% -10%, rgba(91,209,255,.10), transparent),
    radial-gradient(1000px 600px at 100% 0, rgba(155,255,221,.08), transparent),
    linear-gradient(180deg, #09101a, var(--bg));
  color:var(--ink); font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial;
}
a{color:var(--accent); text-decoration:none}
.wrap{max-width:1400px; margin:0 auto; padding:24px 16px 64px}

/* header */
.header{display:flex; gap:12px; align-items:center; margin-bottom:12px; flex-wrap:wrap}
.header h1{font-size:28px; margin:0; letter-spacing:.2px}
.count{opacity:.85; font-size:14px}
.searchbar{position:relative; flex:1; min-width:260px}
.searchbar input{
  width:100%; padding:12px 40px 12px 12px;
  border:1px solid #233246; border-radius:12px; background:#0f151d; color:#e9f2ff;
  outline:none; transition: box-shadow .15s, border-color .15s;
}
:root[data-theme="light"] .searchbar input{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.searchbar input:focus{ box-shadow:0 0 0 3px rgba(91,209,255,.25); border-color:#345375 }
.searchbar .k{position:absolute; right:8px; top:50%; transform:translateY(-50%); opacity:.7; font-size:12px; background:#0b1118; border:1px solid #1f2a3a; padding:2px 6px; border-radius:6px}
:root[data-theme="light"] .searchbar .k{ background:#eef3fb; border-color:#d9e6f8; color:#3e526d }

/* controls */
.controls{display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin:12px 0 18px}
@media (max-width: 1000px){ .controls{ grid-template-columns:1fr } }
.panel{background:#0f151d; border:1px solid #1e2a3a; border-radius:12px; padding:10px}
:root[data-theme="light"] .panel{ background:#fff; border-color:#dbe7ff }
.row{display:flex; gap:10px; align-items:center; flex-wrap:wrap}
select{background:#0f151d; color:#e9f2ff; border:1px solid #233246; border-radius:10px; padding:8px 10px; cursor:pointer}
:root[data-theme="light"] select{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.label{font-size:12px; opacity:.85}
.chips{display:flex; gap:6px; flex-wrap:wrap}
.chip{background:#0b1118; border:1px solid #1f2b3b; color:#d2e4ff; font-size:12px; padding:5px 9px; border-radius:999px; cursor:pointer}
:root[data-theme="light"] .chip{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.chip.active{border-color:#5bd1ff; color:#ffffff}
:root[data-theme="light"] .chip.active{ color:#0b1623; box-shadow:0 0 0 2px rgba(13,123,208,.15) inset }

.btn{font-size:12px; padding:8px 12px; background:#0f151d; border:1px solid #1e2a3a; border-radius:10px; color:#fff; cursor:pointer}
.btn:hover{ border-color:#38506f }
:root[data-theme="light"] .btn{ background:#fff; color:#0b1623; border-color:#c7d5ea }

/* grid cards */
.grid{display:grid; grid-template-columns: repeat(auto-fill, minmax(var(--cardw), 1fr)); gap:14px}
.card{
  position:relative; border:1px solid #1b2636; border-radius:16px; background:linear-gradient(180deg, #0f151d, #0b1016);
  overflow:hidden; transition: transform .14s ease, box-shadow .14s ease, border-color .14s;
}
:root[data-theme="light"] .card{ background:#fff; border-color:#dbe7ff }
.card:hover{ transform: translateY(-3px); border-color:#2b3d57; box-shadow:0 12px 30px rgba(0,0,0,.25) }
.thumb-wrap{ background:linear-gradient(180deg, rgba(91,209,255,.07), rgba(155,255,221,.06)); position:relative }
.thumb-wrap a{display:block}
.thumb-wrap img{ width:100%; height:auto; display:block; filter:drop-shadow(0 12px 22px rgba(0,0,0,.55)) }
.type-ring{ position:absolute; inset:auto 8px 8px auto; width:14px; height:14px; border-radius:50%; border:2px solid currentColor; opacity:.9 }
.type-agl{ color:var(--agl) } .type-teq{ color:var(--teq) } .type-int{ color:var(--int) } .type-str{ color:var(--str) } .type-phy{ color:var(--phy) }

/* favorite star */
.fav{ position:absolute; top:8px; right:8px; z-index:2; width:30px; height:30px; border-radius:50%; border:1px solid #2a3b54; background:#0c121a; display:flex;align-items:center;justify-content:center; cursor:pointer; transition:.15s }
:root[data-theme="light"] .fav{ background:#fff; border-color:#dbe7ff }
.fav:hover{ border-color:#5bd1ff }
.fav svg{ width:16px; height:16px; fill:#a9c0df }
.fav.active svg{ fill:#ffd166 }

.card .content{ padding:12px 12px 14px }
.title{font-size:16px; font-weight:700; margin:0 0 6px; letter-spacing:.2px}
.meta{display:flex; gap:8px; flex-wrap:wrap; margin:0 0 8px}
.meta .pill{font-size:11px; background:#0b1118; padding:4px 8px; border:1px solid #1c2838; border-radius:999px; color:#cfe2ff}
:root[data-theme="light"] .meta .pill{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.leader, .super{font-size:12px; color:var(--muted)}
.leader strong, .super strong{color:#cfe2ff}
.flags{display:flex; gap:6px; flex-wrap:wrap; margin-top:8px}
.badge{display:inline-flex; align-items:center; gap:6px; font-size:11px; padding:3px 8px; background:#0e151d; border:1px solid #233246; border-radius:999px; color:#cadeff}
.badge.seza{border-color:#6aa9ff; color:#e2efff}
.badge.eza{border-color:#2f5f8a; color:#bfe0ff}
.badge.tr{border-color:#476a3c; color:#d9ffd1}
.badge.ex{border-color:#6b5a2d; color:#ffe7ac}
.badge.st{border-color:#6a446e; color:#ffccff}
.badge.ac{border-color:#3a5f76; color:#cdeaff}
.badge.gi{border-color:#6b4f2f; color:#ffe5bd}
.badge.rv{border-color:#355a2e; color:#cdffcc}

.foot{display:flex; gap:8px; margin-top:10px; flex-wrap:wrap}
.button{font-size:12px; padding:8px 12px; background:#0f151d; border:1px solid #1e2a3a; border-radius:10px; color:#fff}
.button:hover{ border-color:#38506f }
.tbar{display:flex; gap:8px; margin-top:8px; flex-wrap:wrap}
.tbar .button{background:#0d1724}
:root[data-theme="light"] .tbar .button{ background:#fff; border-color:#c7d5ea; color:#0b1623 }
.help{font-size:12px; opacity:.75}

/* facet panels */
.facet-title{font-size:12px; opacity:.85; margin-right:6px}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>Dokkan Unit Browser</h1>
    <div class="count" id="count">{{ total }} units</div>
    <div class="searchbar">
      <input id="q" placeholder="Search name, ID, type (AGL/TEQ/INT/STR/PHY), rarity (SSR/UR/LR), category, link…  (Ctrl/Cmd + K)" autocomplete="off" />
      <div class="k">K</div>
    </div>
    <a class="btn" href="/team">🧩 Team Builder</a>
    <a class="btn" href="/finder">👑 Leader Finder</a>
    <button class="btn" id="randomBtn">🎲 Random</button>
    <button class="btn" id="themeBtn" title="Toggle theme">🌓 Theme</button>
  </div>

  <div class="controls">
    <div class="panel">
      <div class="row">
        <label>Sort
          <select id="sort">
            <option value="newest">Newest</option>
            <option value="name">Name (A→Z)</option>
            <option value="rarity">Rarity (LR→UR→SSR)</option>
            <option value="type">Type</option>
          </select>
        </label>
        <label>Type
          <select id="f-type">
            <option value="">All</option>
            <option>AGL</option><option>TEQ</option><option>INT</option><option>STR</option><option>PHY</option>
          </select>
        </label>
        <label>Rarity
          <select id="f-rarity">
            <option value="">All</option>
            <option>SSR</option><option>UR</option><option>LR</option>
          </select>
        </label>
        <label>Art
          <select id="art">
            <option value="auto">Auto</option>
            <option value="regular">Regular</option>
            <option value="eza">EZA</option>
            <option value="seza">S-EZA</option>
          </select>
        </label>
        <label class="help">Tip: Click any art to open details</label>
      </div>
    </div>
    <div class="panel">
      <div class="row">
        <span class="label">Mechanics:</span>
        <div class="chips" id="mech">
          <span class="chip" data-v="EZA">EZA</span>
          <span class="chip" data-v="SEZA">SEZA</span>
          <span class="chip" data-v="Transforms">Transforms</span>
          <span class="chip" data-v="Exchange">Exchange</span>
          <span class="chip" data-v="Standby">Standby</span>
          <span class="chip" data-v="Active">Active</span>
          <span class="chip" data-v="Giant Form">Giant Form</span>
          <span class="chip" data-v="Revival">Revival</span>
        </div>
      </div>
      <div class="row" style="margin-top:8px">
        <span class="facet-title">Categories:</span>
        <div class="chips" id="facetCats">
          {% for name, cnt in top_cats %}
            <span class="chip" data-cat="{{ name|e }}" title="{{ cnt }} units">{{ name }}</span>
          {% endfor %}
        </div>
      </div>
      <div class="row" style="margin-top:8px">
        <span class="facet-title">Links:</span>
        <div class="chips" id="facetLinks">
          {% for name, cnt in top_links %}
            <span class="chip" data-link="{{ name|e }}" title="{{ cnt }} units">{{ name }}</span>
          {% endfor %}
        </div>
      </div>
      <div class="row" style="margin-top:8px">
        <button class="btn" id="favFilter">★ Favorites</button>
        <button class="btn" id="clearFilters">Reset Filters</button>
      </div>
    </div>
  </div>

  <div class="grid" id="grid">
    {% for c in cards %}
    <article class="card"
      data-name="{{ c.name|e }}" data-id="{{ c.unit_id|e }}" data-type="{{ c.type or '' }}" data-rarity="{{ c.rarity or '' }}"
      data-cats="{{ (c.categories or [])|join(' ')|lower() }}" data-links="{{ (c.links or [])|join(' ')|lower() }}"
      data-mech="{{ (c.flags or [])|join(' ') }}" data-release="{{ c.release_ts or 0 }}">
      <div class="thumb-wrap">
        <button class="fav" title="Toggle favorite" data-id="{{ c.unit_id }}"><svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg></button>
        <a href="/unit/{{ c.unit_id }}" aria-label="Open details">
          {% set src_auto = c.img_auto or c.img_regular or c.img_eza or c.img_seza %}
          <img loading="lazy"
               src="{{ src_auto or '/assets/dokkaninfo.com/images/dokkan-info-logo.png' }}"
               data-src-auto="{{ c.img_auto or '' }}"
               data-src-regular="{{ c.img_regular or '' }}"
               data-src-eza="{{ c.img_eza or '' }}"
               data-src-seza="{{ c.img_seza or '' }}"
               alt="{{ c.name|e }}"
               onerror="this.onerror=null; this.src='/assets/dokkaninfo.com/images/dokkan-info-logo.png'">
        </a>
        <span class="type-ring type-{{ (c.type or '').lower() }}"></span>
      </div>
      <div class="content">
        <h3 class="title"><a href="/unit/{{ c.unit_id }}">{{ c.name }}</a></h3>
        <div class="meta">
          {% if c.rarity %}<span class="pill">{{ c.rarity }}</span>{% endif %}
          {% if c.type %}<span class="pill">{{ c.type }}</span>{% endif %}
          {% if c.obtain %}<span class="pill">{{ c.obtain }}</span>{% endif %}
        </div>
        {% if c.leader %}
          <div class="leader"><strong>Leader:</strong> {{ c.leader }}</div>
        {% endif %}
        {% if c.super1.name or c.super1.effect %}
          <div class="super"><strong>Super:</strong>
            {% if c.super1.name %}{{ c.super1.name }} — {% endif %}{{ c.super1.effect }}
          </div>
        {% endif %}
        {% if c.ultra %}
          <div class="super"><strong>Ultra:</strong>
            {% if c.ultra.name %}{{ c.ultra.name }} — {% endif %}{{ c.ultra.effect }}
          </div>
        {% endif %}
        {% if c.passive_lines %}
          <div class="super"><strong>Passive:</strong> {{ c.passive_lines|join(' • ') }}</div>
        {% endif %}
        <div class="flags">
          {% for f in c.flags %}
            <span class="badge {{ 'seza' if f=='SEZA' else 'eza' if f=='EZA' else 'tr' if f=='Transforms' else 'ex' if f=='Exchange' else 'st' if f=='Standby' else 'ac' if f=='Active' else 'gi' if f=='Giant Form' else 'rv' if f=='Revival' else '' }}">{{ f }}</span>
          {% endfor %}
        </div>
        <div class="tbar">
          <a class="button" href="/unit/{{ c.unit_id }}">Details</a>
          {% if c.source %}<a class="button" href="{{ c.source }}" target="_blank" rel="noopener">DokkanInfo ↗</a>{% endif %}
        </div>
      </div>
    </article>
    {% endfor %}
  </div>
</div>

<script>
const $ = (s,root=document)=>root.querySelector(s);
const $$=(s,root=document)=>Array.from(root.querySelectorAll(s));

// theme
(function(){
  const root = document.documentElement;
  const saved = localStorage.getItem("dokkan.theme") || "dark";
  root.setAttribute("data-theme", saved);
  $("#themeBtn").addEventListener("click", ()=>{
    const cur = root.getAttribute("data-theme")==="dark" ? "light" : "dark";
    root.setAttribute("data-theme", cur);
    localStorage.setItem("dokkan.theme", cur);
  });
})();

// favorites
const FKEY = "dokkan.favs";
function getFavs(){ try{ return JSON.parse(localStorage.getItem(FKEY)||"[]"); }catch{ return []; } }
function setFavs(arr){ localStorage.setItem(FKEY, JSON.stringify(Array.from(new Set(arr)))); }
function isFav(id){ return getFavs().includes(id); }
function renderFavButtons(){
  $$(".fav").forEach(b=>{
    const id = b.dataset.id;
    if(isFav(id)) b.classList.add("active"); else b.classList.remove("active");
  });
}
document.addEventListener("click", (e)=>{
  const b = e.target.closest(".fav"); if(!b) return;
  const id = b.dataset.id;
  let arr = getFavs();
  if(arr.includes(id)) arr = arr.filter(x=>x!==id); else arr.push(id);
  setFavs(arr); renderFavButtons(); applyFilters();
});
renderFavButtons();

const grid = $("#grid");
const count = $("#count");
const q = $("#q");
const sortSel = $("#sort");
const fType = $("#f-type");
const fRarity = $("#f-rarity");
const mech = $("#mech");
const favFilter = $("#favFilter");
const clearFilters = $("#clearFilters");
const artSel = $("#art");
const facetCats = $("#facetCats");
const facetLinks = $("#facetLinks");

// keyboard focus
document.addEventListener('keydown', (e)=>{
  if((e.ctrlKey || e.metaKey) && e.key.toLowerCase()==='k'){ e.preventDefault(); q.focus(); q.select(); }
});

// art mode (persisted; now live-swaps thumbnails)
(function(){
  const saved = localStorage.getItem("dokkan.artmode") || "auto";
  artSel.value = saved;
  function applyArt(){
    const mode = artSel.value;
    $$(".card .thumb-wrap img").forEach(img=>{
      const fallback = img.dataset.srcAuto || img.dataset.srcRegular || img.dataset.srcEza || img.dataset.srcSeza || '/assets/dokkaninfo.com/images/dokkan-info-logo.png';
      let src = '';
      if(mode==="auto"){ src = img.dataset.srcAuto || fallback; }
      else if(mode==="regular"){ src = img.dataset.srcRegular || fallback; }
      else if(mode==="eza"){ src = img.dataset.srcEza || fallback; }
      else if(mode==="seza"){ src = img.dataset.srcSeza || fallback; }
      if(img.src !== src && src) img.src = src;
    });
  }
  applyArt();
  artSel.addEventListener("change", ()=>{
    localStorage.setItem("dokkan.artmode", artSel.value);
    applyArt();
  });
})();

let mechActive = new Set();
mech.addEventListener("click", (e)=>{
  const chip = e.target.closest(".chip"); if(!chip) return;
  const v = chip.dataset.v;
  if(mechActive.has(v)){ mechActive.delete(v); chip.classList.remove("active"); }
  else{ mechActive.add(v); chip.classList.add("active"); }
  applyFilters();
});
favFilter.addEventListener("click", ()=>{
  favFilter.classList.toggle("active");
  applyFilters();
});

// facet filters (OR within facet; AND across facets)
let catActive = new Set();
let linkActive = new Set();
facetCats.addEventListener("click", (e)=>{
  const chip = e.target.closest(".chip"); if(!chip) return;
  const name = (chip.dataset.cat||"").toLowerCase();
  if(catActive.has(name)){ catActive.delete(name); chip.classList.remove("active"); }
  else{ catActive.add(name); chip.classList.add("active"); }
  applyFilters();
});
facetLinks.addEventListener("click", (e)=>{
  const chip = e.target.closest(".chip"); if(!chip) return;
  const name = (chip.dataset.link||"").toLowerCase();
  if(linkActive.has(name)){ linkActive.delete(name); chip.classList.remove("active"); }
  else{ linkActive.add(name); chip.classList.add("active"); }
  applyFilters();
});

clearFilters.addEventListener("click", ()=>{
  q.value=""; fType.value=""; fRarity.value=""; mechActive.clear();
  $$(".chip", mech).forEach(c=>c.classList.remove("active"));
  favFilter.classList.remove("active");
  sortSel.value="newest";
  catActive.clear(); linkActive.clear();
  $$(".chip", facetCats).forEach(c=>c.classList.remove("active"));
  $$(".chip", facetLinks).forEach(c=>c.classList.remove("active"));
  applyFilters(); sortCards("newest");
});

function normalize(s){ return (s||"").toLowerCase().trim(); }

function applyFilters(){
  const txt = normalize(q.value);
  const tFilter = fType.value;
  const rFilter = fRarity.value;
  const favOnly = favFilter.classList.contains("active");
  let cards = $$(".card", grid);
  let visible = 0;

  for(const el of cards){
    const id   = (el.dataset.id||"");
    const name = normalize(el.dataset.name);
    const type = (el.dataset.type||"").toUpperCase();
    const rarity=(el.dataset.rarity||"").toUpperCase();
    const cats = normalize(el.dataset.cats);
    const links= normalize(el.dataset.links);
    const mech = el.dataset.mech || "";

    let ok = true;

    if(txt){
      ok = name.includes(txt) || id.includes(txt) || type.toLowerCase().includes(txt)
           || rarity.toLowerCase().includes(txt) || cats.includes(txt) || links.includes(txt);
    }
    if(ok && tFilter){ ok = type === tFilter.toUpperCase(); }
    if(ok && rFilter){ ok = rarity === rFilter.toUpperCase(); }
    if(ok && mechActive.size){
      for(const need of mechActive){ if(!mech.includes(need)){ ok=false; break; } }
    }
    if(ok && catActive.size){
      // OR within categories: must match at least one selected cat
      let hit = false;
      for(const c of catActive){ if(cats.includes(c)){ hit = true; break; } }
      ok = hit;
    }
    if(ok && linkActive.size){
      let hit = false;
      for(const l of linkActive){ if(links.includes(l)){ hit = true; break; } }
      ok = hit;
    }
    if(ok && favOnly){ ok = isFav(id); }

    el.style.display = ok ? "" : "none";
    if(ok) visible++;
  }
  count.textContent = visible + " units";
}
function sortCards(mode){
  const cards = $$(".card", grid);
  const keyers = {
    "name": el => (el.dataset.name||"").toLowerCase(),
    "rarity": el => ({SSR:2,UR:1,LR:0}[(el.dataset.rarity||"").toUpperCase()] ?? 9),
    "type": el => (el.dataset.type||""),
    "newest": el => parseInt(el.dataset.release||"0",10),
  };
  const key = keyers[mode] || keyers["newest"];
  cards.sort((a,b)=> key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0);
  if(mode==="newest" || mode==="rarity"){ cards.reverse(); }
  for(const el of cards){ grid.appendChild(el); }
}

[q, sortSel, fType, fRarity].forEach(el=>{
  el.addEventListener('input', ()=>{
    if(el===sortSel){ sortCards(sortSel.value); }
    else{ applyFilters(); }
  });
});

$("#randomBtn").addEventListener("click", ()=>{
  const cards = $$(".card").filter(c=>c.style.display!=="none");
  if(!cards.length) return;
  const pick = cards[Math.floor(Math.random()*cards.length)];
  window.location.href = "/unit/" + pick.dataset.id;
});

// initial
sortCards("newest");
applyFilters();
renderFavButtons();
</script>
</body>
</html>
"""

DETAIL_HTML = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<title>{{ u.name }} — Dokkan Unit</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root{
  --bg:#0b0f14; --card:#0f151d; --ink:#e9f2ff; --muted:#9fb0c7; --accent:#5bd1ff; --chip:#182330; --chipb:#223144;
}
:root[data-theme="light"]{
  --bg:#f7faff; --card:#ffffff; --ink:#0b1623; --muted:#3e526d; --accent:#0d7bd0; --chip:#eef3fb; --chipb:#d9e6f8;
}
*{box-sizing:border-box}
body{margin:0; background:
      radial-gradient(1200px 800px at 15% -10%, rgba(91,209,255,.10), transparent),
      radial-gradient(1000px 600px at 100% 0, rgba(155,255,221,.08), transparent),
      var(--bg); color:var(--ink); font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial;}
a{color:var(--accent); text-decoration:none}
.wrap{max-width:1200px; margin:0 auto; padding:24px 16px 64px}
.top{display:grid; grid-template-columns: 360px 1fr; gap:18px; align-items:start}
@media (max-width: 980px){ .top{ grid-template-columns: 1fr } }

.card{background:linear-gradient(180deg, var(--card), #0b1016); border:1px solid #1b2636; border-radius:16px; overflow:hidden}
:root[data-theme="light"] .card{ background:#fff; border-color:#dbe7ff }
.card .pad{padding:14px}

.hero{position:relative; background:linear-gradient(135deg, rgba(91,209,255,.10), rgba(155,255,221,.05) 40%, transparent);
      border-radius:16px; border:1px solid #1b2636; padding:10px; }
:root[data-theme="light"] .hero{ border-color:#dbe7ff; background:#fff }
.hero .imgbox{background:#0b1118; border:1px solid #172234; border-radius:12px; display:flex; align-items:center; justify-content:center}
:root[data-theme="light"] .hero .imgbox{ background:#eef3fb; border-color:#d9e6f8 }
.hero .imgbox img{width:100%; height:auto; object-fit:contain; display:block; filter:drop-shadow(0 18px 26px rgba(0,0,0,.55))}
.hmeta{margin-top:10px; display:flex; gap:8px; flex-wrap:wrap}
.hmeta .pill{font-size:12px; background:#0b1118; padding:5px 9px; border:1px solid #1c2838; border-radius:999px; color:#b5c9e6}
:root[data-theme="light"] .hmeta .pill{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }

.hdr{display:flex; justify-content:space-between; align-items:flex-start; gap:12px}
.hdr h1{font-size:22px; margin:0}
.flags{display:flex; gap:6px; flex-wrap:wrap}
.badge{display:inline-flex; align-items:center; gap:6px; font-size:12px; padding:3px 8px; background:var(--chip); border:1px solid var(--chipb); border-radius:999px}
.badge.eza{border-color:#2f5f8a; color:#8fd0ff}
.badge.tr{border-color:#476a3c; color:#b4ff9b}
.badge.ex{border-color:#6b5a2d; color:#ffe59c}
.badge.st{border-color:#6a446e; color:#ffb2ff}
.badge.ac{border-color:#3a5f76; color:#a4e7ff}
.badge.gi{border-color:#6b4f2f; color:#ffd6a4}
.badge.rv{border-color:#355a2e; color:#cdffcc}

.section{margin-top:14px}
.section h3{margin:0 0 8px; font-size:16px}
.kv{display:flex; gap:8px; flex-wrap:wrap}
.kv .kvitem{font-size:12px; color:#b1c5df; background:#0e151d; border:1px solid #19263a; padding:4px 8px; border-radius:8px}
:root[data-theme="light"] .kv .kvitem{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.box{background:#0f151d; border:1px solid #1b2636; border-radius:12px; padding:12px}
:root[data-theme="light"] .box{ background:#fff; border-color:#dbe7ff }

.grid2{display:grid; grid-template-columns: 1fr 1fr; gap:14px}
@media (max-width: 980px){ .grid2{ grid-template-columns: 1fr } }
.stattbl{width:100%; border-collapse:collapse; overflow:hidden; border-radius:10px}
.stattbl th, .stattbl td{border:1px solid #1b2636; padding:8px; text-align:left}
:root[data-theme="light"] .stattbl th, :root[data-theme="light"] .stattbl td{ border-color:#dbe7ff }
.statlbl{opacity:.8}

.skill{margin:8px 0}
.skill strong{color:#cfe2ff}
:root[data-theme="light"] .skill strong{ color:#0b1623 }
.note{opacity:.8; font-size:12px}

.chips{display:flex; gap:6px; flex-wrap:wrap}
.chip{font-size:12px; background:#0b1118; padding:5px 9px; border:1px solid #1c2838; border-radius:999px; color:#b5c9e6}
:root[data-theme="light"] .chip{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }

.actions{display:flex; gap:8px; margin-top:10px; flex-wrap:wrap}
.button{font-size:12px; padding:8px 12px; background:#0f151d; border:1px solid #1e2a3a; border-radius:8px; color:#fff}
:root[data-theme="light"] .button{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.button:hover{border-color:#38506f}

/* Forms rail / tabs / steps */
.forms{display:flex; gap:8px; overflow:auto; padding-bottom:4px; margin:10px 0}
.formbtn{display:flex; gap:8px; align-items:center; border:1px solid #1b2636; background:#0f151d; color:#e9f2ff; padding:6px; border-radius:10px; cursor:pointer; white-space:nowrap}
:root[data-theme="light"] .formbtn{ background:#fff; color:#0b1623; border-color:#dbe7ff }
.formbtn img{width:48px; height:auto; border-radius:8px; background:#0b1118}
:root[data-theme="light"] .formbtn img{ background:#eef3fb }
.formbtn.active{ outline:2px solid rgba(91,209,255,.4) }

.tabs{display:flex; gap:8px; margin:8px 0}
.tab{padding:6px 10px; border:1px solid #1b2636; background:#0f151d; color:#e9f2ff; border-radius:999px; cursor:pointer}
:root[data-theme="light"] .tab{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.tab.active{ outline:2px solid rgba(91,209,255,.4) }

.steps{display:flex; gap:6px; flex-wrap:wrap; margin:6px 0}
.step{padding:4px 8px; border:1px solid #1b2636; background:#0f151d; color:#e9f2ff; border-radius:6px; cursor:pointer}
:root[data-theme="light"] .step{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.step.active{ outline:2px solid rgba(91,209,255,.4) }
</style>
</head>
<body>
  <div class="wrap">
    <div class="hdr">
      <h1 id="title">{{ u.name }}</h1>
      <div class="flags">
        {% for f in u.flags %}
          <span class="badge {{ 'eza' if f=='EZA' else 'tr' if f=='Transforms' else 'ex' if f=='Exchange' else 'st' if f=='Standby' else 'ac' if f=='Active' else 'gi' if f=='Giant Form' else 'rv' if f=='Revival' else '' }}">{{ f }}</span>
        {% endfor %}
      </div>
    </div>

    <div class="top">
      <div class="hero">
        <div class="imgbox">
          <img id="heroImg" src="{{ u.images.full or u.images.character or '/assets/dokkaninfo.com/images/dokkan-info-logo.png' }}" alt="{{ u.name }}"
               onerror="this.onerror=null; this.src='/assets/dokkaninfo.com/images/dokkan-info-logo.png'">
        </div>
        <div class="hmeta" id="metaPills">
          {% if u.rarity %}<span class="pill" id="rarityP">{{ u.rarity }}</span>{% endif %}
          {% if u.type %}<span class="pill" id="typeP">{{ u.type }}</span>{% endif %}
          {% if u.obtain %}<span class="pill" id="obtainP">{{ u.obtain }}</span>{% endif %}
          <span class="pill">ID: {{ u.unit_id }}</span>
          {% if u.release %}<span class="pill" id="relP">Release: {{ u.release }}{% if u.timezone %} {{ u.timezone }}{% endif %}</span>{% endif %}
        </div>
        <div class="actions">
          {% if u.source %}<a class="button" href="{{ u.source }}" target="_blank" rel="noopener">Open on DokkanInfo ↗</a>{% endif %}
          <a class="button" href="/">← Back to all units</a>
          <button class="button" id="themeBtn">🌓 Theme</button>
        </div>
      </div>

      <div class="card">
        <div class="pad">
          <!-- Forms rail -->
          <div class="forms" id="formsRail"></div>

          <!-- Tabs and steps -->
          <div class="tabs" id="tabs"></div>
          <div class="steps" id="steps"></div>

          <div class="grid2">
            <div>
              <div class="section">
                <h3>Leader Skill</h3>
                <div class="box"><span id="leaderSkill">{{ u.leader_skill or "—" }}</span></div>
              </div>
              <div class="section">
                <h3>Super Attack</h3>
                <div class="box">
                  <div class="skill"><strong id="superName">{{ u.super_attack.name if u.super_attack else "—" }}</strong></div>
                  <div class="skill" id="superEff">{{ u.super_attack.effect if u.super_attack else "" }}</div>
                </div>
              </div>
              <div class="section" id="ultraBlock" style="display:none">
                <h3>Ultra / 18-Ki</h3>
                <div class="box">
                  <div class="skill"><strong id="ultraName">—</strong></div>
                  <div class="skill" id="ultraEff"></div>
                </div>
              </div>
              <div class="section" id="activeBlock" style="display:none">
                <h3>Active Skill</h3>
                <div class="box">
                  <div class="skill"><strong id="activeName">—</strong></div>
                  <div class="skill" id="activeEff"></div>
                  <div class="note" id="activeCond"></div>
                </div>
              </div>
              <div class="section" id="standbyBlock" style="display:none">
                <h3>Standby Skill</h3>
                <div class="box">
                  <div class="skill"><strong id="standbyName">—</strong></div>
                  <div class="skill" id="standbyEff"></div>
                </div>
              </div>
            </div>
            <div>
              <div class="section">
                <h3>Passive</h3>
                <div class="box" id="passiveBox">—</div>
              </div>
              <div class="section">
                <h3>Stats</h3>
                <table class="stattbl">
                  <thead>
                    <tr><th></th><th>Base</th><th>55%</th><th>100%</th></tr>
                  </thead>
                  <tbody>
                    <tr><td class="statlbl">HP</td><td id="hpB">—</td><td id="hp55">—</td><td id="hp100">—</td></tr>
                    <tr><td class="statlbl">ATK</td><td id="atkB">—</td><td id="atk55">—</td><td id="atk100">—</td></tr>
                    <tr><td class="statlbl">DEF</td><td id="defB">—</td><td id="def55">—</td><td id="def100">—</td></tr>
                  </tbody>
                </table>
              </div>
              <div class="section">
                <h3>Link Skills</h3>
                <div class="chips" id="linksBox"></div>
              </div>
              <div class="section">
                <h3>Categories</h3>
                <div class="chips" id="catsBox"></div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>

  </div>

<script>
const DATA = {{ u|tojson }};
const $ = (s,root=document)=>root.querySelector(s);
const $$=(s,root=document)=>Array.from(root.querySelectorAll(s));

(function theme(){
  const root = document.documentElement;
  const saved = localStorage.getItem("dokkan.theme") || "dark";
  root.setAttribute("data-theme", saved);
  $("#themeBtn").addEventListener("click", ()=>{
    const cur = root.getAttribute("data-theme")==="dark" ? "light" : "dark";
    root.setAttribute("data-theme", cur);
    localStorage.setItem("dokkan.theme", cur);
  });
})();

function textOr(lines, effect){
  if(Array.isArray(lines) && lines.length){
    const li = lines.map(ln=>{
      const t = (ln.text||"").trim(); const c = (ln.context||"").trim();
      if(t && c) return `<li>${t} — <span class="note">${c}</span></li>`;
      if(t) return `<li>${t}</li>`;
      if(c) return `<li><span class="note">${c}</span></li>`;
      return "";
    }).filter(Boolean).join("");
    return `<ul>${li}</ul>`;
  }
  return effect || "—";
}

const formsRail = $("#formsRail");
const tabs = $("#tabs");
const steps = $("#steps");

// deep-link state
function getParams(){
  const p = new URLSearchParams(location.search);
  return {
    form: p.get("form"),
    mode: p.get("mode"),
    step: p.get("step") ? parseInt(p.get("step"),10) : null
  };
}
function setParams({form, mode, step}){
  const p = new URLSearchParams(location.search);
  if(form) p.set("form", form); else p.delete("form");
  if(mode) p.set("mode", mode); else p.delete("mode");
  if(step!=null) p.set("step", String(step)); else p.delete("step");
  history.replaceState(null, "", "?"+p.toString());
}

let curForm = null;   // a form object from DATA.forms
let curMode = "regular"; // "regular" | "eza" | "seza"
let curStep = null;   // integer or null
let curVar = null;    // current variant detail

function buildUI(){
  // forms rail
  formsRail.innerHTML = "";
  DATA.forms.forEach((f, idx)=>{
    const btn = document.createElement("button");
    btn.className = "formbtn";
    btn.dataset.idx = idx;
    btn.dataset.root = f.root;
    btn.innerHTML = `<img src="${f.thumb || '/assets/dokkaninfo.com/images/dokkan-info-logo.png'}" alt="">
                     <span>${f.title}</span>`;
    formsRail.appendChild(btn);
  });
  // default form: from URL ?form= or base/first
  const {form, mode, step} = getParams();
  let initial = null;
  if(form){
    initial = DATA.forms.find(f=>f.root===form) || null;
  }
  if(!initial){ initial = DATA.forms.find(f=>f.root==="base") || DATA.forms[0]; }
  selectForm(initial);
  if(mode && ["regular","eza","seza"].includes(mode)) curMode = mode;
  updateSteps();
  if(step!=null) curStep = step;
  updateActiveStates();
  pickVariant();
}
function selectForm(f){
  curForm = f;
  // tabs
  tabs.innerHTML = "";
  const t1 = document.createElement("button"); t1.className="tab"; t1.textContent="Regular"; t1.dataset.m="regular"; tabs.appendChild(t1);
  if(f.has_eza){ const t2=document.createElement("button"); t2.className="tab"; t2.textContent="EZA"; t2.dataset.m="eza"; tabs.appendChild(t2); }
  if(f.has_seza){ const t3=document.createElement("button"); t3.className="tab"; t3.textContent="S-EZA"; t3.dataset.m="seza"; tabs.appendChild(t3); }
  if(!["regular","eza","seza"].includes(curMode)) curMode = "regular";
  curStep = null;
  updateSteps();
  updateActiveStates();
  pickVariant();
}
function updateSteps(){
  steps.innerHTML = "";
  const list = curMode==="eza" ? curForm.eza_steps : (curMode==="seza" ? curForm.seza_steps : []);
  if(list && list.length){
    list.forEach(v=>{
      const b = document.createElement("button"); b.className="step"; b.textContent = v.step ?? "?"; b.dataset.step = v.step ?? "0";
      steps.appendChild(b);
    });
    // default to highest if needed
    if(curStep==null){ curStep = list[list.length-1].step ?? null; }
  }else{
    curStep = null;
  }
}
function updateActiveStates(){
  $$(".formbtn", formsRail).forEach((b,i)=>{
    b.classList.toggle("active", DATA.forms[i]===curForm);
  });
  $$(".tab", tabs).forEach(t=> t.classList.toggle("active", t.dataset.m===curMode));
  $$(".step", steps).forEach(s=> s.classList.toggle("active", parseInt(s.dataset.step,10)===curStep));
  setParams({form: curForm?.root, mode: curMode, step: curStep});
}
function pickVariant(){
  let v = null;
  if(curMode==="regular"){ v = curForm.regular; }
  else if(curMode==="eza"){ v = (curForm.eza_steps||[]).find(x=>x.step===curStep) || (curForm.eza_steps||[]).slice(-1)[0]; }
  else if(curMode==="seza"){ v = (curForm.seza_steps||[]).find(x=>x.step===curStep) || (curForm.seza_steps||[]).slice(-1)[0]; }
  curVar = v || curForm.regular || (curForm.eza_steps||[]).slice(-1)[0] || (curForm.seza_steps||[]).slice(-1)[0];
  renderVariant();
  updateActiveStates();
}

formsRail.addEventListener("click", (e)=>{
  const btn = e.target.closest(".formbtn"); if(!btn) return;
  selectForm(DATA.forms[parseInt(btn.dataset.idx,10)]);
});
tabs.addEventListener("click", (e)=>{
  const t = e.target.closest(".tab"); if(!t) return;
  curMode = t.dataset.m;
  updateSteps();
  pickVariant();
});
steps.addEventListener("click", (e)=>{
  const s = e.target.closest(".step"); if(!s) return;
  curStep = parseInt(s.dataset.step,10);
  pickVariant();
});

function renderVariant(){
  if(!curVar) return;
  // title
  const disp = (curVar.display_name && curVar.display_name.trim()) ? curVar.display_name.trim() : DATA.name;
  document.title = `${disp} — Dokkan Unit`;
  $("#title").textContent = disp;

  // art
  const art = curVar.images || {};
  const src = art.full || art.character || "/assets/dokkaninfo.com/images/dokkan-info-logo.png";
  $("#heroImg").src = src;

  // pills
  if($("#rarityP")) $("#rarityP").textContent = curVar.rarity || DATA.rarity || "";
  if($("#typeP")) $("#typeP").textContent = curVar.type || DATA.type || "";
  if($("#obtainP")) $("#obtainP").textContent = curVar.obtain || DATA.obtain || "";
  if($("#relP")) $("#relP").textContent = "Release: " + (curVar.release || DATA.release || "");

  // kit sections
  $("#leaderSkill").textContent = (curVar.leader_skill || "—").replace(/Key/g,"Ki"); // Ki fix
  const su = curVar.super_attack || null;
  $("#superName").textContent = su?.name || "—";
  $("#superEff").textContent = su?.effect || "";

  const ul = curVar.ultra_super_attack || null;
  $("#ultraBlock").style.display = (ul && (ul.name || ul.effect)) ? "" : "none";
  if(ul){ $("#ultraName").textContent = ul.name || "—"; $("#ultraEff").textContent = ul.effect || ""; }

  const ac = curVar.active_skill || null;
  $("#activeBlock").style.display = (ac && (ac.name || ac.effect || ac.activation_conditions || (ac.lines && ac.lines.length))) ? "" : "none";
  if(ac){ $("#activeName").textContent = ac.name || "—"; $("#activeEff").textContent = ac.effect || ""; $("#activeCond").textContent = ac.activation_conditions || ""; }

  const st = curVar.standby_skill || null;
  $("#standbyBlock").style.display = (st && (st.name || st.effect || (st.lines && st.lines.length))) ? "" : "none";
  if(st){ $("#standbyName").textContent = st.name || "—"; $("#standbyEff").textContent = st.effect || ""; }

  const p = curVar.passive_skill || null;
  $("#passiveBox").innerHTML = p ? textOr(p.lines, p.effect) : "—";

  const stats = curVar.stats || {HP:{},ATK:{},DEF:{}};
  $("#hpB").textContent = stats.HP?.Base ?? "—";
  $("#hp55").textContent = stats.HP?.["55%"] ?? "—";
  $("#hp100").textContent = stats.HP?.["100%"] ?? "—";
  $("#atkB").textContent = stats.ATK?.Base ?? "—";
  $("#atk55").textContent = stats.ATK?.["55%"] ?? "—";
  $("#atk100").textContent = stats.ATK?.["100%"] ?? "—";
  $("#defB").textContent = stats.DEF?.Base ?? "—";
  $("#def55").textContent = stats.DEF?.["55%"] ?? "—";
  $("#def100").textContent = stats.DEF?.["100%"] ?? "—";

  const links = curVar.links || [];
  $("#linksBox").innerHTML = links.length ? links.map(l=>`<span class="chip">${l}</span>`).join("") : "—";
  const cats = curVar.categories || [];
  $("#catsBox").innerHTML = cats.length ? cats.map(c=>`<span class="chip">${c}</span>`).join("") : "—";
}

buildUI();
</script>
</body>
</html>
"""

# ---------------------- TEAM BUILDER (wider left, synergy, min-boost filter) --------------------------
TEAM_HTML = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<title>Team Builder — Dokkan</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root{
  --bg:#0b0f14; --card:#0e141d; --ink:#e9f2ff; --muted:#9fb0c7; --accent:#5bd1ff; --accent-2:#9bffdd;
  --chip:#182330; --chipb:#223144; --slot:#0e1520; --ok:#12d18e; --warn:#ffd166; --bad:#ff6b6b;
  --shadow:0 10px 30px rgba(0,0,0,.30);
  --pickCardW: 280px; /* controls picker card width (Small/Med/Large) */
  --teamW: min(780px, 46vw); /* a bit wider for comfort */
  --glass-bg: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));
  --glass-brd: 1px solid rgba(255,255,255,.08);
}
:root[data-theme="light"]{
  --bg:#f7faff; --card:#ffffff; --ink:#0b1623; --muted:#3e526d; --accent:#0d7bd0; --accent-2:#0ab39b;
  --chip:#eef3fb; --chipb:#d9e6f8; --slot:#f2f6ff; --shadow:0 10px 30px rgba(9,16,26,.08);
}
*{box-sizing:border-box}
body{
  margin:0; background:
    radial-gradient(1200px 800px at 15% -10%, rgba(91,209,255,.10), transparent),
    radial-gradient(1000px 600px at 100% 0, rgba(155,255,221,.08), transparent),
    linear-gradient(180deg, #09101a, var(--bg));
  color:var(--ink); font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial;
}
a{color:var(--accent); text-decoration:none}
.wrap{max-width:1700px; margin:0 auto; padding:24px 16px 64px}

/* header */
.header{display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap}
.header h1{font-size:28px; margin:0}
.btn{font-size:12px; padding:8px 12px; background:#0f151d; border:1px solid #1e2a3a; border-radius:10px; color:#fff; cursor:pointer; transition:.15s}
.btn:hover{ border-color:#38506f; transform:translateY(-1px) }
:root[data-theme="light"] .btn{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.btn.ghost{ background:transparent; border-color:#233246 }
.btn.primary{ border-color:#2a9ed6; box-shadow:0 0 0 3px rgba(91,209,255,.16) inset }

/* layout */
.cols{
  display:grid;
  grid-template-columns: var(--teamW) 1fr;
  gap:18px;
  align-items:start;
}
@media (max-width: 1150px){
  :root{ --teamW: 100% }
  .cols{ grid-template-columns: 1fr }
}

/* panels - modern glassy */
.panel{
  background:var(--glass-bg);
  border:var(--glass-brd);
  border-radius:16px;
  padding:12px;
  box-shadow: var(--shadow);
  backdrop-filter: blur(6px);
}
:root[data-theme="light"] .panel{ backdrop-filter:none }

/* sticky team */
.panel.team{ position:sticky; top:12px; z-index:1 }

.h2{margin:0 0 8px; font-size:16px}
.row{display:flex; gap:8px; align-items:center; flex-wrap:wrap}
.note{opacity:.85; font-size:12px}

/* team board */
.board{
  display:grid;
  grid-template-columns: 1fr 1fr;
  gap:12px;
  margin-top:10px;
}
@media (max-width: 560px){
  .board{ grid-template-columns: 1fr }
}
.slot{
  background:var(--slot);
  border:1px dashed #2a3c55;
  border-radius:12px;
  padding:10px;
  display:grid;
  grid-template-columns: 116px 1fr;
  gap:10px;
  align-items:center;
  min-height:122px;
  transition:border-color .15s, background .15s, transform .05s;
}
:root[data-theme="light"] .slot{ border-color:#dbe7ff }
.slot.dragover{ border-color: var(--accent); background: rgba(91,209,255,.06) }
.slot .imgwrap{
  position:relative; border-radius:10px; overflow:hidden;
  background:#0b1118; border:1px solid #172234; display:flex; align-items:center; justify-content:center;
}
:root[data-theme="light"] .slot .imgwrap{ background:#eef3fb; border-color:#d9e6f8 }
.slot img{ width:100%; height:auto; display:block; object-fit:cover }
.slot .meta{ display:flex; flex-direction:column; gap:6px; min-width:0 }
.slot .name{
  font-weight:800; line-height:1.15;
  display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
  overflow:hidden; text-overflow:ellipsis; max-height:3.2em;
}
.slot .sub{ font-size:12px; color:var(--muted); display:flex; gap:6px; flex-wrap:wrap }
.badge{ display:inline-flex; align-items:center; gap:6px; font-size:11px;
  padding:3px 8px; background:#0e151d; border:1px solid #233246; border-radius:999px; color:#cadeff }
:root[data-theme="light"] .badge{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.actions{ display:flex; gap:6px; flex-wrap:wrap }
.lock.active{ box-shadow:0 0 0 2px rgba(155,255,221,.20) inset }

/* warnings / status */
.warn{ color:var(--warn) }
.ok{ color:var(--ok) }
.bad{ color:var(--bad) }

/* picker controls */
.controls{display:flex; gap:8px; align-items:center; flex-wrap:wrap}
.input, .select{background:#0f151d; color:#e9f2ff; border:1px solid #233246; border-radius:10px; padding:8px 10px}
:root[data-theme="light"] .input, :root[data-theme="light"] .select{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.select{cursor:pointer}

/* eligible grid */
.grid{
  display:grid;
  grid-template-columns: repeat(auto-fill, minmax(var(--pickCardW), 1fr));
  gap:14px;
  margin-top:12px;
}
.card{
  background:linear-gradient(180deg, #0f151d, #0b1016);
  border:1px solid #1b2636;
  border-radius:14px;
  overflow:hidden;
  transition: transform .12s ease, box-shadow .12s ease, border-color .12s;
  position:relative;
}
.card:hover{ transform: translateY(-2px); border-color:#2a3b54; box-shadow: var(--shadow) }
:root[data-theme="light"] .card{ background:#fff; border-color:#dbe7ff }
.card .img{ background:#0c121a; display:flex; align-items:center; justify-content:center }
:root[data-theme="light"] .card .img{ background:#eef3fb }
.card .img img{ width:100%; height:auto; display:block; object-fit:contain; max-height:240px }
.card .body{ padding:10px }
.pills{ display:flex; gap:6px; flex-wrap:wrap }
.pill{ font-size:11px; background:#0b1118; padding:4px 8px; border:1px solid #1c2838; border-radius:999px; color:#cfe2ff }
:root[data-theme="light"] .pill{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.ctitle{ font-weight:800; margin:4px 0 6px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis }
.chips{ display:flex; gap:6px; flex-wrap:wrap }
.chip{ background:#0b1118; border:1px solid #1f2b3b; color:#d2e4ff; font-size:12px; padding:5px 9px; border-radius:999px }
:root[data-theme="light"] .chip{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.cactions{ display:flex; gap:6px; flex-wrap:wrap }
.button{ font-size:12px; padding:6px 10px; background:#0f151d; border:1px solid #1e2a3a; border-radius:8px; color:#fff; cursor:pointer; transition:.15s }
.button:hover{border-color:#38506f; transform:translateY(-1px)}
:root[data-theme="light"] .button{ background:#fff; color:#0b1623; border-color:#c7d5ea }

.card[draggable="true"]{ cursor:grab }
.card.dragging{ opacity:.75; transform:scale(.98) }

/* active categories from leader */
.bar{ display:flex; gap:6px; flex-wrap:wrap; align-items:center; margin-top:8px }
.kv{ display:flex; gap:8px; flex-wrap:wrap; margin-top:8px }
.kv .kvitem{ font-size:12px; color:#b1c5df; background:#0e151d; border:1px solid #19263a; padding:4px 8px; border-radius:8px }
:root[data-theme="light"] .kv .kvitem{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }

/* divider */
.hr{ height:1px; background:linear-gradient(90deg, transparent, #2b3d57 40%, #2b3d57 60%, transparent); margin:12px 0 }

/* tag dots */
.dot{ width:8px; height:8px; border-radius:50%; display:inline-block; background:#2a3b54 }
.dot.lr{ background:#ffd166 } .dot.ur{ background:#8bffb1 } .dot.ssr{ background:#a7c7ff }

/* best leader chips */
.lchip{
  display:flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px;
  background:#0d1724; border:1px solid #223246; color:#d8e8ff; font-size:12px; white-space:nowrap;
}
:root[data-theme="light"] .lchip{ background:#fff; border-color:#dbe7ff; color:#0b1623 }
.lchip strong{font-weight:800; max-width:280px; overflow:hidden; text-overflow:ellipsis}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>🧩 Team Builder</h1>
    <a class="btn" href="/">← Back to all units</a>
    <a class="btn" href="/finder">👑 Leader Finder</a>
    <button class="btn" id="themeBtn">🌓 Theme</button>
  </div>

  <div class="cols">
    <!-- LEFT: TEAM BOARD (sticky, wider, flexible) -->
    <div class="panel team" id="teamPanel">
      <div class="row" style="justify-content:space-between">
        <div class="h2" style="margin:0">Team Board</div>
        <div class="row">
          <button class="btn" id="pickLeaderBtn">Pick Leader</button>
          <button class="btn primary" id="suggestLeaderBtn" title="Pick the best leader for current team">✨ Suggest Best Leader</button>
          <button class="btn" id="removeLeaderBtn" disabled>Remove Leader</button>
        </div>
      </div>

      <div class="row" style="margin-top:6px">
        <label class="kvitem">Min Boost
          <select class="select" id="minBoost">
            <option value="0">Any</option>
            <option value="200">200%+</option>
            <option value="220">220%+</option>
          </select>
        </label>
        <label class="kvitem">Prevent duplicates <input type="checkbox" id="noDup" checked></label>
        <button class="btn" id="swapModeBtn" title="Swap two slots">⇄ Swap</button>
        <button class="btn" id="autoFill">✨ Auto-fill best</button>
      </div>

      <div class="kv" id="leaderInfo" style="margin-top:6px"></div>

      <!-- Dynamic best leaders strip -->
      <div class="bar" id="bestLeaderStrip"></div>

      <div class="board" id="board">
        <!-- Leader -->
        <div class="slot" data-slot="0" draggable="true">
          <div class="imgwrap"><img id="slot0img" alt="" style="display:none"></div>
          <div class="meta">
            <div class="name" id="slot0name">Leader</div>
            <div class="sub"><span class="badge" id="slot0badge">Pick a leader</span></div>
            <div class="actions">
              <button class="button" id="slot0clear" disabled>Clear</button>
              <a class="button" id="slot0details" target="_blank" style="display:none">Details ↗</a>
            </div>
          </div>
        </div>

        <!-- Slots 1-5 -->
        {% for i in range(1,6) %}
        <div class="slot" data-slot="{{i}}" draggable="true">
          <div class="imgwrap"><img id="slot{{i}}img" alt="" style="display:none"></div>
          <div class="meta">
            <div class="name" id="slot{{i}}name">Empty</div>
            <div class="sub">
              <span class="badge" id="slot{{i}}boost">—</span>
              <span class="badge" id="slot{{i}}syn">Links 0 • Cats 0</span>
            </div>
            <div class="actions">
              <button class="button lock" data-slot="{{i}}" title="Lock this slot">🔒 Lock</button>
              <button class="button remove" data-slot="{{i}}">Remove</button>
              <a class="button" id="slot{{i}}details" target="_blank" style="display:none">Details ↗</a>
            </div>
          </div>
        </div>
        {% endfor %}
      </div>

      <div class="hr"></div>

      <!-- Team summary -->
      <div class="h2">Team Summary</div>
      <div class="kv" id="teamSummary">
        <span class="kvitem"><strong>Total Shared Links</strong> <span id="sumLinks">0</span></span>
        <span class="kvitem"><strong>Avg Links/Pair</strong> <span id="avgLinks">0</span></span>
        <span class="kvitem"><strong>Members</strong> <span id="countMembers">0</span>/6</span>
        <span class="kvitem"><strong>Duplicates</strong> <span id="dups" class="ok">0</span></span>
      </div>

      <div class="row" style="margin-top:10px">
        <button class="btn" id="clearTeam">Clear team</button>
        <button class="btn" id="shareTeam">Copy share link</button>
        <button class="btn" id="downloadTeam">Export preset</button>
        <label class="kvitem">Card size
          <select class="select" id="cardSize">
            <option value="260">Small</option>
            <option value="280" selected>Medium</option>
            <option value="320">Large</option>
          </select>
        </label>
      </div>

      <!-- Presets -->
      <div class="hr"></div>
      <div class="h2">Presets</div>
      <div class="row">
        <select class="select" id="presetSelect" style="min-width:200px"></select>
        <button class="btn" id="savePreset">Save current</button>
        <button class="btn" id="loadPreset" disabled>Load</button>
        <button class="btn" id="deletePreset" disabled>Delete</button>
        <input class="input" id="presetName" placeholder="Preset name…" style="flex:1; min-width:160px">
      </div>
    </div>

    <!-- RIGHT: PICKER -->
    <div class="panel picker">
      <div class="row" style="justify-content:space-between">
        <div class="h2" style="margin:0">Eligible Teammates</div>
        <div class="row">
          <label class="kvitem">Sort
            <select class="select" id="sort">
              <option value="boost">Best Boost</option>
              <option value="synergy">Best Synergy</option>
              <option value="rarity">LR → UR → SSR</option>
              <option value="name">Name (A→Z)</option>
              <option value="type">Type</option>
            </select>
          </label>
          <label class="kvitem">Type
            <select class="select" id="type">
              <option value="">All</option>
              <option>AGL</option><option>TEQ</option><option>INT</option><option>STR</option><option>PHY</option>
            </select>
          </label>
          <label class="kvitem">Rarity
            <select class="select" id="rarity">
              <option value="">All</option>
              <option value="LR">LR</option>
              <option value="UR">UR</option>
              <option value="SSR">SSR</option>
            </select>
          </label>
          <label class="kvitem">Favorites <input type="checkbox" id="favOnly"></label>
        </div>
      </div>

      <div class="row" style="margin-top:6px">
        <input class="input" id="eligibleQ" placeholder="Search name, ID, category, link…" style="flex:1; min-width:220px">
        <button class="btn ghost" id="clearFilters">Reset</button>
      </div>

      <div class="bar" id="activeCats"></div>

      <div class="grid" id="cards"></div>
    </div>
  </div>
</div>

<!-- Leader picker modal -->
<div class="panel" id="leaderModal" style="position:fixed; inset:40px; display:none; z-index:50; overflow:auto">
  <div class="row" style="justify-content:space-between">
    <div class="h2" style="margin:0">Pick a Leader</div>
    <button class="btn" id="closeLeader">Close</button>
  </div>
  <div class="row" style="margin-top:8px">
    <input class="input" id="leaderQ" placeholder="Search leaders by name, ID, category…" style="flex:1; min-width:240px" />
    <label class="kvitem">Type
      <select class="select" id="leaderType">
        <option value="">All</option>
        <option>AGL</option><option>TEQ</option><option>INT</option><option>STR</option><option>PHY</option>
      </select>
    </label>
    <label class="kvitem">Favorites <input type="checkbox" id="leaderFavOnly"></label>
  </div>

  <!-- Top suggestions appear here when you open the modal -->
  <div class="bar" id="modalBestStrip" style="margin-top:10px"></div>

  <div class="grid" id="leaderGrid" style="margin-top:12px"></div>
</div>

<script>
const UNITS = {{ units|tojson }};
const $ = (s,root=document)=>root.querySelector(s);
const $$=(s,root=document)=>Array.from(root.querySelectorAll(s));

/* theme */
(function theme(){
  const root = document.documentElement;
  const saved = localStorage.getItem("dokkan.theme") || "dark";
  root.setAttribute("data-theme", saved);
  $("#themeBtn").addEventListener("click", ()=>{
    const cur = root.getAttribute("data-theme")==="dark" ? "light" : "dark";
    root.setAttribute("data-theme", cur);
    localStorage.setItem("dokkan.theme", cur);
  });
})();

const cards = $("#cards");
const sortSel = $("#sort");
const typeSel = $("#type");
const raritySel = $("#rarity");
const favOnly = $("#favOnly");
const activeCats = $("#activeCats");
const eligibleQ = $("#eligibleQ");
const clearFilters = $("#clearFilters");
const cardSize = $("#cardSize");

const pickLeaderBtn = $("#pickLeaderBtn");
const suggestLeaderBtn = $("#suggestLeaderBtn");
const removeLeaderBtn = $("#removeLeaderBtn");
const minBoostSel = $("#minBoost");
const noDup = $("#noDup");
const swapModeBtn = $("#swapModeBtn");
const autoFillBtn = $("#autoFill");

const leaderModal = $("#leaderModal");
const leaderQ = $("#leaderQ");
const leaderType = $("#leaderType");
const leaderFavOnly = $("#leaderFavOnly");
const leaderGrid = $("#leaderGrid");
const closeLeader = $("#closeLeader");

const leaderInfo = $("#leaderInfo");
const bestLeaderStrip = $("#bestLeaderStrip");
const modalBestStrip = $("#modalBestStrip");

const slots = {};
for(let i=0;i<=5;i++){
  slots[i] = {
    wrap: document.querySelector(`.slot[data-slot="${i}"]`),
    img: $(`#slot${i}img`),
    name: $(`#slot${i}name`),
    badge: $(`#slot${i}badge`) || $(`#slot${i}boost`),
    syn: $(`#slot${i}syn`),
    clearBtn: $(`#slot${i}clear`) || document.querySelector(`.remove[data-slot="${i}"]`),
    details: $(`#slot${i}details`),
    lockBtn: document.querySelector(`.lock[data-slot="${i}"]`),
    locked: false,
    id: ""
  };
}

/* helpers */
function isFav(u){ try{ return (JSON.parse(localStorage.getItem("dokkan.favs")||"[]")).includes(u.id); }catch{ return false; } }
function clamp(txt, max=80){ return (txt||"").length>max ? (txt.slice(0,max-1)+"…") : (txt||""); }

/* parsing + boost (shared with Finder) */
function parseLeaderSkill(text){
  const res = {primary:[], secondary:[], ki:null, main_pct:null, add_pct:null, secondary_total:null, has_secondary:false, all_types:false, flat_total_max:null};
  if(!text) return res;
  const t = text.replace(/\s+/g,' ').trim();
  res.all_types = /all types?/i.test(t);
  const kis = Array.from(t.matchAll(/Ki\s*\+(\d+)/ig)).map(m=>parseInt(m[1],10));
  res.ki = kis.length ? Math.max(...kis) : null;
  const idx = t.toLowerCase().indexOf("plus an additional");
  const head = idx>=0 ? t.slice(0,idx) : t; const tail = idx>=0 ? t.slice(idx) : "";
  const headPcts = Array.from(head.matchAll(/\+(\d+)%/g)).map(m=>parseInt(m[1],10));
  res.main_pct = headPcts.length ? Math.max(...headPcts) : null;
  res.primary = Array.from(head.matchAll(/"([^"]+)"/g)).map(m=>m[1]);
  if(tail){ res.has_secondary = true;
    res.secondary = Array.from(tail.matchAll(/"([^"]+)"/g)).map(m=>m[1]);
    const adds = Array.from(tail.matchAll(/\+(\d+)%/g)).map(m=>parseInt(m[1],10));
    if(adds.length){ res.add_pct = Math.max(...adds); }
    if(res.main_pct!=null && res.add_pct!=null){ res.secondary_total = res.main_pct + res.add_pct; }
  }
  const all = Array.from(t.matchAll(/\+(\d+)%/g)).map(m=>parseInt(m[1],10));
  if(all.length) res.flat_total_max = Math.max(...all);
  return res;
}
function maxBoostForUnit(leaderUnit, unit){
  const p = parseLeaderSkill(leaderUnit.leader_skill||"");
  if(!p) return 0;
  if(p.all_types){
    return p.main_pct ?? p.flat_total_max ?? 0;
  }
  const set = new Set(unit.categories||[]);
  const hasPrimary = p.primary.some(c=>set.has(c));
  if(!hasPrimary) return 0;
  let total = p.main_pct ?? 0;
  if(p.has_secondary && p.secondary && p.add_pct){
    const hasSec = p.secondary.some(c=>set.has(c));
    if(hasSec){
      total = Math.max(total, (p.secondary_total || 0));
    }
  }
  total = Math.max(total, p.flat_total_max || 0);
  return total;
}
function synergyWithLeader(leaderUnit, unit){
  if(!leaderUnit) return {links:0, cats:0, score:0};
  const Llinks = new Set(leaderUnit.links || []);
  const Lcats  = new Set(leaderUnit.categories || []);
  const links = (unit.links||[]).filter(x=>Llinks.has(x)).length;
  const cats  = (unit.categories||[]).filter(x=>Lcats.has(x)).length;
  return {links, cats, score: links*2 + cats};
}
function pairSharedLinks(u1,u2){
  const a = new Set(u1?.links||[]);
  return (u2?.links||[]).filter(l=>a.has(l)).length;
}

/* find best leaders for current team members */
function currentMembers(){
  const out = [];
  for(let i=1;i<=5;i++){ if(slots[i].id){ const u = UNITS.find(x=>x.id===slots[i].id); if(u) out.push(u); } }
  return out;
}
function scoreLeaderCandidate(L){
  const min = parseInt(minBoostSel.value,10)||0;
  const members = currentMembers();
  if(!members.length) return null;
  let covered = 0, boostSum = 0, synSum = 0;
  for(const m of members){
    const b = maxBoostForUnit(L, m);
    if(b >= min && b > 0){ covered++; }
    boostSum += b;
    synSum += synergyWithLeader(L, m).score;
  }
  // heavy weight on coverage, then boost, then synergy; LR favored slightly
  const rarityBonus = {"LR":2,"UR":1,"SSR":0}[L.rarity||""] || 0;
  const score = covered*100000 + boostSum*100 + synSum*5 + rarityBonus;
  return {L, covered, boostSum, synSum, score, mainpct: (parseLeaderSkill(L.leader_skill||"").main_pct||0)};
}
function bestLeaders(limit=6){
  const cands = UNITS.filter(u => (u.leader_skill||"").trim());
  const scored = [];
  for(const L of cands){
    const s = scoreLeaderCandidate(L);
    if(s) scored.push(s);
  }
  scored.sort((a,b)=> b.score - a.score || a.L.rarity_rank - b.L.rarity_rank || a.L.name.localeCompare(b.L.name));
  return scored.slice(0, limit);
}
function renderBestLeaderStrip(){
  const picks = bestLeaders(6);
  if(!picks.length){ bestLeaderStrip.innerHTML = ""; return; }
  bestLeaderStrip.innerHTML = picks.map(x=>(
    `<span class="lchip" title="Covers ${x.covered} member(s); main ${x.mainpct}%">
      <strong>${x.L.name}</strong>
      <span class="chip">Covers ${x.covered}</span>
      <span class="chip">Boost Σ ${x.boostSum}%</span>
      <button class="btn" data-leader="${x.L.id}">Use</button>
    </span>`
  )).join("");
  bestLeaderStrip.addEventListener("click", (e)=>{
    const b = e.target.closest("button[data-leader]");
    if(!b) return;
    const L = UNITS.find(u=>u.id===b.dataset.leader);
    if(L) setLeader(L);
  }, {once:true});
}
function renderModalBestStrip(){
  const picks = bestLeaders(5);
  modalBestStrip.innerHTML = picks.length ? (
    `<span class="chip" style="opacity:.7">Best now:</span>` +
    picks.map(x=>`<span class="lchip"><strong>${x.L.name}</strong><span class="chip">${x.covered} cover</span><button class="btn" data-leader="${x.L.id}">Use</button></span>`).join("")
  ) : "";
  modalBestStrip.addEventListener("click",(e)=>{
    const b = e.target.closest("button[data-leader]"); if(!b) return;
    const L = UNITS.find(u=>u.id===b.dataset.leader); if(L){ setLeader(L); leaderModal.style.display="none"; }
  }, {once:true});
}

/* state */
let leader = null;
let leaderParsed = null;
let swapMode = false;

/* UI: leader & board */
function setLeader(u){
  leader = u || null;
  leaderParsed = u ? parseLeaderSkill(u.leader_skill||"") : null;
  renderLeaderInfo();
  updateSlot(0, u);
  removeLeaderBtn.disabled = !u;
  renderEligible();
  syncShare();
  computeTeamSummary();
}
function renderLeaderInfo(){
  leaderInfo.innerHTML = "";
  if(!leader){ renderBestLeaderStrip(); return; }
  const p = leaderParsed;
  const cats = p.primary.length ? p.primary : (p.all_types ? ["All Types"] : []);
  const sec = p.secondary || [];
  const nodes = [];
  if(p.ki!=null) nodes.push(`<span class="kvitem"><strong>Ki</strong> +${p.ki}</span>`);
  if(p.main_pct!=null) nodes.push(`<span class="kvitem"><strong>Main</strong> ${p.main_pct}%</span>`);
  if(p.secondary_total!=null) nodes.push(`<span class="kvitem"><strong>Also belong</strong> ${p.secondary_total}%</span>`);
  leaderInfo.innerHTML = nodes.join(" ") + `<div class="bar" style="margin-top:6px">${cats.map(c=>`<span class="chip">${c}</span>`).join("")}${sec.length?`<span class="chip" style="opacity:.7">also:</span>`:""}${sec.map(c=>`<span class="chip">${c}</span>`).join("")}</div>`;
  renderBestLeaderStrip();
}

/* Slot helpers */
function getUnitById(id){ return UNITS.find(x=>x.id===id) || null; }
function currentTeamIds(){
  const out = [];
  for(let i=0;i<=5;i++){ if(slots[i].id) out.push(slots[i].id); }
  return out;
}
function duplicateCount(){
  const ids = currentTeamIds().filter(Boolean);
  const map = new Map();
  ids.forEach(id=> map.set(id, (map.get(id)||0)+1));
  let d=0; map.forEach(v=>{ if(v>1) d+=v-1; });
  return d;
}
function updateSlot(slotIdx, unit){
  const s = slots[slotIdx];
  s.id = unit ? unit.id : "";
  s.img.style.display = unit ? "" : "none";
  s.img.src = unit?.img || "";
  s.name.textContent = unit ? unit.name : (slotIdx===0 ? "Leader" : "Empty");
  if(slotIdx===0){
    const txt = unit ? (unit.leader_skill||'—').replace(/Key/g,'Ki') : "Pick a leader";
    s.badge.textContent = unit ? clamp(txt, 90) : "Pick a leader";
    s.details.style.display = unit ? "" : "none";
    if(unit) s.details.href = `/unit/${unit.id}`;
    s.clearBtn && (s.clearBtn.disabled = !unit);
  }else{
    if(leader){
      const boost = unit ? maxBoostForUnit(leader, unit) : 0;
      s.badge.textContent = unit ? `Boost ${boost}%` : "—";
      const syn = unit ? synergyWithLeader(leader, unit) : {links:0,cats:0};
      if(s.syn) s.syn.textContent = `Links ${syn.links} • Cats ${syn.cats}`;
    }else{
      s.badge.textContent = unit ? "—" : "—";
      if(s.syn) s.syn.textContent = "Links 0 • Cats 0";
    }
    s.details.style.display = unit ? "" : "none";
    if(unit) s.details.href = `/unit/${unit.id}`;
  }
  computeTeamSummary();
  renderBestLeaderStrip();
}

/* Eligible list */
function unitEligible(u){
  if(!leaderParsed || !leader) return false;
  const min = parseInt(minBoostSel.value,10)||0;
  const boost = maxBoostForUnit(leader, u);
  if(boost < min) return false;
  return boost > 0;
}
function renderEligible(){
  if(!leader){
    cards.innerHTML = `<div class="note">Pick a leader to list eligible units.</div>`;
    activeCats.innerHTML="";
    return;
  }
  const type = typeSel.value;
  const rar = raritySel.value;
  const fav = favOnly.checked;
  const q = (eligibleQ.value||"").toLowerCase().trim();

  let arr = UNITS.filter(u=>{
    const okE = unitEligible(u);
    const okT = !type || u.type===type;
    const okR = !rar || (u.rarity||"")===rar;
    const okF = !fav || isFav(u);
    let okQ = true;
    if(q){
      const cats = (u.categories||[]).join(" ").toLowerCase();
      const links = (u.links||[]).join(" ").toLowerCase();
      okQ = u.name.toLowerCase().includes(q) || u.id.includes(q) || (u.type||"").toLowerCase().includes(q) || cats.includes(q) || links.includes(q);
    }
    return okE && okT && okR && okF && okQ;
  });

  // decorate
  const teamIds = new Set(currentTeamIds());
  arr = arr.map(u=>{
    const boost = maxBoostForUnit(leader,u);
    const syn = synergyWithLeader(leader,u);
    return {u, boost, syn, isAdded: teamIds.has(u.id) || (leader && u.id===leader.id)};
  });

  // sort
  const mode = sortSel.value;
  if(mode==="boost"){ arr.sort((a,b)=> b.boost - a.boost || b.syn.score - a.syn.score || a.u.rarity_rank - b.u.rarity_rank || a.u.name.localeCompare(b.u.name)); }
  else if(mode==="synergy"){ arr.sort((a,b)=> b.syn.score - a.syn.score || b.boost - a.boost || a.u.rarity_rank - b.u.rarity_rank || a.u.name.localeCompare(b.u.name)); }
  else if(mode==="rarity"){ arr.sort((a,b)=> a.u.rarity_rank - b.u.rarity_rank || a.u.name.localeCompare(b.u.name)); }
  else if(mode==="name"){ arr.sort((a,b)=> a.u.name.localeCompare(b.u.name)); }
  else if(mode==="type"){ arr.sort((a,b)=> (a.u.type||"").localeCompare(b.u.type||"") || (a.u.rarity_rank - b.u.rarity_rank)); }

  // render (no hover leader-skill preview anymore)
  cards.innerHTML = "";
  for(const row of arr){
    const u = row.u;
    const badge = u.rarity==="LR" ? `<span class="pill"><span class="dot lr"></span> LR</span>` :
                   u.rarity==="UR" ? `<span class="pill"><span class="dot ur"></span> UR</span>` :
                                     `<span class="pill"><span class="dot ssr"></span> SSR</span>`;
    const disabled = row.isAdded;
    const el = document.createElement("article");
    el.className="card";
    el.setAttribute("draggable","true");
    el.dataset.id = u.id;
    el.innerHTML = `
      <div class="img" title="Drag to a slot to add">
        <img src="${u.img || '/assets/dokkaninfo.com/images/dokkan-info-logo.png'}" alt="">
      </div>
      <div class="body">
        <div class="pills"><span class="pill">${u.type||''}</span>${badge}<span class="pill">Boost ${row.boost||0}%</span></div>
        <div class="ctitle" title="${u.name}">${u.name}</div>
        <div class="chips" style="margin-top:6px"><span class="chip">Links ${row.syn.links}</span><span class="chip">Cats ${row.syn.cats}</span></div>
        <div class="cactions" style="margin-top:6px">
          <button class="button add" data-id="${u.id}" ${disabled?'disabled':''}>${disabled?'Added':'Add to team'}</button>
          <a class="button" href="/unit/${u.id}" target="_blank">Details ↗</a>
        </div>
      </div>
    `;
    // drag from picker
    el.addEventListener("dragstart", (e)=>{ el.classList.add("dragging"); e.dataTransfer.setData("text/plain", u.id); });
    el.addEventListener("dragend", ()=> el.classList.remove("dragging"));
    cards.appendChild(el);
  }

  // active category chips (from leader)
  const chips = [];
  const p = leaderParsed;
  if(p.all_types){ chips.push(`<span class="chip">All Types</span>`); }
  p.primary.forEach(c=>chips.push(`<span class="chip">${c}</span>`));
  if(p.secondary && p.secondary.length){
    chips.push(`<span class="chip" style="opacity:.7">also:</span>`);
    p.secondary.forEach(c=>chips.push(`<span class="chip">${c}</span>`));
  }
  activeCats.innerHTML = chips.join("");
}

/* interactions - picker -> add buttons */
cards.addEventListener("click", (e)=>{
  const btn = e.target.closest(".add");
  if(!btn) return;
  const u = getUnitById(btn.dataset.id);
  addUnitToFirstOpen(u);
});

/* board: drag & drop */
function enableDrops(){
  Object.values(slots).forEach(s=>{
    const el = s.wrap;
    el.addEventListener("dragover", (e)=>{ e.preventDefault(); el.classList.add("dragover"); });
    el.addEventListener("dragleave", ()=> el.classList.remove("dragover"));
    el.addEventListener("drop", (e)=>{
      e.preventDefault(); el.classList.remove("dragover");
      const id = e.dataTransfer.getData("text/plain");
      const fromSlot = e.dataTransfer.getData("text/slot");
      if(fromSlot){ // reordering slots
        const a = parseInt(fromSlot,10), b = parseInt(el.dataset.slot,10);
        swapSlots(a,b); return;
      }
      if(!id) return;
      const u = getUnitById(id);
      if(!u) return;
      placeInSlot(parseInt(el.dataset.slot,10), u);
    });
  });
  // dragging slots to reorder
  Object.values(slots).forEach(s=>{
    s.wrap.addEventListener("dragstart", (e)=>{
      const idx = parseInt(s.wrap.dataset.slot,10);
      e.dataTransfer.setData("text/slot", String(idx));
      s.wrap.classList.add("dragging");
    });
    s.wrap.addEventListener("dragend", ()=> s.wrap.classList.remove("dragging"));
  });
}
enableDrops();

/* slot controls */
document.querySelectorAll(".remove").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    const i = parseInt(btn.dataset.slot,10);
    updateSlot(i, null);
    renderEligible();
  });
});
document.querySelectorAll(".lock").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    const i = parseInt(btn.dataset.slot,10);
    slots[i].locked = !slots[i].locked;
    btn.classList.toggle("active", slots[i].locked);
  });
});
const clearLeaderBtn = $("#slot0clear");
if(clearLeaderBtn){
  clearLeaderBtn.addEventListener("click", ()=>{
    setLeader(null);
    updateSlot(0, null);
    renderEligible();
  });
}

/* core actions */
function placeInSlot(slotIdx, unit){
  if(slotIdx===0){ setLeader(unit); return; }
  if(slots[slotIdx].locked){ flashWarn("Slot is locked"); return; }
  if(noDup.checked){
    const ids = currentTeamIds().filter(Boolean);
    if(ids.includes(unit.id)){ flashWarn("Already in team"); return; }
    if(leader && unit.id===leader.id){ flashWarn("Same as leader"); return; }
  }
  updateSlot(slotIdx, unit);
  renderEligible();
}
function addUnitToFirstOpen(unit){
  for(let i=1;i<=5;i++){
    if(!slots[i].id && !slots[i].locked){
      placeInSlot(i, unit); return;
    }
  }
  flashWarn("Team is full or locked");
}
function swapSlots(a,b){
  if(a===b) return;
  if(slots[a].locked || slots[b].locked){ flashWarn("Locked slot"); return; }
  const ua = slots[a].id ? getUnitById(slots[a].id) : null;
  const ub = slots[b].id ? getUnitById(slots[b].id) : null;
  updateSlot(a, ub);
  updateSlot(b, ua);
  renderEligible();
}

/* feedback */
function flashWarn(text){
  const el = document.createElement("div");
  el.textContent = text;
  el.style.position="fixed"; el.style.left="50%"; el.style.top="18px"; el.style.transform="translateX(-50%)";
  el.style.background="#1b2636"; el.style.border="1px solid #2a3b54"; el.style.padding="8px 12px"; el.style.borderRadius="10px";
  el.style.color="#ffd166"; el.style.zIndex="100";
  document.body.appendChild(el);
  setTimeout(()=> el.remove(), 1200);
}

/* filters & size */
[sortSel, typeSel, raritySel, favOnly, minBoostSel].forEach(el=> el.addEventListener("change", ()=>{ renderLeaderInfo(); renderEligible(); syncShare(); }));
eligibleQ.addEventListener("input", ()=> renderEligible());
clearFilters.addEventListener("click", ()=>{
  eligibleQ.value=""; typeSel.value=""; raritySel.value=""; favOnly.checked=false; sortSel.value="boost";
  renderEligible();
});
cardSize.addEventListener("change", ()=>{
  const px = cardSize.value;
  document.documentElement.style.setProperty('--pickCardW', px+'px');
});

/* swap mode (click two slots to swap) */
swapModeBtn.addEventListener("click", ()=>{
  swapMode = !swapMode;
  swapModeBtn.classList.toggle("active", swapMode);
  swapModeBtn.textContent = swapMode ? "✓ Swap (tap two slots)" : "⇄ Swap";
});
let firstSwap = null;
$("#board").addEventListener("click", (e)=>{
  if(!swapMode) return;
  const s = e.target.closest(".slot");
  if(!s) return;
  const idx = parseInt(s.dataset.slot,10);
  if(firstSwap==null){ firstSwap = idx; s.style.outline="2px solid rgba(91,209,255,.5)"; }
  else{
    const firstEl = document.querySelector(`.slot[data-slot="${firstSwap}"]`);
    if(firstEl) firstEl.style.outline="";
    const a = firstSwap, b = idx; firstSwap=null;
    swapSlots(a,b);
  }
});

/* share & export */
function syncShare(){
  const p = new URLSearchParams();
  if(leader) p.set("leader", leader.id);
  const min = parseInt(minBoostSel.value,10)||0;
  if(min) p.set("min", String(min));
  const ids = currentTeamIds().slice(1).filter(Boolean); // only teammates
  if(ids.length) p.set("team", ids.join(","));
  history.replaceState(null, "", "?"+p.toString());
}
$("#shareTeam").addEventListener("click", ()=>{
  syncShare(); navigator.clipboard?.writeText(location.href);
  $("#shareTeam").textContent="Link copied!"; setTimeout(()=>$("#shareTeam").textContent="Copy share link", 1200);
});
$("#downloadTeam").addEventListener("click", ()=>{
  const data = {
    leader: leader?.id || "",
    min: parseInt(minBoostSel.value,10)||0,
    team: currentTeamIds().slice(1).filter(Boolean)
  };
  const blob = new Blob([JSON.stringify(data,null,2)], {type:"application/json"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = "dokkan-team.json"; a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1000);
});

/* presets (localStorage) */
const PREKEY = "dokkan.team.presets.v3";
function getPresets(){ try{ return JSON.parse(localStorage.getItem(PREKEY)||"[]"); }catch{ return []; } }
function setPresets(arr){ localStorage.setItem(PREKEY, JSON.stringify(arr)); }
function refreshPresetSelect(){
  const sel = $("#presetSelect");
  const list = getPresets();
  sel.innerHTML = list.length ? list.map((p,i)=>`<option value="${i}">${p.name}</option>`).join("") : "";
  $("#loadPreset").disabled = !list.length;
  $("#deletePreset").disabled = !list.length;
}
$("#savePreset").addEventListener("click", ()=>{
  const name = ($("#presetName").value||"").trim() || new Date().toLocaleString();
  const data = {
    name,
    leader: leader?.id || "",
    min: parseInt(minBoostSel.value,10)||0,
    team: currentTeamIds().slice(1).filter(Boolean)
  };
  const list = getPresets(); list.push(data); setPresets(list); refreshPresetSelect();
  $("#presetName").value=""; flashWarn("Preset saved ✓");
});
$("#loadPreset").addEventListener("click", ()=>{
  const idx = parseInt($("#presetSelect").value||"-1",10); const list = getPresets(); if(idx<0 || !list[idx]) return;
  const p = list[idx];
  if(p.leader){ const u = getUnitById(p.leader); if(u) setLeader(u); else setLeader(null); }
  minBoostSel.value = String(p.min||0);
  for(let i=1;i<=5;i++){ updateSlot(i,null); }
  let k=1;
  (p.team||[]).forEach(id=>{ if(k<=5){ const u=getUnitById(id); if(u) updateSlot(k++, u); }});
  renderEligible(); computeTeamSummary(); syncShare();
});
$("#deletePreset").addEventListener("click", ()=>{
  const idx = parseInt($("#presetSelect").value||"-1",10); const list = getPresets(); if(idx<0 || !list[idx]) return;
  list.splice(idx,1); setPresets(list); refreshPresetSelect();
});
refreshPresetSelect();

/* leader picker modal */
pickLeaderBtn.addEventListener("click", ()=>{
  leaderModal.style.display="block";
  renderLeaderPicker(); renderModalBestStrip(); leaderQ.focus();
});
suggestLeaderBtn.addEventListener("click", ()=>{
  const top = bestLeaders(1)[0];
  if(top){ setLeader(top.L); }
  else{ flashWarn("Add teammates first"); }
});
closeLeader.addEventListener("click", ()=> leaderModal.style.display="none");
leaderQ.addEventListener("input", renderLeaderPicker);
leaderType.addEventListener("change", renderLeaderPicker);
leaderFavOnly.addEventListener("change", renderLeaderPicker);

function renderLeaderPicker(){
  const q = (leaderQ.value||"").toLowerCase().trim();
  const t = leaderType.value;
  const fav = leaderFavOnly.checked;
  const candidates = UNITS.filter(u => (u.leader_skill||"").trim());
  let arr = candidates.filter(u=>{
    const okType = t? (u.type===t) : true;
    const okFav = fav? isFav(u) : true;
    let okQ = true;
    if(q){
      const cats = (u.categories||[]).join(" ").toLowerCase();
      okQ = u.name.toLowerCase().includes(q) || u.id.includes(q) || (u.type||"").toLowerCase().includes(q) || cats.includes(q);
    }
    return okType && okFav && okQ;
  });
  arr.sort((a,b)=> a.rarity_rank - b.rarity_rank || a.name.localeCompare(b.name));

  leaderGrid.innerHTML = "";
  arr.slice(0,120).forEach(u=>{
    const badge = u.rarity==="LR" ? `<span class="pill">LR</span>` : u.rarity==="UR" ? `<span class="pill">UR</span>` : `<span class="pill">SSR</span>`;
    const p = parseLeaderSkill(u.leader_skill);
    const extra = p.secondary_total!=null ? `<span class="chip">also: ${p.secondary_total}%</span>` : "";
    const el = document.createElement("article");
    el.className="card";
    el.innerHTML = `
      <div class="img"><img src="${u.img || '/assets/dokkaninfo.com/images/dokkan-info-logo.png'}" alt=""></div>
      <div class="body">
        <div class="pills"><span class="pill">${u.type||''}</span>${badge}<span class="pill">${p.main_pct??'?' }%</span></div>
        <div class="ctitle">${u.name}</div>
        <div class="chips" style="margin-top:6px">${(p.primary||[]).slice(0,3).map(c=>`<span class="chip">${c}</span>`).join("")}${extra}</div>
        <div class="cactions" style="margin-top:6px">
          <button class="button choose" data-id="${u.id}">Choose Leader</button>
          <a class="button" href="/unit/${u.id}" target="_blank">Details ↗</a>
        </div>
      </div>`;
    el.querySelector(".choose").addEventListener("click", ()=>{
      setLeader(u); leaderModal.style.display="none";
    });
    leaderGrid.appendChild(el);
  });
}

/* auto-fill best (fill empty, unlocked slots by highest boost; tiebreak: synergy, rarity) */
autoFillBtn.addEventListener("click", ()=>{
  if(!leader){ flashWarn("Pick a leader first"); return; }
  const filled = new Set(currentTeamIds().filter(Boolean));
  const candidates = UNITS
    .filter(u=> unitEligible(u) && u.id!==leader.id && !filled.has(u.id))
    .map(u=> ({u, boost:maxBoostForUnit(leader,u), syn:synergyWithLeader(leader,u)}))
    .sort((a,b)=> b.boost - a.boost || b.syn.score - a.syn.score || a.u.rarity_rank - b.u.rarity_rank);

  for(let i=1;i<=5;i++){
    if(!slots[i].id && !slots[i].locked){
      const pick = candidates.shift();
      if(!pick) break;
      updateSlot(i, pick.u);
    }
  }
  renderEligible(); computeTeamSummary(); syncShare();
});

/* team summary */
function computeTeamSummary(){
  const ids = currentTeamIds();
  $("#countMembers").textContent = ids.filter(Boolean).length;
  // duplicates
  const d = duplicateCount();
  const dEl = $("#dups");
  dEl.textContent = d;
  dEl.className = d ? "bad" : "ok";

  // shared links across team (pairwise)
  const members = ids.map(getUnitById).filter(Boolean);
  let totalLinks = 0, pairs = 0;
  for(let i=0;i<members.length;i++){
    for(let j=i+1;j<members.length;j++){
      totalLinks += pairSharedLinks(members[i], members[j]);
      pairs++;
    }
  }
  $("#sumLinks").textContent = totalLinks;
  $("#avgLinks").textContent = pairs ? (Math.round((totalLinks/pairs)*10)/10) : 0;
}

/* clear team */
$("#clearTeam").addEventListener("click", ()=>{
  setLeader(null);
  updateSlot(0,null);
  for(let i=1;i<=5;i++){ updateSlot(i,null); slots[i].locked=false; slots[i].lockBtn?.classList.remove("active"); }
  renderEligible(); computeTeamSummary(); syncShare();
});

/* remove leader button */
removeLeaderBtn.addEventListener("click", ()=>{
  setLeader(null);
  renderEligible();
});

/* restore from URL */
(function restore(){
  refreshPresetSelect();

  const params = new URLSearchParams(location.search);
  const lid = params.get("leader");
  if(lid){
    const u = getUnitById(lid);
    if(u) setLeader(u);
  }
  const min = parseInt(params.get("min")||"0",10)||0;
  if(min){ minBoostSel.value = String(min); }
  const team = params.get("team");
  if(team){
    const arr = team.split(",");
    let idx = 1;
    for(const id of arr){
      if(idx>5) break;
      const u = getUnitById(id);
      if(u){ updateSlot(idx++, u); }
    }
  }
  renderEligible(); computeTeamSummary(); renderBestLeaderStrip();
})();

/* picker card size init */
document.documentElement.style.setProperty('--pickCardW', cardSize.value+'px');
</script>
</body>
</html>
"""

# ---------------------- LEADER FINDER (pin picks, highlight, min-boost, center shows leaders & A/B boosts, swap) --------------------------
FINDER_HTML = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<title>Leader Finder — Dokkan</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root{
  --bg:#0b0f14; --card:#101720; --ink:#e9f2ff; --muted:#9fb0c7; --accent:#5bd1ff; --chip:#182330; --chipb:#223144; --slot:#0e1520;
}
:root[data-theme="light"]{
  --bg:#f7faff; --card:#ffffff; --ink:#0b1623; --muted:#3e526d; --accent:#0d7bd0; --chip:#eef3fb; --chipb:#d9e6f8; --slot:#f2f6ff;
}
*{box-sizing:border-box}
body{
  margin:0; background:
    radial-gradient(1200px 800px at 15% -10%, rgba(91,209,255,.10), transparent),
    radial-gradient(1000px 600px at 100% 0, rgba(155,255,221,.08), transparent),
    linear-gradient(180deg, #09101a, var(--bg));
  color:var(--ink); font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial;
}
a{color:var(--accent); text-decoration:none}
.wrap{max-width:1500px; margin:0 auto; padding:24px 16px 64px}

.header{display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap}
.header h1{font-size:28px; margin:0}
.btn{font-size:12px; padding:8px 12px; background:#0f151d; border:1px solid #1e2a3a; border-radius:10px; color:#fff; cursor:pointer}
.btn:hover{ border-color:#38506f }
:root[data-theme="light"] .btn{ background:#fff; color:#0b1623; border-color:#c7d5ea }

.cols{display:grid; grid-template-columns: 1fr 1.2fr; gap:16px}
@media (max-width: 1200px){ .cols{ grid-template-columns:1fr } }

.panel{background:var(--card); border:1px solid #1e2a3a; border-radius:12px; padding:12px}
:root[data-theme="light"] .panel{ border-color:#dbe7ff }
.h2{margin:0 0 8px; font-size:16px}
.controls{display:flex; gap:8px; align-items:center; flex-wrap:wrap}
.input, .select{background:#0f151d; color:#e9f2ff; border:1px solid #233246; border-radius:10px; padding:8px 10px}
:root[data-theme="light"] .input, :root[data-theme="light"] .select{ background:#fff; color:#0b1623; border-color:#c7d5ea }
.select{cursor:pointer}
.switch{display:flex; gap:6px; align-items:center; font-size:12px; opacity:.9}

.grid{display:grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap:12px; margin-top:10px}
.card{background:linear-gradient(180deg, #0f151d, #0b1016); border:1px solid #1b2636; border-radius:14px; overflow:hidden; position:relative; transition:.12s}
:root[data-theme="light"] .card{ background:#fff; border-color:#dbe7ff }
.card .img{background:#0c121a}
:root[data-theme="light"] .card .img{ background:#eef3fb }
.card .img img{width:100%; height:auto; display:block}
.card .body{padding:10px}
.pills{display:flex; gap:6px; flex-wrap:wrap}
.pill{font-size:11px; background:#0b1118; padding:4px 8px; border:1px solid #1c2838; border-radius:999px; color:#cfe2ff}
:root[data-theme="light"] .pill{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.ctitle{font-weight:700; margin:4px 0 6px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis}
.chips{display:flex; gap:6px; flex-wrap:wrap}
.chip{background:#0b1118; border:1px solid #1f2b3b; color:#d2e4ff; font-size:12px; padding:5px 9px; border-radius:999px}
:root[data-theme="light"] .chip{ background:#eef3fb; border-color:#d9e6f8; color:#0b1623 }
.cactions{display:flex; gap:6px; flex-wrap:wrap}
.button{font-size:12px; padding:6px 10px; background:#0f151d; border:1px solid #1e2a3a; border-radius:8px; color:#fff; cursor:pointer}
.button:hover{border-color:#38506f}
:root[data-theme="light"] .button{ background:#fff; color:#0b1623; border-color:#c7d5ea }

.card.sel{ outline:2px solid rgba(91,209,255,.55) }
.card.dim{ opacity:.35; filter:grayscale(.3) }
.badge{position:absolute; top:8px; right:8px; font-size:11px; background:#0b1118; color:#d2e4ff; border:1px solid #1f2b3b; padding:3px 7px; border-radius:999px}

.tray{display:flex; gap:8px; flex-wrap:wrap; margin-top:6px}
.tag{display:flex; gap:6px; align-items:center; padding:5px 9px; border-radius:999px; background:#0d1724; border:1px solid #223246; color:#d8e8ff; font-size:12px; max-width:280px}
:root[data-theme="light"] .tag{ background:#fff; border-color:#dbe7ff; color:#0b1623 }
.tag .x{cursor:pointer; opacity:.8}

.centerTop{display:flex; gap:10px; align-items:center; justify-content:space-between; flex-wrap:wrap}
.note{opacity:.85; font-size:12px}
.small{font-size:12px; opacity:.9}
.hr{height:1px; background:linear-gradient(90deg, transparent, #2b3d57 40%, #2b3d57 60%, transparent); margin:10px 0}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>👑 Leader Finder</h1>
    <a class="btn" href="/">← All units</a>
    <a class="btn" href="/team">🧩 Team Builder</a>
    <button class="btn" id="themeBtn">🌓 Theme</button>
  </div>

  <div class="cols">
    <!-- LEFT: pick units -->
    <div class="panel">
      <div class="h2">Pick Units (multi-select)</div>
      <div class="controls">
        <input class="input" id="qUnits" placeholder="Search name, ID, type, category…" style="flex:1; min-width:240px" />
        <label>Type
          <select class="select" id="tUnits">
            <option value="">All</option>
            <option>AGL</option><option>TEQ</option><option>INT</option><option>STR</option><option>PHY</option>
          </select>
        </label>
        <label class="switch">Favorites <input type="checkbox" id="fUnits"></label>
        <button class="btn" id="clearSel">Clear</button>
      </div>
      <div class="tray" id="selectedTray"></div>
      <div class="grid" id="unitGrid"></div>
      <div class="small" id="unitHint" style="margin-top:6px; opacity:.75">Tip: Click a card to toggle selection (up to 6). Greyed cards would result in no leaders at current settings.</div>
    </div>

    <!-- RIGHT: leaders -->
    <div class="panel">
      <div class="centerTop">
        <div class="h2" style="margin:0">Leaders covering all selected</div>
        <div class="controls">
          <label>Min Boost</label>
          <select class="select" id="minBoost">
            <option value="170" selected>170%+</option>
            <option value="200">200%+</option>
            <option value="220">220%+</option>
            <option value="0">Any</option>
          </select>
          <label class="switch"><input type="checkbox" id="includeAllTypes"> Include All-Types</label>
          <button class="btn" id="shareLink">Copy share link</button>
        </div>
      </div>

      <div class="note" id="stateNote">Select 2–6 units to begin.</div>
      <div class="grid" id="leaderGrid" style="margin-top:8px"></div>
    </div>
  </div>
</div>

<script>
const UNITS = {{ units|tojson }};
const $ = (s,root=document)=>root.querySelector(s);
const $$=(s,root=document)=>Array.from(root.querySelectorAll(s));

/* theme */
(function(){
  const root = document.documentElement;
  const saved = localStorage.getItem("dokkan.theme") || "dark";
  root.setAttribute("data-theme", saved);
  $("#themeBtn").addEventListener("click", ()=>{
    const cur = root.getAttribute("data-theme")==="dark" ? "light" : "dark";
    root.setAttribute("data-theme", cur);
    localStorage.setItem("dokkan.theme", cur);
  });
})();

/* favorites */
const isFav = u => { try{ return (JSON.parse(localStorage.getItem("dokkan.favs")||"[]")).includes(u.id); }catch{ return false; } };

/* ====== Parsing leader skills (ATK/DEF-aware) ====== */
function extractKiMax(text){
  const kis = Array.from(text.matchAll(/Ki\s*\+(\d+)/ig)).map(m=>parseInt(m[1],10));
  return kis.length ? Math.max(...kis) : null;
}
function extractAtkDefPct(segment){
  // Heuristic: take the highest % that is within ~35 chars of "ATK" or "DEF" or "ATK & DEF" or "HP, ATK & DEF"
  const cand = Array.from(segment.matchAll(/\+(\d+)%/g)).map(m=>{
    const pct = parseInt(m[1],10);
    const i = m.index;
    const win = segment.slice(Math.max(0,i-40), i+40).toUpperCase();
    const hasAtkDef = /ATK\s*&\s*DEF|ATK\s*,?\s*DEF|ATK|DEF/.test(win) || /HP\s*,\s*ATK\s*&\s*DEF/.test(win);
    return {pct, ok: hasAtkDef};
  }).filter(x=>x.ok).map(x=>x.pct);
  return cand.length ? Math.max(...cand) : null;
}
function parseLeaderSkill(textRaw){
  const out = {all_types:false, ki:null, primary:[], secondary:[], main_atkdef:null, add_atkdef:null, secondary_total_atkdef:null};
  if(!textRaw) return out;
  const t = textRaw.replace(/Key/g,"Ki").replace(/\s+/g,' ').trim();
  out.all_types = /all types?/i.test(t);
  out.ki = extractKiMax(t);

  // Split main vs "plus an additional ..."
  const idx = t.toLowerCase().indexOf("plus an additional");
  const head = idx>=0 ? t.slice(0,idx) : t;
  const tail = idx>=0 ? t.slice(idx) : "";

  out.main_atkdef = extractAtkDefPct(head);
  out.primary = Array.from(head.matchAll(/"([^"]+)"/g)).map(m=>m[1]);

  if(tail){
    out.secondary = Array.from(tail.matchAll(/"([^"]+)"/g)).map(m=>m[1]);
    out.add_atkdef = extractAtkDefPct(tail);
    if(out.main_atkdef!=null && out.add_atkdef!=null){
      out.secondary_total_atkdef = out.main_atkdef + out.add_atkdef;
    }
  }
  return out;
}

/* Compute unit's ATK/DEF boost under leader */
function atkdefBoostForUnit(parsedLeader, unit){
  if(!parsedLeader) return 0;
  if(parsedLeader.all_types){
    return parsedLeader.main_atkdef || 0;
  }
  const cats = new Set(unit.categories||[]);
  const hitPrimary = (parsedLeader.primary||[]).some(c=>cats.has(c));
  if(!hitPrimary) return 0;
  let total = parsedLeader.main_atkdef || 0;
  if(parsedLeader.secondary && parsedLeader.add_atkdef){
    const hitSec = parsedLeader.secondary.some(c=>cats.has(c));
    if(hitSec){
      total = Math.max(total, parsedLeader.secondary_total_atkdef || total);
    }
  }
  return total;
}

/* Pre-parse all leaders once */
const PARSED = {};
UNITS.forEach(u=>{
  PARSED[u.id] = parseLeaderSkill(u.leader_skill || "");
});

/* ====== UI State ====== */
const qUnits=$("#qUnits"), tUnits=$("#tUnits"), fUnits=$("#fUnits");
const unitGrid=$("#unitGrid"), selectedTray=$("#selectedTray"), unitHint=$("#unitHint");
const minBoostSel=$("#minBoost"), includeAllTypes=$("#includeAllTypes");
const leaderGrid=$("#leaderGrid"), stateNote=$("#stateNote"), shareLink=$("#shareLink");
const clearSel=$("#clearSel");
const MAX_SELECTED = 5;
let selected = []; // array of unit IDs

/* Helpers */
function idToUnit(id){ return UNITS.find(u=>u.id===id) || null; }
function dedupe(arr){ return Array.from(new Set(arr)); }
function selectionValid(){ return selected.length>=2; }

/* Unit library render + click-to-select */
function filterUnits(){
  const q = (qUnits.value||"").toLowerCase().trim();
  const t = tUnits.value;
  const fav = fUnits.checked;
  return UNITS.filter(u=>{
    const okT = t ? (u.type===t) : true;
    const okF = fav ? isFav(u) : true;
    let okQ = true;
    if(q){
      const cats = (u.categories||[]).join(" ").toLowerCase();
      okQ = u.name.toLowerCase().includes(q) || u.id.includes(q) || (u.type||"").toLowerCase().includes(q) || cats.includes(q);
    }
    return okT && okF && okQ;
  }).sort((a,b)=> a.rarity_rank - b.rarity_rank || a.name.localeCompare(b.name));
}

function leadersForSelection(ids){
  const min = parseInt(minBoostSel.value,10)||0;
  const includeAT = includeAllTypes.checked;
  const cand = UNITS.filter(u=>{
    if(!(u.leader_skill||"").trim()) return false;
    if(!includeAT && PARSED[u.id]?.all_types) return false;
    return true;
  });
  const ok = [];
  for(const L of cand){
    const parsed = PARSED[L.id];
    let allOK = true;
    const boosts = [];
    for(const id of ids){
      const u = idToUnit(id);
      const b = atkdefBoostForUnit(parsed, u);
      boosts.push(b);
      if(!(b>=min && b>0)){ allOK=false; break; }
    }
    if(allOK){
      const main = parsed.main_atkdef || 0;
      const ki = parsed.ki || 0;
      const minAcross = boosts.length? Math.min(...boosts) : 0;
      ok.push({L, main, ki, boosts, minAcross});
    }
  }
  ok.sort((a,b)=>
    b.minAcross - a.minAcross || b.main - a.main || (a.L.rarity_rank - b.L.rarity_rank) || a.L.name.localeCompare(b.L.name)
  );
  return ok;
}

function renderUnitGrid(){
  const list = filterUnits();
  const min = parseInt(minBoostSel.value,10)||0;
  const includeAT = includeAllTypes.checked;
  const current = new Set(selected);

  // Pre-compute “would break results” dimming
  let currentLeaders = selectionValid()? leadersForSelection(selected) : null;
  unitGrid.innerHTML = "";
  list.slice(0,200).forEach(u=>{
    const el = document.createElement("article");
    el.className = "card";
    el.dataset.id = u.id;
    if(current.has(u.id)) el.classList.add("sel");

    // If adding this unit makes leaders empty, dim it
    if(selectionValid()){
      const nextSel = current.has(u.id) ? selected.filter(x=>x!==u.id) : dedupe([...selected, u.id]).slice(0,MAX_SELECTED);
      const bad = nextSel.length>=2 && leadersForSelection(nextSel).length===0;
      if(!current.has(u.id) && bad) el.classList.add("dim");
    }

    const rarity = u.rarity || '';
    el.innerHTML = `
      <div class="img"><img src="${u.img || '/assets/dokkaninfo.com/images/dokkan-info-logo.png'}" alt=""></div>
      <div class="body">
        <div class="pills"><span class="pill">${u.type||''}</span><span class="pill">${rarity}</span></div>
        <div class="ctitle" title="${u.name}">${u.name}</div>
      </div>
      ${current.has(u.id) ? '<div class="badge">Selected</div>' : ''}
    `;
    unitGrid.appendChild(el);
  });
}

function renderSelectedTray(){
  selectedTray.innerHTML = selected.map(id=>{
    const u = idToUnit(id);
    return `<span class="tag" data-id="${u.id}" title="${u.name}">
      <span style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${u.name}</span>
      <span class="x" title="Remove">✕</span>
    </span>`;
  }).join("");
}

/* Leaders grid (clean middle) */
function renderLeaders(){
  if(!selectionValid()){
    stateNote.textContent = selected.length? "Pick at least one more unit." : "Select 2–6 units to begin.";
    leaderGrid.innerHTML = "";
    return;
  }
  const picks = leadersForSelection(selected);
  stateNote.textContent = picks.length ? `Showing ${picks.length} leaders` : "No leaders match. Adjust min boost or include All-Types.";
  leaderGrid.innerHTML = "";

  picks.slice(0,150).forEach(x=>{
    const u = x.L;
    const chips = [
      `<span class="pill">ATK/DEF ${x.main || 0}%</span>`,
      x.ki ? `<span class="pill">Ki +${x.ki}</span>` : "",
      PARSED[u.id]?.all_types ? `<span class="pill">All-Types</span>` : ""
    ].filter(Boolean).join("");

    // build team param (best effort)
    const teamParam = selected.filter(id=>id!==u.id).join(",");

    const el = document.createElement("article");
    el.className="card";
    el.innerHTML = `
      <div class="img"><img src="${u.img || '/assets/dokkaninfo.com/images/dokkan-info-logo.png'}" alt=""></div>
      <div class="body">
        <div class="pills">${chips}</div>
        <div class="ctitle" title="${u.name}">${u.name}</div>
        <div class="chips" style="margin-top:6px"><span class="chip">Min across picks: ${x.minAcross}%</span><span class="chip">${selected.length} / ${selected.length} covered</span></div>
        <div class="cactions" style="margin-top:6px">
          <a class="button" href="/unit/${u.id}" target="_blank">Details ↗</a>
          <a class="button" href="/team?leader=${u.id}&min=${parseInt(minBoostSel.value,10)||0}${teamParam?`&team=${teamParam}`:''}" target="_blank">Use as Leader ↗</a>
        </div>
      </div>`;
    leaderGrid.appendChild(el);
  });
}

/* Event delegation — stable, no once:true */
unitGrid.addEventListener("click", (e)=>{
  const card = e.target.closest(".card");
  if(!card) return;
  const id = card.dataset.id;
  if(selected.includes(id)){
    selected = selected.filter(x=>x!==id);
  }else{
    if(selected.length>=MAX_SELECTED){ return; }
    selected = dedupe([...selected, id]);
  }
  renderSelectedTray();
  renderUnitGrid();
  renderLeaders();
  syncShare();
});

selectedTray.addEventListener("click", (e)=>{
  const t = e.target.closest(".tag .x");
  if(!t) return;
  const id = t.parentElement.dataset.id;
  selected = selected.filter(x=>x!==id);
  renderSelectedTray();
  renderUnitGrid();
  renderLeaders();
  syncShare();
});

/* filters */
[qUnits, tUnits, fUnits].forEach(el=> el.addEventListener("input", ()=>{ renderUnitGrid(); }));
[minBoostSel, includeAllTypes].forEach(el=> el.addEventListener("change", ()=>{
  renderUnitGrid();
  renderLeaders();
  syncShare();
}));
clearSel.addEventListener("click", ()=>{
  selected = [];
  renderSelectedTray();
  renderUnitGrid();
  renderLeaders();
  syncShare();
});

/* share */
function syncShare(){
  const p = new URLSearchParams();
  if(selected.length) p.set("sel", selected.join(","));
  const min = parseInt(minBoostSel.value,10)||0;
  if(min) p.set("min", String(min));
  if(includeAllTypes.checked) p.set("alltypes","1");
  history.replaceState(null, "", "?"+p.toString());
}
$("#shareLink").addEventListener("click", ()=>{
  syncShare();
  navigator.clipboard?.writeText(location.href);
  shareLink.textContent="Link copied!";
  setTimeout(()=> shareLink.textContent="Copy share link", 1200);
});

/* init + restore */
(function restore(){
  const p = new URLSearchParams(location.search);
  const sel = (p.get("sel")||"").split(",").map(s=>s.trim()).filter(Boolean);
  const min = parseInt(p.get("min")||"170",10)||170;
  const all = p.get("alltypes")==="1";
  selected = sel.filter(idToUnit).slice(0,MAX_SELECTED);
  minBoostSel.value = String(min);
  includeAllTypes.checked = !!all;
})();
renderSelectedTray();
renderUnitGrid();
renderLeaders();
</script>
</body>
</html>
"""

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Use env flag in real deployments; debug True for local dev
    app.run(debug=True)
