import json
import re
from pathlib import Path
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, abort, jsonify, render_template, request, send_file

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

# NEW: map rarity to stage index; we’ll pick art by sorting all linked card_ids
RARITY_STAGE_INDEX = {"SSR": 0, "UR": 1, "LR": 2}

def _int_card_id(x: Any) -> int:
    try:
        return int(str(x))
    except Exception:
        return -1

def choose_variant_art(variant: Dict) -> Dict[str, Optional[str]]:
    """
    Choose art by sorting all linked card IDs instead of relying on ±1 patterns.
    If a variant has multiple 'card_art' entries, we sort by card_id ascending
    and pick the index that matches the variant rarity (SSR=0, UR=1, LR=2),
    clamping to the available range.
    """
    idx = variant.get("assets_index") or {}
    v_id = variant.get("form_id") or variant.get("unit_id")

    card_art = [it for it in (idx.get("card_art") or []) if isinstance(it, dict)]
    by_sub: Dict[str, List[Dict]] = {"full_card": [], "character": []}
    for it in card_art:
        st = it.get("subtype")
        if st in by_sub:
            # normalize + keep int card_id for robust sort (IDs can end with 50/60/70 etc.)
            it["_cid_int"] = _int_card_id(it.get("card_id"))
            by_sub[st].append(it)

    # sort linked IDs
    for k in by_sub:
        by_sub[k].sort(key=lambda it: (it["_cid_int"], str(it.get("card_id") or "")))

    # which stage do we want?
    rarity = (variant.get("rarity") or "").upper()
    stage_idx = RARITY_STAGE_INDEX.get(rarity, 0)

    def pick(subtype: str) -> Optional[str]:
        lst = by_sub.get(subtype) or []
        if not lst:
            return None
        i = min(stage_idx, len(lst) - 1)  # clamp
        return norm_rel(lst[i].get("path") or "")

    full = pick("full_card")
    char = pick("character")

    # fallbacks: sniff variant.assets if index didn't provide
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

        "links": links[:8],
        "categories": cats[:12],
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
        "links": links,      # include links for synergy
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
# Folding helpers (awakening graph)
# --------------------------------------------------------------------------------------
def _variant_to_ids(meta: Dict) -> List[str]:
    """Collect all awakening 'to_ids' declared on any variant of this unit."""
    out: List[str] = []
    for v in (meta.get("variants") or []):
        aw = v.get("awakening") or {}
        for tid in (aw.get("to_ids") or []):
            s = str(tid)
            if s:
                out.append(s)
    # de-dup but keep order
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

def _rarity_rank_for_meta(meta: Dict) -> int:
    """
    Rank for choosing the 'best' node in a chain: LR > UR > SSR.
    Uses the rarity of the variant you'd normally display for this meta.
    """
    variants = meta.get("variants") or []
    chosen = best_variant_for_display(variants)
    r = (chosen.get("rarity") or meta.get("rarity") or "").upper()
    return {"SSR": 1, "UR": 2, "LR": 3}.get(r, 0)

def _release_ts_for_meta(meta: Dict) -> int:
    """Millisecond timestamp of the chosen display variant's release date for tie-breaking."""
    variants = meta.get("variants") or []
    chosen = best_variant_for_display(variants)
    dt = parse_dt(chosen.get("release_date"))
    return int(dt.timestamp() * 1000) if dt else -1

def _build_awaken_graph(metas: List[Dict]) -> Tuple[Dict[str, Dict], Dict[str, List[str]]]:
    """
    Returns:
        by_id: unit_id -> meta
        graph: unit_id -> list of to_ids (only those present locally)
    """
    by_id = {str(m.get("unit_id")): m for m in metas}
    graph: Dict[str, List[str]] = {}
    local_ids = set(by_id.keys())
    for uid, m in by_id.items():
        to_ids = [t for t in _variant_to_ids(m) if t in local_ids]
        graph[uid] = to_ids
    return by_id, graph

def _best_reachable_id(start_id: str, by_id: Dict[str, Dict], graph: Dict[str, List[str]], cache: Dict[str, str]) -> str:
    """
    Walk outward through awakening edges and return the single 'best' id
    reachable from start_id (including itself), where best is chosen by:
      1) highest rarity rank (LR > UR > SSR),
      2) if tied, later release date,
      3) if still tied, numerically larger unit id.
    """
    if start_id in cache:
        return cache[start_id]

    best = start_id
    best_key = (_rarity_rank_for_meta(by_id[best]), _release_ts_for_meta(by_id[best]), int(best))

    stack = [start_id]
    seen = set([start_id])

    while stack:
        cur = stack.pop()
        # evaluate candidate
        cur_key = (_rarity_rank_for_meta(by_id[cur]), _release_ts_for_meta(by_id[cur]), int(cur))
        if cur_key > best_key:
            best, best_key = cur, cur_key

        # expand
        for nxt in graph.get(cur, []):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)

    cache[start_id] = best
    return best

def pick_chain_best_id(meta: Dict, by_id: Dict[str, Dict], graph: Dict[str, List[str]], cache: Dict[str, str]) -> str:
    """
    For this unit's awakening chain, pick the best id present locally
    by exploring the graph from this unit.
    """
    uid = str(meta.get("unit_id") or "")
    return _best_reachable_id(uid, by_id, graph, cache)

def filter_to_max_awakened(metas: List[Dict]) -> List[Dict]:
    """
    Deduplicate chains so we keep just one meta per awakening chain:
    the highest-rarity (then latest) unit present according to the
    metadata 'to_ids' graph.
    """
    if not metas:
        return []

    by_id, graph = _build_awaken_graph(metas)
    cache: Dict[str, str] = {}

    keep_ids = set(pick_chain_best_id(m, by_id, graph, cache) for m in metas)
    kept = [by_id[i] for i in keep_ids if i in by_id]

    # sort newest first (consistent with the rest of the app)
    def sort_key(meta):
        variants = meta.get("variants") or []
        chosen = best_variant_for_display(variants)
        dt = parse_dt(chosen.get("release_date"))
        return dt or datetime.fromtimestamp(0)

    kept.sort(key=sort_key, reverse=True)
    return kept

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

def _iter_categories_detailed(meta: Dict):
    """
    Yield all category-detailed records from anywhere they may appear:
    - variant.categories_detailed (expected)
    - variant.kit.categories_detailed (seen in some dumps)
    - meta.categories_detailed (rare)
    """
    # meta-level (rare)
    for cd in (meta.get("categories_detailed") or []):
        if isinstance(cd, dict):
            yield cd

    for v in (meta.get("variants") or []):
        # variant-level (expected)
        for cd in (v.get("categories_detailed") or []):
            if isinstance(cd, dict):
                yield cd
        # occasionally nested under kit (seen in the wild)
        kit = v.get("kit") or {}
        for cd in (kit.get("categories_detailed") or []):
            if isinstance(cd, dict):
                yield cd


def build_category_assets(metas) -> dict:
    """
    Map: Category Name -> /assets/<relative icon path>.
    Prefers 'en' locale but falls back to whatever exists.
    Normalizes backslashes.
    """
    out, pref = {}, {}
    for meta in metas or []:
        for cd in _iter_categories_detailed(meta):
            name = cd.get("name")
            rel  = cd.get("asset_rel")
            if not name or not rel:
                continue
            rel = norm_rel(rel)  # "dokkaninfo.com/assets/..."
            url = f"/assets/{rel}"
            # prefer en, else anything
            score = 2 if (cd.get("locale") or "").lower() == "en" else 1
            if name not in out or score > pref.get(name, 0):
                out[name] = url
                pref[name] = score
    print(f"[cat-assets] mapped {len(out)} category icons")
    return out

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@app.route("/")
def home():
    clear_cache_if_requested()
    metas_all = load_all_units()
    cat_assets = build_category_assets(metas_all)  # <-- robust now; also logs size

    metas = filter_to_max_awakened(metas_all)
    cards = [to_unit_summary(m) for m in metas]
    top_cats, top_links = compute_facets(cards)
    return render_template(
        "index.html",
        cards=cards,
        total=len(cards),
        top_cats=top_cats,
        top_links=top_links,
        cat_assets=cat_assets,
    )

@app.route("/unit/<unit_id>")
def unit_detail(unit_id: str):
    clear_cache_if_requested()
    metas = load_all_units()
    meta = next((m for m in metas if str(m.get("unit_id")) == str(unit_id)), None)
    if not meta:
        abort(404)
    detail = to_unit_detail(meta)
    return render_template("detail.html", u=detail)

@app.route("/team")
def team_builder():
    clear_cache_if_requested()
    metas = load_all_units()
    light = [to_light_unit(m) for m in metas]
    return render_template("team.html", units=light)

@app.route("/finder")
def leader_finder():
    clear_cache_if_requested()
    metas = load_all_units()
    light = [to_light_unit(m) for m in metas]
    return render_template("finder.html", units=light)

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
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # debug True for local dev
    app.run(debug=True)
