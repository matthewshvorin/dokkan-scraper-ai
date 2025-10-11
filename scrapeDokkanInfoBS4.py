# scrapeDokkanInfo_play_bs4_eza_dropdown_singlefolder.py
# Playwright (headed) + BeautifulSoup
# Single-folder-per-family with variants[] aggregation (base + EZA steps + ALL transformations/variations)
#
# Output per family (folder: "[RARITY] [TYPE] [Display] - <baseId>/METADATA.json"):
# {
#   "unit_id": "<baseId>",            # canonical family id (base form's id)
#   "form_id": "<baseId>",
#   "display_name": "...",            # base form display name
#   "rarity": "UR|LR|...",
#   "type": "AGL|TEQ|INT|STR|PHY",
#   "source_base_url": "https://dokkaninfo.com/cards/<baseId>",
#   "variants": [
#       {
#         "key": "base" | "eza_step_1" | ... | "form_<id>_base" | "form_<id>_eza_step_<n>",
#         "variant_label": "[Boiling Power] Super Saiyan 2 Goku — Base" | "... — EZA Step 3" | "... (#4014770) — Base",
#         "form_id": "4014770",
#         "display_name": "[Boiling Power] Super Saiyan 2 Goku",
#         "rarity": "UR",
#         "type": "AGL",
#         "eza": false|true,
#         "step": null|int,
#         "is_super_eza": bool,
#         "source_url": "final navigated URL",
#         "release_date": "...", "timezone": "...", "eza_release_date": "...",
#         "obtain_type": "Summonable|...",
#         "kit": {
#           "leader_skill": "...",
#           "super_attack": {"name": "...", "effect": "..."},
#           "ultra_super_attack": {"name": "...", "effect": "..."},
#           "passive_skill": {"name": "...", "effect": "...", "lines": [...]},
#           "transformation": {"can_transform": bool, "condition": str|null},
#           "reversible_exchange": {"can_exchange": bool, "condition": str|null},
#           "transformation_conditions": str|null,
#           "active_skill": {"name": "...", "effect": "...", "activation_conditions": "..."},
#           "standby_skill": {...}|null,
#           "finish_skills": [...],
#           "link_skills": [...],
#           "categories": [...],
#           "stats": {...},
#           "domains": [...]
#         },
#         "assets": ["dokkaninfo.com\\...png", ...],
#         "assets_index": {  # NEW per-variant
#            "<category>": [
#               {"path": "dokkaninfo.com\\...png", "subtype":"...", "card_id":"...", "locale":"en", "note":"..."},
#               ...
#            ],
#            ...
#         }
#       }
#   ],
#   "assets": ["union of assets across all variants (base + eza + transformations)"],
#   "assets_index": { ... union across variants ... }  # NEW top-level
# }
#
# Also updates output/cards/CARDS_INDEX.json:
#   {
#     "<cardId>": {
#       "folder": "...", "display_name": "...", "rarity": "...", "type": "...",
#       "variants": ["base","eza_step_1",...,"form_<id>_base",...]
#     }
#   }
#
# Usage:
#   python scrapeDokkanInfo_play_bs4_eza_dropdown_singlefolder.py

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------ Config -------------
BASE = "https://dokkaninfo.com"
INDEX_URL = f"{BASE}/cards?sort=open_at_eza"   # includes EZAs

OUTROOT = Path("output/cards")
ASSETS_ROOT = Path("output/assets")
INDEX_PATH = OUTROOT / "CARDS_INDEX.json"
LOGDIR = Path("output/logs")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_HEADERS = {"User-Agent": USER_AGENT, "Referer": BASE}
CATEGORIES_INDEX_PATH = OUTROOT / "CATEGORIES_INDEX.json"

TIMEOUT = 60_000
SLEEP_BETWEEN_CARDS = 0
MAX_PAGES = 200
MAX_NEW_CARDS = 200     # limit how many BASE families to save if COUNT_MODE="bases"; if "total", counts forms incl. transformations
COUNT_MODE = "bases"    # "bases" or "total"

MAX_FAMILY_SIZE = 40    # safety cap for BFS across transformations/variations

# NEW: Skip already-scraped families entirely (uses CARDS_INDEX.json and on-disk folders)
SKIP_EXISTING = True
STORE_ASSETS_LIST = False                      # drop top-level and per-variant "assets"
KEEP_ASSET_CATEGORIES = {"card_art", "thumbnail"}
KEEP_ASSET_LOCALES = {"en"}
# ---- Seed test ----
SEED_URLS: List[str] = [
 #"https://dokkaninfo.com/cards/1010441",
]

HEADERS = [
    "Leader Skill",
    "Super Attack",
    "Ultra Super Attack",
    "Passive Skill",
    "Active Skill",
    "Activation Condition(s)",
    "Transformation Condition(s)",
    "Link Skills",
    "Categories",
    "Stats",
]

CATEGORY_BLACKLIST_TOKENS = {
    "background", "icon", "rarity", "element", "eza", "undefined",
    "venatus", "show more", "links", "categories",
}
EXT_FILE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp)$", re.IGNORECASE)

CARD_ID_IN_HREF_RE = re.compile(r"/cards/(\d+)")
CARD_ID_IN_SRC_RE = re.compile(r"card_(\d+)_", re.IGNORECASE)

TYPE_SET = {"str", "teq", "int", "agl", "phy"}
RARITY_RANK = {"N":0, "R":1, "SR":2, "SSR":3, "UR":4, "LR":5}

AWAKEN_ROW_SEL = "div.row.d-flex.flex-wrap.border.border-1.card-icon"
CAT_ID_IN_HREF = re.compile(r"/categories/(\d+)$")

def parse_categories_detailed(soup: BeautifulSoup, page_url: str) -> List[Dict[str, Optional[str]]]:
    """
    Returns: [{"id":"50","name":"Inhuman Deeds","asset_rel":"dokkaninfo.com/...png","locale":"en"}, ...]
    """
    items: List[Dict[str, Optional[str]]] = []
    for a in soup.select('a[href^="/categories/"]'):
        href = a.get("href") or ""
        m = CAT_ID_IN_HREF.search(href)
        if not m:
            continue
        cid = m.group(1)
        im = a.find("img")
        if not im:
            continue
        name = (im.get("alt") or im.get("title") or "").strip()
        src = im.get("src") or ""
        absu = urljoin(page_url, src)
        relp = _url_to_asset_rel(absu)
        rels = str(relp) if relp else None
        loc = _extract_locale_from_rel(rels) if rels else None
        items.append({"id": cid, "name": name, "asset_rel": rels, "locale": loc})
    # de-dup per (id, locale, path)
    seen = set(); out = []
    for it in items:
        key = (it.get("id"), it.get("locale"), it.get("asset_rel"))
        if key in seen:
            continue
        seen.add(key); out.append(it)
    return out

def load_category_index() -> Dict[str, dict]:
    if CATEGORIES_INDEX_PATH.exists():
        try:
            return json.loads(CATEGORIES_INDEX_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_category_index(index: Dict[str, dict]) -> None:
    CATEGORIES_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    CATEGORIES_INDEX_PATH.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

def _index_add_category_item(idx: Dict[str, dict], item: Dict[str, Optional[str]]) -> None:
    """
    item = {"id": "50", "name": "Inhuman Deeds", "asset_rel": "dokkaninfo.com/...png", "locale": "en"}
    """
    cid = str(item.get("id") or "").strip()
    if not cid:
        return
    name = (item.get("name") or "").strip()
    rel  = item.get("asset_rel") or None
    loc  = (item.get("locale") or "en").lower()

    node = idx.get(cid) or {"id": cid, "labels": {}, "assets": []}
    if name:
        node["labels"][loc] = name

    if rel and rel not in {a.get("path") for a in node["assets"]}:
        node["assets"].append({"path": rel, "locale": loc})

    # Optional slug for pretty URLs
    if "slug" not in node and name:
        node["slug"] = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    idx[cid] = node

def _collect_card_ids_in_row(row: Tag) -> list[str]:
    ids = []
    for a in row.select("a.card-icon[href]"):
        href = a.get("href") or ""
        m = CARD_ID_IN_HREF_RE.search(href)
        if m:
            ids.append(m.group(1))
    # Keep order but unique
    out, seen = [], set()
    for i in ids:
        if i not in seen:
            seen.add(i); out.append(i)
    return out

def parse_awaken_links_from_soup(soup: BeautifulSoup, rarity_hint: Optional[str]) -> dict:
    """Return {'from': [...], 'to': [...]} using headings when present; fallback by rarity."""
    res = {"from": [], "to": []}
    rows = soup.select(AWAKEN_ROW_SEL)
    if not rows:
        return res

    def nearby_heading_text(row: Tag) -> str:
        prev = row.find_previous_sibling("div")
        txt = (prev.get_text(" ", strip=True).lower() if prev else "")
        return txt

    for row in rows:
        ids = _collect_card_ids_in_row(row)
        if not ids:
            continue
        label = nearby_heading_text(row)
        if "awakened from" in label:
            res["from"].extend(ids); continue
        if "awakens to" in label or "dokkan awaken" in label:
            res["to"].extend(ids); continue

        # Fallback heuristic: LR pages almost always show the "from" strip only.
        if (rarity_hint or "").upper() == "LR":
            res["from"].extend(ids)
        else:
            # SSR-only pages sometimes show the "to" strip only.
            res["to"].extend(ids)

    # de-dupe
    res["from"] = list(dict.fromkeys(res["from"]))
    res["to"] = list(dict.fromkeys(res["to"]))
    return res

def _rarity_rank(r: Optional[str]) -> int:
    return RARITY_RANK.get((r or "").upper(), -1)
# ------------ Logging -------------
def setup_logging() -> Path:
    LOGDIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = LOGDIR / f"run-{stamp}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.info("Logging to %s", log_path)
    return log_path

# ------------ Index helpers -------------
def load_index() -> Dict[str, dict]:
    if INDEX_PATH.exists():
        try:
            return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            logging.warning("Failed to read index (%s). Starting fresh.", e)
    return {}

def save_index(index: Dict[str, dict]) -> None:
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

def index_add_variant(index: Dict[str, dict],
                      char_id: str,
                      folder: Path,
                      display_name: str,
                      rarity: Optional[str],
                      type_token_upper: Optional[str],
                      variant_key: str) -> None:
    node = index.get(char_id)
    if not node:
        node = index[char_id] = {
            "folder": str(folder),
            "display_name": display_name,
            "rarity": rarity,
            "type": type_token_upper,
            "variants": [],
            "saved_at": datetime.utcnow().isoformat() + "Z",
        }
    else:
        # Always keep this form-id’s identity current
        node["folder"] = str(folder)
        if display_name:
            node["display_name"] = display_name
        if rarity:
            node["rarity"] = rarity
        if type_token_upper:
            node["type"] = type_token_upper

    if variant_key not in node["variants"]:
        node["variants"].append(variant_key)

    save_index(index)

# ------------ NEW: existing detection / skipping -------------
EXISTING_ID_FROM_FOLDER_RE = re.compile(r"-\s*(\d+)$")

def _parse_unit_id_from_folder_name(name: str) -> Optional[str]:
    m = EXISTING_ID_FROM_FOLDER_RE.search(name)
    return m.group(1) if m else None

def collect_existing_unit_ids(outroot: Path, index: Dict[str, dict]) -> Set[str]:
    existing: Set[str] = set()
    # From index (authoritative if present)
    existing.update([k for k in (index or {}).keys()])
    # From disk folders
    if outroot.exists():
        for child in outroot.iterdir():
            if not child.is_dir():
                continue
            # Try METADATA.json first
            meta = child / "METADATA.json"
            cid: Optional[str] = None
            if meta.exists():
                try:
                    data = json.loads(meta.read_text(encoding="utf-8"))
                    cid_val = data.get("unit_id") or data.get("form_id")
                    if cid_val:
                        cid = str(cid_val)
                except Exception:
                    cid = None
            if not cid:
                cid = _parse_unit_id_from_folder_name(child.name)
            if cid:
                existing.add(cid)
    return existing

# ------------ Helpers -------------
def sanitize_filename(name: str) -> str:
    name = (
        name.replace(":", " -")
        .replace("/", "-")
        .replace("\\", "-")
        .replace("|", "-")
        .replace("*", "x")
        .replace("?", "")
        .replace('"', "'")
        .strip()
    )
    name = re.sub(r"\s+", " ", name)
    return name.rstrip(" .")

def extract_character_id_from_url(url: str) -> Optional[str]:
    m = CARD_ID_IN_HREF_RE.search(url)
    return m.group(1) if m else None

def extract_ids_from_col5_images(page_html: str) -> List[str]:
    soup = BeautifulSoup(page_html, "lxml")
    required = {"row", "cursor-pointer", "unselectable", "border", "border-2", "border-dark", "margin-top-bottom-5"}
    header_div = None
    for div in soup.find_all("div"):
        cls = set(div.get("class") or [])
        if required.issubset(cls):
            header_div = div
            break
    if not header_div:
        return []
    tiles = header_div.find_all("div", class_=lambda v: v and "col-5" in v.split())
    if not tiles:
        return []
    ids: List[str] = []
    seen: Set[str] = set()
    for sub in tiles[1:]:
        # Try by link first
        a = sub.find("a", href=CARD_ID_IN_HREF_RE)
        if a:
            href = a.get("href") or ""
            mid = CARD_ID_IN_HREF_RE.search(href)
            if mid:
                cid = mid.group(1)
                if cid not in seen:
                    seen.add(cid)
                    ids.append(cid)
                continue
        img = sub.find("img")
        if not img:
            continue
        src = img.get("src") or ""
        m = CARD_ID_IN_SRC_RE.search(src)
        if m:
            cid = m.group(1)
            if cid not in seen:
                seen.add(cid)
                ids.append(cid)
    return ids

def build_next_index_url(curr_url: str) -> str:
    parsed = urlparse(curr_url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "page" not in q:
        q["page"] = "2"
    else:
        try:
            q["page"] = str(int(q["page"]) + 1)
        except Exception:
            q["page"] = "2"
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

# ------------ Variant helpers -------------
def parse_variant_from_url(url: str) -> Tuple[bool, Optional[int]]:
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    eza_flag = str(q.get("eza", "false")).lower() == "true"
    step = q.get("step")
    try:
        step_i = int(step) if step is not None else None
    except Exception:
        step_i = None
    return eza_flag, step_i

def super_eza_step_for_rarity(rarity: Optional[str]) -> Optional[int]:
    if not rarity:
        return None
    r = rarity.upper()
    if r == "LR":
        return 4
    if r == "UR":
        return 8
    return None

def build_variant_key(eza: bool, step: Optional[int]) -> str:
    if not eza:
        return "base"
    if step is None:
        return "eza"
    return f"eza_step_{step}"

def build_form_variant_key(form_id: str, eza: bool, step: Optional[int]) -> str:
    if not eza:
        return f"form_{form_id}_base"
    if step is None:
        return f"form_{form_id}_eza"
    return f"form_{form_id}_eza_step_{step}"

def normalize_to_base_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def make_variant_url(base_url: str, eza: bool, step: Optional[int]) -> str:
    parsed = urlparse(base_url)
    q = {}
    if eza:
        q["eza"] = "true"
        if step is not None:
            q["step"] = str(step)
    else:
        q["eza"] = "false"
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

def build_variant_label(display_name: Optional[str],
                        form_id: Optional[str],
                        family_base_id: Optional[str],
                        eza: bool,
                        step: Optional[int]) -> str:
    dn = display_name or "Unknown"
    part = "Base" if not eza else (f"EZA Step {step}" if step is not None else "EZA")
    if family_base_id is None or (form_id and form_id == family_base_id):
        return f"{dn} — {part}"
    # Non-base form: include id so labels stay unique across same names
    return f"{dn} (#" + (form_id or "?") + f") — {part}"

# ------------ TEXT parsing -------------
def _split_sections(page_text: str) -> Dict[str, List[str]]:
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in page_text.splitlines()]
    indices: List[Tuple[str, int]] = []
    for idx, ln in enumerate(lines):
        if ln in HEADERS:
            indices.append((ln, idx))
    sections: Dict[str, List[str]] = {}
    for i, (hdr, start_i) in enumerate(indices):
        end_i = len(lines)
        if i + 1 < len(indices):
            end_i = indices[i + 1][1]
        block = [l for l in lines[start_i + 1:end_i] if l != ""]
        sections[hdr] = block
    return sections

def _condense_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _dedup_sentences(text: str) -> str:
    parts = [p.strip() for p in re.split(r'(?<=[.!?])\s+', text) if p.strip()]
    out = []
    seen = set()
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return " ".join(out)

def _text_before_after_step_scope(soup: BeautifulSoup) -> Tuple[str, str]:
    """
    Returns (base_text, eza_text):
      - base_text: concatenated visible-ish text BEFORE the first EZA step multiselect
      - eza_text:  concatenated visible-ish text AFTER  the first EZA step multiselect
    If no multiselect is found, base_text = whole page, eza_text = "".
    """
    step_node = soup.select_one("div.multiselect")
    # choose a traversal root that exists
    root = soup.body if soup.body else soup

    # If no EZA step UI is present, treat the entire page as base scope
    if not step_node:
        return (root.get_text("\n", strip=True), "")

    before_parts: List[str] = []
    after_parts: List[str] = []
    in_after = False

    # Walk the document in order and split text around the first multiselect node
    for node in root.descendants:
        if node is step_node:
            in_after = True
            continue
        if isinstance(node, NavigableString):
            txt = str(node)
            if txt and txt.strip():
                (after_parts if in_after else before_parts).append(txt)

    base_text = "\n".join(before_parts)
    eza_text = "\n".join(after_parts)
    return base_text, eza_text

def _clean_leader(block: List[str]) -> Optional[str]:
    if not block:
        return None
    leader = _condense_spaces(" ".join(block))
    leader = _dedup_sentences(leader)
    return leader or None

def _clean_super_like(block: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not block:
        return None, None
    name = block[0]
    rest = block[1:]
    eff_parts: List[str] = []
    for ln in rest:
        if not ln:
            continue
        if re.fullmatch(r"\d+\s*%$", ln):
            continue
        if re.search(r"\bSA\s*Lv\b", ln, flags=re.IGNORECASE):
            continue
        eff_parts.append(ln)
    eff = "; ".join(eff_parts)
    eff = re.sub(r"\s*;\s*", "; ", eff)
    eff = re.sub(r"\s*Raises ATK & DEF\s*Causes", " Raises ATK & DEF; Causes", eff, flags=re.IGNORECASE)
    eff = _condense_spaces(eff)
    return (name or None), (eff or None)

# ---------- Passive (DOM-driven) ----------
PASSIVE_ICON_ONCE = "passive_skill_dialog_icon_01"
PASSIVE_ICON_PERMA = "passive_skill_dialog_icon_02"
PASSIVE_ARROW_UP = "passive_skill_dialog_arrow01"
PASSIVE_ARROW_DOWN = "passive_skill_dialog_arrow02"
ENTRANCE_REGEX = re.compile(r"(activates\s+the\s+entrance\s+animation|upon\s+the\s+character[’']?s\s+entry)", re.IGNORECASE)

def _find_passive_content_div(soup: BeautifulSoup) -> Optional[Tag]:
    bnode = soup.find("b", string=re.compile(r"^\s*Passive Skill\s*$", re.IGNORECASE))
    if not bnode:
        return None
    title_row = bnode.find_parent("div", class_=re.compile(r"\brow\b"))
    if not title_row:
        return None
    content = title_row.find_next_sibling("div")
    hops = 0
    while content and hops < 6:
        cls = content.get("class") or []
        if any(c.startswith("bg-") for c in cls) or content.find("ul") or content.find("strong"):
            return content
        content = content.find_next_sibling("div")
        hops += 1
    return title_row.find_parent("div", class_=re.compile(r"\bborder\b")) or title_row

def _li_text_with_inline_markers(li: Tag) -> str:
    parts: List[str] = []
    for node in li.children:
        if isinstance(node, NavigableString):
            parts.append(str(node))
        elif isinstance(node, Tag):
            if node.name == "img":
                src = (node.get("src") or "").lower()
                if PASSIVE_ARROW_UP in src:
                    parts.append(" up")
                elif PASSIVE_ARROW_DOWN in src:
                    parts.append(" down")
                elif PASSIVE_ICON_ONCE in src or PASSIVE_ICON_PERMA in src:
                    continue
                else:
                    continue
            else:
                parts.append(node.get_text(" ", strip=False))
    return _condense_spaces("".join(parts))

def _li_icons(li: Tag) -> Tuple[bool, bool, List[str], List[str]]:
    once = False
    permanent = False
    arrows: List[str] = []
    tokens: List[str] = []
    for im in li.find_all("img"):
        src = (im.get("src") or "").lower()
        if PASSIVE_ICON_ONCE in src:
            once = True
            tokens.append(PASSIVE_ICON_ONCE)
        elif PASSIVE_ICON_PERMA in src:
            permanent = True
            tokens.append(PASSIVE_ICON_PERMA)
        elif PASSIVE_ARROW_UP in src:
            arrows.append("up")
            tokens.append(PASSIVE_ARROW_UP)
        elif PASSIVE_ARROW_DOWN in src:
            arrows.append("down")
            tokens.append(PASSIVE_ARROW_DOWN)
        else:
            m = re.search(r"/([a-z0-9_]+)\.(?:png|jpg|jpeg|gif|webp)$", src)
            if m:
                tokens.append(m.group(1))
    return once, permanent, arrows, tokens

def parse_passive_lines_from_dom(soup: BeautifulSoup) -> Tuple[List[Dict[str, object]], str]:
    content = _find_passive_content_div(soup)
    if not content:
        return [], ""

    lines: List[Dict[str, object]] = []
    current_context: Optional[str] = None
    in_basic_scope: bool = False

    for child in content.descendants:
        if isinstance(child, Tag):
            if child.name in {"strong", "b"}:
                txt = child.get_text(" ", strip=True)
                if txt:
                    if re.fullmatch(r"(?i)\s*basic effect\(s\)\s*", txt):
                        in_basic_scope = True
                        continue
                    current_context = _condense_spaces(txt)
                    in_basic_scope = False

            if child.name == "li":
                once, permanent, arrows, tokens = _li_icons(child)
                text = _li_text_with_inline_markers(child)
                if not text:
                    continue
                if not once and not permanent and in_basic_scope:
                    permanent = True
                ctx_join = f"{current_context or ''} {text}"
                if not once and ENTRANCE_REGEX.search(ctx_join):
                    once = True
                lines.append({
                    "text": text,
                    "context": current_context,
                    "once": once,
                    "permanent": permanent,
                    "arrows": arrows,
                    "icons": tokens,
                })

    parts: List[str] = []
    last_ctx = object()
    for it in lines:
        seg = it["text"]
        ctx = it.get("context")
        if ctx and ctx != last_ctx:
            parts.append(f"{ctx}: {seg}")
            last_ctx = ctx
        elif ctx and ctx == last_ctx:
            parts.append(seg)
        else:
            parts.append(seg)
    consolidated = "; ".join(parts)
    consolidated = re.sub(r"\s*;\s*", "; ", consolidated).strip()
    consolidated = re.sub(r"^\s*Basic effect\(s\):\s*", "", consolidated, flags=re.IGNORECASE)
    return lines, consolidated

def render_passive_effect_with_markers(lines: List[Dict[str, object]]) -> str:
    rendered: List[str] = []
    last_ctx: Optional[str] = None
    for it in lines:
        marker = "(Once) " if it.get("once") else "(Forever) " if it.get("permanent") else ""
        seg = f"{marker}{it.get('text') or ''}".strip()
        ctx = it.get("context")
        if ctx != last_ctx:
            if ctx:
                seg = f"{ctx}: {seg}"
            last_ctx = ctx
        rendered.append(seg)
    return re.sub(r"\s*;\s*", "; ", "; ".join(rendered)).strip()

# ---------- Passive fallback ----------

def _group_passive_lines_fallback(lines: List[str]) -> str:
    if not lines:
        return ""
    lines = [ln for ln in lines if ln not in HEADERS and not re.fullmatch(r"Basic effect\(s\):?", ln, flags=re.IGNORECASE)]
    activ_idx = next((i for i, ln in enumerate(lines) if ln.lower().startswith("activates the entrance animation")), None)
    if activ_idx is not None and activ_idx != 0:
        first = lines.pop(activ_idx)
        lines.insert(0, first)
    leading_patterns = [
        r"^Activates the Entrance Animation",
        r"^Ki \+\d",
        r"^ATK",
        r"^DEF",
        r"^Guards all attacks",
        r"^For every attack performed",
        r"^For every attack received",
        r"^Launches an additional attack",
        r"^For every Super Attack the enemy launches",
        r"^When receiving an Unarmed Super Attack",
    ]
    def is_leading(s: str) -> bool:
        return any(re.search(p, s, flags=re.IGNORECASE) for p in leading_patterns)

    groups: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if is_leading(ln) and cur:
            groups.append(cur); cur = [ln]
        else:
            if not cur: cur = [ln]
            else: cur.append(ln)
    if cur:
        groups.append(cur)
    out_parts: List[str] = []
    for g in groups:
        g = [x for x in g if x and x not in HEADERS and not re.fullmatch(r"Basic effect\(s\):?", x, flags=re.IGNORECASE)]
        if not g:
            continue
        clause = _condense_spaces(" ".join(g))
        clause = re.sub(r"^\s*Basic effect\(s\):\s*", "", clause, flags=re.IGNORECASE)
        clause = re.sub(r"^(For every [^.]+?)(?!:)\s", r"\1: ", clause, flags=re.IGNORECASE)
        out_parts.append(clause)
    effect = "; ".join(out_parts)
    effect = re.sub(r"\s*;\s*", "; ", effect).strip()
    effect = re.sub(r"^\s*Basic effect\(s\):\s*", "", effect, flags=re.IGNORECASE)
    return effect

# ------------ Active/Activation/Categories/Stats/Release -------------

def _clean_active(block: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not block:
        return None, None
    name = block[0]
    body = []
    for ln in block[1:]:
        if ln in HEADERS or re.fullmatch(r"Link Skills", ln, re.IGNORECASE):
            break
        body.append(ln)
    effect = _condense_spaces("; ".join([_condense_spaces(b) for b in body if b]))
    return (name or None), (effect or None)

def _clean_activation(block: List[str]) -> Optional[str]:
    if not block:
        return None
    text = _condense_spaces(" ".join(block))
    for h in HEADERS:
        text = text.replace(h, "")
    return text.strip() or None

def _clean_links(block: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for ln in block or []:
        s = _condense_spaces(ln)
        if not s or s in seen:
            continue
        seen.add(s); out.append(s)
    return out

def _parse_stats_textual(block: List[str], page_text: str) -> Dict[str, object]:
    stats: Dict[str, object] = {}
    m_cost = re.search(r"\bCost\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_cost: stats["Cost"] = int(m_cost.group(1))
    m_max = re.search(r"\bMax\s*Lv\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_max: stats["Max Lv"] = int(m_max.group(1))
    m_sa = re.search(r"\bSA\s*Lv\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_sa: stats["SA Lv"] = int(m_sa.group(1))
    return stats

def _parse_stats_table_dom(soup: BeautifulSoup) -> Dict[str, object]:
    out: Dict[str, object] = {}
    table = None
    for th in soup.find_all("th"):
        if th.find("b") and th.find("b").get_text(strip=True).lower() == "stats":
            tbl = th.find_parent("table")
            if tbl:
                table = tbl
                break
    if not table:
        return out
    header_row = table.find("tr")
    if not header_row:
        return out
    headers = []
    for th in header_row.find_all("th")[1:]:
        headers.append(th.get_text(strip=True))
    key_map = {
        "Base Min": "Base Min",
        "Base Max": "Base Max",
        "55%": "55%",
        "100%": "100%",
        "EZA B. Max": "EZA B. Max",
        "EZA 100%": "EZA 100%",
    }
    norm_headers = [key_map.get(h, h) for h in headers]
    for row in table.find_all("tr")[1:]:
        stat_name_th = row.find("th")
        if not stat_name_th:
            continue
        stat_name = stat_name_th.get_text(strip=True).upper()
        if stat_name not in {"HP", "ATK", "DEF"}:
            continue
        cells = [td.get_text(strip=True).replace(",", "") for td in row.find_all("td")]
        values: Dict[str, int] = {}
        for i, val in enumerate(cells):
            if i >= len(norm_headers):
                break
            hkey = norm_headers[i]
            if not val:
                continue
            try:
                values[hkey] = int(val)
            except ValueError:
                continue
        if values:
            out[stat_name] = values
    return out

def _parse_release(page_text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(
        r"Release Date\s+([0-9/.\-]+)\s+([0-9: ]+[APMapm]{2})\s+([A-Z]{2,4})",
        page_text,
        flags=re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)} {m.group(2)}", m.group(3)
    return None, None

def _parse_release_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    def grab_block(b_label: str) -> Optional[str]:
        b = soup.find("b", string=re.compile(r"^\s*{0}\s*$".format(re.escape(b_label)), re.IGNORECASE))
        if not b:
            return None
        row = b.find_parent("div", class_=re.compile(r"\brow\b"))
        if not row:
            return None
        nxt = row.find_next_sibling("div")
        hops = 0
        while nxt and hops < 3:
            text = nxt.get_text("\n", strip=True)
            if text:
                return _condense_spaces(text.replace("\n", " "))
            nxt = nxt.find_next_sibling("div")
            hops += 1
        return None

    rd_text = grab_block("Release Date")
    eza_rd_text = grab_block("EZA Release Date")

    def split_dt_tz(txt: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not txt:
            return None, None
        m = re.search(r"([0-9/.\-]+\s+[0-9: ]+[APMapm]{2})\s+([A-Z]{2,4})", txt)
        if m:
            return m.group(1), m.group(2)
        return txt, None

    rel_dt, rel_tz = split_dt_tz(rd_text)
    eza_rel_dt, _ = split_dt_tz(eza_rd_text)
    return rel_dt, rel_tz, eza_rel_dt

def parse_categories_from_soup(soup: BeautifulSoup) -> List[str]:
    cats1 = [(im.get("alt") or im.get("title") or "") for im in soup.select('a[href*="/categories/"] img')]
    cats1 = [c for c in cats1 if c]
    cats2 = [(im.get("alt") or im.get("title") or "") for im in soup.select('img[src*="/card_category/label/"]')]
    cats2 = [c for c in cats2 if c]
    cats3 = []
    cat_el: Optional[Tag] = None
    for el in soup.find_all(string=True):
        if isinstance(el, NavigableString) and str(el).strip() == "Categories":
            cat_el = el.parent if isinstance(el.parent, Tag) else None
            if cat_el:
                break
    if cat_el:
        for sib in cat_el.next_siblings:
            if isinstance(sib, NavigableString):
                txt = str(sib).strip()
                if txt in HEADERS:
                    break
                continue
            if isinstance(sib, Tag):
                txt = sib.get_text(strip=True)
                if txt in HEADERS:
                    break
                for im in sib.find_all("img"):
                    src = im.get("src") or ""
                    if "/card_category/label/" in src:
                        lab = im.get("alt") or im.get("title") or ""
                        if lab:
                            cats3.append(lab)
                for a in sib.find_all("a"):
                    href = a.get("href") or ""
                    if "/categories/" in href:
                        t = a.get_text(strip=True)
                        if t:
                            cats3.append(t)
    merged = []
    seen = set()
    for pool in (cats1, cats2, cats3):
        for c in pool:
            s = c.strip()
            if s and s not in seen:
                seen.add(s); merged.append(s)
    return _clean_categories_python(merged)

def _clean_categories_python(cats: List[str]) -> List[str]:
    out = []
    seen = set()
    for s in cats or []:
        s = (s or "").strip().strip("•· ")
        if not s: continue
        low = s.lower()
        if low in CATEGORY_BLACKLIST_TOKENS: continue
        if EXT_FILE_PATTERN.search(s): continue
        if re.fullmatch(r"[\d\s%:]+", s): continue
        if s in HEADERS or "Links:" in s or "Show More" in s: continue
        if s in seen: continue
        seen.add(s); out.append(s)
    return out

# ------------ Rarity, Type, Obtain Type -------------

def detect_rarity_from_dom(soup: BeautifulSoup, image_urls_fallback: List[str]) -> Optional[str]:
    rarity_map = {"lr": "LR", "ur": "UR", "ssr": "SSR", "sr": "SR", "r": "R", "n": "N"}
    node = soup.select_one("div.card-icon-item.card-icon-item-rarity.card-info-above-thumb img[src]")
    if node:
        src = (node.get("src") or "").lower()
        m = re.search(r"cha_rare(?:_sm)?_(lr|ur|ssr|sr|r|n)\.png", src)
        if m:
            return rarity_map.get(m.group(1).lower())
    for url in image_urls_fallback or []:
        low = url.lower()
        m = re.search(r"cha_rare(?:_sm)?_(lr|ur|ssr|sr|r|n)\.png", low)
        if m:
            return rarity_map.get(m.group(1).lower())
    return None

def detect_type_token_from_dom(soup: BeautifulSoup) -> Optional[str]:
    candidates = soup.select("div.row.justify-content-center.align-items-center.padding-top-bottom-10.border.border-2")
    if not candidates:
        return None
    cls_list = candidates[0].get("class") or []
    type_found = None
    for cls in cls_list:
        if cls.startswith("border-") or cls.startswith("bg-"):
            suffix = cls.split("-", 1)[-1].strip().lower()
            if suffix in TYPE_SET:
                type_found = suffix
    return type_found

def parse_obtain_type(soup: BeautifulSoup) -> Optional[str]:
    for div in soup.find_all("div", class_=re.compile(r"\brow\b")):
        cls = " ".join(div.get("class") or [])
        if "padding-top-bottom-10" in cls:
            txt = div.get_text(" ", strip=True)
            if "Summonable" in txt:
                return "Summonable"
    return None

# ------------ Passive extras (transform/exchange) -------------

def extract_transform_and_exchange(passive_effect: str) -> Tuple[str, Dict[str, Optional[str]], Dict[str, Optional[str]]]:
    """
    Preserve full 'Reversible Exchange' clause for the exchange condition.
    Remove transform/exchange clauses from the displayed passive 'effect' to avoid duplication.
    """
    if not passive_effect:
        return passive_effect, {"can_transform": False, "condition": None}, {"can_exchange": False, "condition": None}

    clauses = [c.strip() for c in re.split(r"\s*;\s*", passive_effect) if c.strip()]
    keep: List[str] = []
    transform_clauses: List[str] = []
    exchange_clauses: List[str] = []

    for c in clauses:
        low = c.lower()
        if "reversible exchange" in low:
            exchange_clauses.append(c)
            continue
        if re.search(r"\btransforms?\b", low) or "transformation" in low:
            transform_clauses.append(c)
            continue
        keep.append(c)

    def pick_longest(cands: List[str]) -> Optional[str]:
        if not cands:
            return None
        best = max(cands, key=lambda s: len(s or ""))
        return _condense_spaces(best)

    transform_condition_raw = pick_longest(transform_clauses)
    exchange_condition_raw = pick_longest(exchange_clauses)

    cleaned_effect = "; ".join(keep).strip()
    transformation = {"can_transform": bool(transform_condition_raw), "condition": transform_condition_raw or None}
    reversible_exchange = {"can_exchange": bool(exchange_condition_raw), "condition": exchange_condition_raw or None}
    return cleaned_effect, transformation, reversible_exchange

# ------------ Domains / Standby / Finish -------------

def detect_type_suffix_from_classes(cls_list: List[str]) -> Optional[str]:
    t = None
    for cls in cls_list or []:
        if cls.startswith("border-") or cls.startswith("bg-"):
            suf = cls.split("-", 1)[-1].strip().lower()
            if suf in TYPE_SET:
                t = suf
    return t

def parse_domains(soup: BeautifulSoup) -> List[Dict[str, Optional[str]]]:
    domains: List[Dict[str, Optional[str]]] = []
    for bnode in soup.find_all("b", string=re.compile(r"^\s*Domain Effect\(s\)\s*$", re.IGNORECASE)):
        outer_row = bnode.find_parent("div", class_=re.compile(r"\brow\b"))
        if not outer_row: continue
        bolds = outer_row.find_all("b")
        domain_name = bolds[1].get_text(strip=True) if len(bolds) >= 2 else None
        container = outer_row.find_parent("div", class_=re.compile(r"\bborder\b"))
        type_suffix = detect_type_suffix_from_classes(container.get("class") or []) if container else None
        effect_text = None
        effect_row = outer_row.find_next_sibling("div")
        hops = 0
        while effect_row and hops < 3 and not effect_text:
            if effect_row.get("class") and any(c.startswith("bg-") and c.endswith("-2") for c in effect_row.get("class")):
                effect_text = effect_row.get_text(" ", strip=True); break
            deep = effect_row.find("div", class_=re.compile(r"\bbg-.*-2\b"))
            if deep:
                effect_text = deep.get_text(" ", strip=True); break
            effect_row = effect_row.find_next_sibling("div"); hops += 1
        domains.append({"name": domain_name, "effect": effect_text, "type": (type_suffix.upper() if type_suffix else None)})
    seen = set(); uniq = []
    for d in domains:
        key = (d.get("name") or "", d.get("effect") or "")
        if key in seen: continue
        seen.add(key); uniq.append(d)
    return uniq

def collect_effect_and_conditions(content_div: Tag, cond_label_regex: re.Pattern) -> Tuple[str, Optional[str]]:
    if not content_div:
        return "", None
    effect_lines: List[str] = []; cond_lines: List[str] = []; collecting_conditions = False
    for node in content_div.descendants:
        if isinstance(node, Tag) and node.name == "b" and node.string and cond_label_regex.search(node.string.strip()):
            collecting_conditions = True; continue
        if isinstance(node, Tag) and node.name == "hr":
            continue
        if isinstance(node, NavigableString):
            text = str(node).strip()
            if not text: continue
            (cond_lines if collecting_conditions else effect_lines).append(text)
    effect = _condense_spaces(" ".join(effect_lines))
    effect = re.sub(r"(Standby|Finish)\s+Skill\s+Condition\(s\)\s*$", "", effect, flags=re.IGNORECASE).strip()
    condition = _condense_spaces(" ".join(cond_lines)) if cond_lines else None
    if condition:
        condition = re.sub(r"^(Standby|Finish)\s+Skill\s+Condition\(s\)\s*", "", condition, flags=re.IGNORECASE).strip()
    effect = re.sub(r"\s*;\s*", "; ", effect)
    return effect, (condition or None)

def parse_skill_blocks(soup: BeautifulSoup, header_label: str, cond_label: str) -> List[Dict[str, Optional[str]]]:
    results: List[Dict[str, Optional[str]]]= []
    bnodes = soup.find_all("b", string=re.compile(rf"^\s*{re.escape(header_label)}\s*$", re.IGNORECASE))
    for bnode in bnodes:
        title_row = bnode.find_parent("div", class_=re.compile(r"\brow\b"))
        if not title_row: continue
        bolds = title_row.find_all("b")
        skill_name = bolds[1].get_text(strip=True) if len(bolds) >= 2 else None
        content_row = title_row.find_next_sibling("div")
        hops = 0
        while content_row and hops < 5:
            cls = content_row.get("class") or []
            if any(c.startswith("bg-") and (c.endswith("-2") or c in TYPE_SET) for c in cls) or content_row.find("div", class_=re.compile(r"\bbg-.*-2\b")):
                break
            content_row = content_row.find_next_sibling("div"); hops += 1
        container = title_row.find_parent("div", class_=re.compile(r"\bborder\b"))
        type_suffix = detect_type_suffix_from_classes(container.get("class") or []) if container else None
        type_upper = type_suffix.upper() if type_suffix else None
        effect, conditions = collect_effect_and_conditions(content_row or title_row, re.compile(rf"\b{re.escape(cond_label)}\b", re.IGNORECASE))
        results.append({"name": skill_name, "effect": effect or None, "conditions": conditions, "type": type_upper})
    return results

def parse_standby_skill(soup: BeautifulSoup) -> Optional[Dict[str, Optional[str]]]:
    blocks = parse_skill_blocks(soup, header_label="Standby Skill", cond_label="Standby Condition(s)")
    if not blocks: return None
    return max(blocks, key=lambda b: len(b.get("effect") or ""))

def parse_finish_skills(soup: BeautifulSoup) -> List[Dict[str, Optional[str]]]:
    return parse_skill_blocks(soup, header_label="Finish Skill", cond_label="Finish Skill Condition(s)")

# ------------ EZA detection -------------

def discover_eza_steps_on_page_soup(soup: Optional[BeautifulSoup], rarity_hint: Optional[str]) -> Tuple[List[int], Optional[int]]:
    """
    UI-driven EZA detection on the current page:
    - EZA considered present if either dropdown exists or a PRE-EZA / EZA toggle is visible.
    - Steps are derived from the dropdown (with existing fallback).
    - No neighbor-ID probing.
    """
    if not soup:
        return [], None
    has_toggle = bool(
        soup.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "PRE-EZA") and
        soup.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "EZA")
    )
    if not (has_eza_dropdown(soup) or has_toggle):
        return [], None
    steps = discover_eza_steps_with_fallback(soup, rarity_hint=rarity_hint)
    max_step = max(steps) if steps else None
    return steps, max_step


def discover_eza_steps_from_dropdown(soup: BeautifulSoup) -> List[int]:
    steps: List[int] = []
    for span in soup.select("div.multiselect ul.multiselect__content li.multiselect__element span.multiselect__option span"):
        txt = (span.get_text(strip=True) or "").strip()
        if txt.isdigit():
            steps.append(int(txt))
    return sorted(set(steps))

def has_eza_dropdown(soup: BeautifulSoup) -> bool:
    if not soup.select_one("div.multiselect"):
        return False
    if discover_eza_steps_from_dropdown(soup):
        return True
    single = soup.select_one("div.multiselect__tags span.multiselect__single")
    if single and (single.get_text(strip=True) or "").strip().isdigit():
        return True
    return False

def discover_eza_steps_with_fallback(soup: BeautifulSoup, rarity_hint: Optional[str]) -> List[int]:
    steps = discover_eza_steps_from_dropdown(soup)
    if steps:
        # Backfill 1..max to avoid missing middle steps
        return list(range(1, max(steps) + 1))
    single = soup.select_one("div.multiselect__tags span.multiselect__single")
    if not single:
        return []
    v = (single.get_text(strip=True) or "").strip()
    if not v.isdigit():
        return []
    cur = int(v)
    # If rarity is known, ensure full expected span (UR: 1..8 inc. super, LR: 1..4 inc. super)
    if rarity_hint and rarity_hint.upper() == "UR":
        return list(range(1, max(cur, 8) + 1))
    if rarity_hint and rarity_hint.upper() == "LR":
        return list(range(1, max(cur, 4) + 1))
    return list(range(1, cur + 1))

# ------------ Assets downloader -------------
EXT_FILE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp)$", re.IGNORECASE)

def _url_to_asset_rel(url: str) -> Optional[Path]:
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return None
        host = parsed.netloc.lower()
        if host not in {"dokkaninfo.com", "www.dokkaninfo.com"}:
            return None
        if not EXT_FILE_PATTERN.search(parsed.path):
            return None
        parts = [p for p in PurePosixPath(parsed.path).parts if p and p != "/"]
        return Path(host, *parts)
    except Exception:
        return None

def download_assets_for_card(image_urls: List[str]) -> List[str]:
    ASSETS_ROOT.mkdir(parents=True, exist_ok=True)
    rel_paths: List[str] = []
    seen_rel: Set[str] = set()

    sess = requests.Session()
    sess.headers.update(REQUEST_HEADERS)

    for u in image_urls or []:
        rel = _url_to_asset_rel(u)
        if rel is None:
            continue
        rel_str = str(rel)
        if rel_str in seen_rel:
            continue
        seen_rel.add(rel_str)

        target = ASSETS_ROOT / rel
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and target.stat().st_size > 0:
            rel_paths.append(rel_str)
            continue

        try:
            with sess.get(u, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(target, "wb") as f:
                    for chunk in r.iter_content(65536):
                        if chunk:
                            f.write(chunk)
            rel_paths.append(rel_str)
        except Exception as e:
            logging.warning("Asset failed: %s -> %s", u, e)

    return rel_paths

# ------------ NEW: Asset classification -------------
CARD_FILE_ID_RE = re.compile(r"/card/(\d+)/", re.IGNORECASE)
LOCALE_RE = re.compile(r"/(en|jp|kr|tw|cn)/", re.IGNORECASE)

def _extract_card_id_from_rel(rel: str) -> Optional[str]:
    p = rel.replace("\\", "/")
    m = CARD_FILE_ID_RE.search(p)
    if m:
        return m.group(1)
    m2 = CARD_ID_IN_SRC_RE.search(p)
    return m2.group(1) if m2 else None

def _extract_locale_from_rel(rel: str) -> Optional[str]:
    p = rel.replace("\\", "/")
    m = LOCALE_RE.search(p)
    return m.group(1).lower() if m else None

def classify_single_asset(rel: str) -> Dict[str, Optional[str]]:
    """
    Returns a dict with fields: path, category, subtype, card_id, locale, note
    Categories: card_art, thumbnail, ui, category_label, event_banner, equipment, site, other
    """
    p = rel.replace("\\", "/").lower()

    # Site / branding
    if "dokkan-info-logo" in p:
        return {"path": rel, "category": "site", "subtype": "logo", "card_id": None, "locale": None, "note": "DokkanInfo branding"}
    if "/venatus" in p or "ad" in p and "/image/" in p:
        return {"path": rel, "category": "site", "subtype": "ad", "card_id": None, "locale": None, "note": "Ad asset"}

    # UI elements
    if "cha_rare_sm_" in p or "cha_rare_" in p:
        return {"path": rel, "category": "ui", "subtype": "rarity_icon", "card_id": None, "locale": None, "note": None}
    if "/cha_type_icon_" in p:
        return {"path": rel, "category": "ui", "subtype": "type_icon", "card_id": None, "locale": None, "note": None}
    if "/character_thumb_bg/" in p:
        return {"path": rel, "category": "ui", "subtype": "thumb_bg", "card_id": None, "locale": None, "note": None}
    if "/ingame/battle/skill_dialog/" in p:
        return {"path": rel, "category": "ui", "subtype": "passive_mark", "card_id": None, "locale": None, "note": None}
    if "/ingame/common/condition/st_" in p:
        return {"path": rel, "category": "ui", "subtype": "condition_icon", "card_id": None, "locale": None, "note": None}
    if "/charamenu/dokkan/" in p:
        return {"path": rel, "category": "ui", "subtype": "menu_icon", "card_id": None, "locale": None, "note": None}

    # Category chips
    if "/card_category/label/" in p:
        lab = re.search(r"card_category_label_(\d+)_", p)
        return {"path": rel, "category": "category_label", "subtype": lab.group(1) if lab else None, "card_id": None, "locale": _extract_locale_from_rel(rel), "note": None}

    # Event banners
    if "/ingame/events/" in p:
        return {"path": rel, "category": "event_banner", "subtype": "zbattle" if "zbattle" in p else "event", "card_id": None, "locale": _extract_locale_from_rel(rel), "note": None}

    # Equipment
    if "/item/equipment/" in p or "/layout/en/image/item/equipment/" in p:
        st = "item" if "/equ_item_" in p else "thumb_bg"
        return {"path": rel, "category": "equipment", "subtype": st, "card_id": None, "locale": _extract_locale_from_rel(rel), "note": None}

    # Thumbnails
    if "/character/thumb/" in p:
        cid = _extract_card_id_from_rel(rel)
        return {"path": rel, "category": "thumbnail", "subtype": "card_thumb", "card_id": cid, "locale": _extract_locale_from_rel(rel), "note": None}

    # Card art (primary target)
    if "/character/card/" in p:
        cid = _extract_card_id_from_rel(rel)
        loc = _extract_locale_from_rel(rel)
        file = p.rsplit("/", 1)[-1]
        subtype = None
        note = None
        if cid and file == f"{cid}.png":
            subtype = "full_card"
        elif "_bg." in file:
            subtype = "bg"
        elif "_character." in file:
            subtype = "character"
        elif "_circle." in file:
            subtype = "circle"
        elif "_effect." in file:
            subtype = "effect"
        elif "cutin" in file:
            subtype = "cutin"
        elif "_sp02_name." in file:
            subtype = "super_name_alt"
        elif "_sp02_phrase." in file:
            subtype = "super_phrase_alt"
        elif "_sp_name." in file:
            subtype = "super_name"
        elif "_sp_phrase." in file:
            subtype = "super_phrase"
        else:
            subtype = "card_asset"
        return {"path": rel, "category": "card_art", "subtype": subtype, "card_id": cid, "locale": loc, "note": note}

    # Fallback
    return {"path": rel, "category": "other", "subtype": None, "card_id": _extract_card_id_from_rel(rel), "locale": _extract_locale_from_rel(rel), "note": None}

def build_assets_index(rel_paths: List[str]) -> Dict[str, List[Dict[str, Optional[str]]]]:
    buckets: Dict[str, List[Dict[str, Optional[str]]]] = {}
    seen_paths: Set[str] = set()
    for rel in rel_paths or []:
        if rel in seen_paths:
            continue
        seen_paths.add(rel)
        item = classify_single_asset(rel)
        cat = item["category"] or "other"
        buckets.setdefault(cat, []).append(item)
    # stable sort by subtype then path for readability
    for cat in buckets:
        buckets[cat].sort(key=lambda x: (x.get("subtype") or "", x.get("path") or ""))
    return buckets

def merge_assets_index(dst: Dict[str, List[dict]], src: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    if not dst:
        dst = {}
    if not src:
        return dst
    for cat, items in src.items():
        existing = dst.setdefault(cat, [])
        have = {x.get("path") for x in existing}
        for it in items:
            if it.get("path") not in have:
                existing.append(it)
        existing.sort(key=lambda x: (x.get("subtype") or "", x.get("path") or ""))
    return dst

# ------------ Scraping core (builds a single variant dict) -------------
def _prune_assets_index(idx: dict) -> dict:
    if not idx: return {}
    out = {}
    for cat, items in idx.items():
        if cat not in KEEP_ASSET_CATEGORIES:
            continue
        kept = []
        for it in items:
            loc = (it.get("locale") or "en").lower()
            if KEEP_ASSET_LOCALES and loc not in KEEP_ASSET_LOCALES:
                continue
            kept.append(it)
        if kept:
            out[cat] = kept
    return out
def _prune_assets_index(idx: dict) -> dict:
    if not idx: return {}
    out = {}
    for cat, items in idx.items():
        if cat not in KEEP_ASSET_CATEGORIES:
            continue
        kept = []
        for it in items:
            loc = (it.get("locale") or "en").lower()
            if KEEP_ASSET_LOCALES and loc not in KEEP_ASSET_LOCALES:
                continue
            kept.append(it)
        if kept:
            out[cat] = kept
    return out

def scrape_variant_from_html(page_html: str, page_url: str, variant: Dict[str, object]) -> Tuple[Dict[str, object], Dict[str, object]]:
    """
    Returns (unit_level_fields, variant_record)
    unit_level_fields carries display_name/rarity/type/source_base_url + union assets (+ assets_index)
    variant_record is a single item for variants[]
    """
    soup = BeautifulSoup(page_html, "lxml")

    # NEW: scope text to the correct variant side (base vs EZA)
    req_eza_flag = bool(variant.get("eza"))
    base_text_scope, eza_text_scope = _text_before_after_step_scope(soup)
    page_text = (eza_text_scope if req_eza_flag else base_text_scope) or soup.get_text("\n", strip=True)

    # Parse headers from the scoped text only (prevents EZA blocks overriding base)
    sections = _split_sections(page_text)

    leader_skill = _clean_leader(sections.get("Leader Skill") or [])
    super_name, super_effect = _clean_super_like(sections.get("Super Attack") or [])
    ultra_name, ultra_effect = _clean_super_like(sections.get("Ultra Super Attack") or [])

    if not super_name:
        mS = re.search(r"Super Attack\s+([\s\S]*?)\s+Ultra Super Attack", page_text, flags=re.IGNORECASE)
        if mS:
            block = [ln.strip() for ln in mS.group(1).splitlines() if ln.strip()]
            sn, se = _clean_super_like(block)
            super_name = super_name or sn
            super_effect = super_effect or se

    if not ultra_name:
        mU = re.search(
            r"Ultra Super Attack\s+([\s\S]*?)\s+(Passive Skill|Active Skill|Link Skills|Categories|Stats|Transformation Condition\(s\))",
            page_text,
            flags=re.IGNORECASE,
        )
        if mU:
            block = [ln.strip() for ln in mU.group(1).splitlines() if ln.strip()]
            un, ue = _clean_super_like(block)
            ultra_name = ultra_name or un
            ultra_effect = ultra_effect or ue

    passive_lines, _ = parse_passive_lines_from_dom(soup)
    passive_marked = render_passive_effect_with_markers(passive_lines)
    if not passive_lines and (sections.get("Passive Skill") or []):
        passive_block = sections.get("Passive Skill") or []
        passive_marked = _group_passive_lines_fallback(passive_block[1:])
    passive_block_text = sections.get("Passive Skill") or []
    passive_name = passive_block_text[0] if passive_block_text else None

    effect_for_scan, transformation, reversible_exchange = extract_transform_and_exchange(passive_marked)

    active_name, active_effect = _clean_active(sections.get("Active Skill") or [])
    activation_conditions = _clean_activation(sections.get("Activation Condition(s)") or [])
    transformation_conditions = _clean_activation(sections.get("Transformation Condition(s)") or [])
    link_skills = _clean_links(sections.get("Link Skills") or [])

    # Categories (names) for compatibility, plus detailed for index
    categories = parse_categories_from_soup(soup)
    categories_detailed = parse_categories_detailed(soup, page_url)

    stats_textual = _parse_stats_textual(sections.get("Stats") or [], page_text)
    stats_dom = _parse_stats_table_dom(soup)
    stats = {**stats_textual, **stats_dom}

    rel_dt_dom, rel_tz_dom, eza_rel_dom = _parse_release_dom(soup)
    if rel_dt_dom:
        release_date, tz = rel_dt_dom, rel_tz_dom
    else:
        release_date, tz = _parse_release(page_text)

    # Collect and download images (we still download all images on page)
    image_urls = []
    seen = set()
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        absu = urljoin(page_url, src)
        if absu not in seen:
            seen.add(absu)
            image_urls.append(absu)
    assets_rel_paths = download_assets_for_card(image_urls)
    assets_index = build_assets_index(assets_rel_paths)
    assets_index = _prune_assets_index(assets_index)

    rarity = detect_rarity_from_dom(soup, image_urls)
    type_token = detect_type_token_from_dom(soup)
    type_token_upper = type_token.upper() if type_token else None
    awak = parse_awaken_links_from_soup(soup, rarity_hint=rarity)

    h1 = soup.select_one("h1")
    base_display_name = (
        h1.get_text(strip=True)
        if (h1 and h1.get_text(strip=True))
        else (soup.title.string.strip() if (soup.title and soup.title.string) else "")
    )

    char_id = extract_character_id_from_url(page_url)

    domains = parse_domains(soup)
    standby_skill = parse_standby_skill(soup)
    finish_skills = parse_finish_skills(soup)

    # ---- unit-level fields ----
    unit_fields = {
        "unit_id": char_id,
        "form_id": char_id,
        "display_name": base_display_name,
        "rarity": rarity,
        "type": type_token_upper,
        "source_base_url": normalize_to_base_url(page_url),
        "assets": assets_rel_paths[:],       # list (back-compat)
        "assets_index": assets_index.copy(), # categorized for union
    }

    # ---- variant record ----
    variant_record = {
        "key": variant.get("key", "base"),
        "eza": bool(variant.get("eza")),
        "step": variant.get("step"),
        "is_super_eza": False,  # set later
        "source_url": page_url,
        "release_date": release_date,
        "timezone": tz,
        "eza_release_date": eza_rel_dom,
        "obtain_type": parse_obtain_type(soup),
        "awakening": {"from_ids": awak["from"], "to_ids": awak["to"]},
        "rarity_rank": _rarity_rank(rarity),
        "kit": {
            "leader_skill": leader_skill,
            "super_attack": {"name": super_name, "effect": super_effect},
            "ultra_super_attack": {"name": ultra_name, "effect": ultra_effect},
            "passive_skill": {
                "name": passive_name,
                "effect": effect_for_scan,
                "lines": passive_lines,
            },
            "transformation": transformation,
            "reversible_exchange": reversible_exchange,
            "transformation_conditions": transformation_conditions,
            "active_skill": {"name": active_name, "effect": active_effect, "activation_conditions": activation_conditions},
            "standby_skill": standby_skill,
            "finish_skills": finish_skills,
            "link_skills": link_skills,
            "categories": categories,
            "categories_detailed": categories_detailed,   # <---- NEW
            "stats": stats,
            "domains": domains,
        },
        "assets": assets_rel_paths,      # list (back-compat)
        "assets_index": assets_index,    # NEW per-variant
    }
    if not STORE_ASSETS_LIST:
        unit_fields.pop("assets", None)
        variant_record.pop("assets", None)
    return unit_fields, variant_record

# ------------ Single-folder write/merge -------------

def merge_variant_into_unit_json(folder: Path, unit_fields: Dict[str, object], variant_record: Dict[str, object]) -> Dict[str, object]:
    meta_path = folder / "METADATA.json"
    if meta_path.exists():
        try:
            current = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            current = {}
    else:
        current = {}

    # seed structure
    if not current:
        current = {
            "unit_id": unit_fields.get("unit_id"),
            "form_id": unit_fields.get("form_id"),
            "display_name": unit_fields.get("display_name"),
            "rarity": unit_fields.get("rarity"),
            "type": unit_fields.get("type"),
            "source_base_url": unit_fields.get("source_base_url"),
            "variants": [],
            "assets": [],
            "assets_index": {},   # NEW: top-level union
        }

    # refresh top-level fields if missing (don't overwrite once set)
    for k in ("display_name", "rarity", "type", "source_base_url"):
        if not current.get(k) and unit_fields.get(k):
            current[k] = unit_fields[k]

    # union assets (list)
    def _union(a: List[str], b: List[str]) -> List[str]:
        seen = set(a or [])
        out = list(a or [])
        for x in b or []:
            if x not in seen:
                seen.add(x); out.append(x)
        return out

    current["assets"] = _union(current.get("assets") or [], unit_fields.get("assets") or [])

    # union assets_index (dict of lists)
    current["assets_index"] = merge_assets_index(current.get("assets_index") or {}, unit_fields.get("assets_index") or {})

    # upsert variant by key
    key = variant_record.get("key")
    variants: List[dict] = current.get("variants") or []
    replaced = False
    for i, v in enumerate(variants):
        if v.get("key") == key:
            variants[i] = variant_record
            replaced = True
            break
    if not replaced:
        variants.append(variant_record)
    current["variants"] = variants

    # ---------------------------
    # C) Annotate awakening chains & "fully awakened"
    # ---------------------------

    # Local rarity mapping + helper for robustness
    RARITY_RANK = {"N": 0, "R": 1, "SR": 2, "SSR": 3, "UR": 4, "LR": 5}

    def _rarity_rank_of_variant(v: dict) -> int:
        # Prefer explicit rarity_rank if you stored it during scrape (step B)
        if isinstance(v.get("rarity_rank"), int):
            return v["rarity_rank"]
        # Fallback to textual rarity field
        r = (v.get("rarity") or "").upper()
        return RARITY_RANK.get(r, -1)

    # Normalize awakening fields on all variants
    variants = current.get("variants") or []
    for v in variants:
        awk = v.get("awakening") or {}
        # Ensure structure exists and is lists
        v["awakening"] = {
            "from_ids": list(awk.get("from_ids") or []),
            "to_ids": list(awk.get("to_ids") or []),
        }

    # Index variants by their form_id (string)
    var_by_id: Dict[str, dict] = {str(v.get("form_id")): v for v in variants if v.get("form_id")}

    # Collect whether any awakening links exist in this family
    all_from: set[str] = set()
    all_to: set[str] = set()
    for v in variants:
        all_from.update(str(i) for i in (v.get("awakening", {}).get("from_ids") or []))
        all_to.update(str(i) for i in (v.get("awakening", {}).get("to_ids") or []))
    family_has_any_chain = bool(all_from or all_to)

    def _next_ids(fid: str) -> List[str]:
        """Get the 'awakens to' ids for this form, preferring ones inside this file."""
        v = var_by_id.get(str(fid)) or {}
        ids = [str(i) for i in (v.get("awakening", {}).get("to_ids") or [])]
        internal = [i for i in ids if i in var_by_id]
        return internal if internal else ids

    def _chain_head(fid: str) -> str:
        """Follow 'to' links until terminal; on forks, choose highest rarity then highest id."""
        seen: set[str] = set()
        cur = str(fid)
        while True:
            nxts = _next_ids(cur)
            if not nxts:
                return cur
            # Choose best candidate by rarity, then numeric id
            def _key(nid: str):
                v = var_by_id.get(nid)
                rr = _rarity_rank_of_variant(v) if v else -1
                try:
                    num = int(nid)
                except Exception:
                    num = -1
                return (rr, num)
            nxt = max(nxts, key=_key)
            if nxt in seen:           # cycle guard
                return cur
            seen.add(nxt)
            cur = nxt

    # Annotate each variant
    for v in variants:
        fid = str(v.get("form_id")) if v.get("form_id") is not None else None
        head = _chain_head(fid) if fid else None
        v["awaken_chain_head_id"] = head
        # If no chain data exists at all, treat everything as "fully awakened" for folded views
        v["is_fully_awakened"] = (fid == head) if family_has_any_chain else True

    current["variants"] = variants

    # ---------------------------
    # NEW: Update global CATEGORIES_INDEX.json from this variant's detailed categories
    # ---------------------------
    try:
        cat_items = (variant_record.get("kit") or {}).get("categories_detailed") or []
        if cat_items:
            cat_index = load_category_index()
            for it in cat_items:
                _index_add_category_item(cat_index, it)
            save_category_index(cat_index)
    except Exception as e:
        logging.warning("Failed to update category index: %s", e)

    meta_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    return current

def ensure_unit_folder(unit_fields: Dict[str, object]) -> Path:
    disp = unit_fields.get("display_name") or "Unknown Card"
    rarity = unit_fields.get("rarity")
    t = unit_fields.get("type")
    cid = unit_fields.get("unit_id") or "unknown"
    prefix_parts = []
    if rarity: prefix_parts.append(rarity)
    if t: prefix_parts.append(f"[{t}]")
    prefix = " ".join(prefix_parts)
    disp_with_type_bracketed = f"{prefix} [{disp}]" if prefix else f"[{disp}]"
    folder_name = sanitize_filename(f"{disp_with_type_bracketed} - {cid}")
    card_dir = OUTROOT / folder_name
    card_dir.mkdir(parents=True, exist_ok=True)

    attr = card_dir / "ATTRIBUTION.txt"
    if not attr.exists():
        src = unit_fields.get("source_base_url") or ""
        attr.write_text(
            "Data and image assets collected from DokkanInfo.\n"
            f"Source base: {src}\n"
            "Site: https://dokkaninfo.com\n\n"
            "Notes:\n"
            "- Personal/educational use.\n"
            "- Respect the site's Terms and original owners' rights.\n"
            '- If you share output, credit: “Data/images via dokkaninfo.com”.\n',
            encoding="utf-8",
        )
    return card_dir

# ------------ Main -------------

def main():
    log_path = setup_logging()
    logging.info("Starting DokkanInfo scraper (headed) — EZA via dropdown + single-folder variants + transformations")
    OUTROOT.mkdir(parents=True, exist_ok=True)
    ASSETS_ROOT.mkdir(parents=True, exist_ok=True)

    index = load_index()
    existing_ids = collect_existing_unit_ids(OUTROOT, index)
    if existing_ids:
        logging.info("Existing unit families detected: %d", len(existing_ids))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        def goto_ok(url: str):
            """Navigate and return (ok_flag, html_or_none, final_url_str)."""
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
                ok = bool(resp and resp.ok)
                if not ok and resp:
                    logging.warning("Non-OK response %s for %s", resp.status, url)
                page.wait_for_timeout(700)
                html = page.content()
                return ok, html, page.url
            except PWTimeoutError as e:
                logging.warning("Load timeout for %s -> %s", url, e)
                return False, None, None
            except Exception as e:
                logging.warning("Navigation error for %s -> %s", url, e)
                return False, None, None

        # NEW: make sure the base scrape actually shows PRE-EZA DOM
        def ensure_pre_eza_mode():
            """
            Some pages render in EZA mode by default regardless of ?eza=false.
            If the PRE-EZA/EZA toggle exists, click PRE-EZA and let DOM settle.
            """
            try:
                if page.locator("div.multiselect").count() > 0 and page.locator("b", has_text="PRE-EZA").count() > 0:
                    page.locator("b", has_text="PRE-EZA").first.click()
                    page.wait_for_timeout(500)
            except Exception as e:
                logging.debug("ensure_pre_eza_mode() no-op: %s", e)

        def scrape_one_variant(url: str,
                               rarity_hint: Optional[str] = None,
                               force_folder: Optional[Path] = None,
                               variant_key_override: Optional[str] = None,
                               family_base_id: Optional[str] = None,
                               eza_max_step_hint: Optional[int] = None) -> Tuple[Optional[str], Optional[str], Optional[Path], bool, Optional[str]]:
            """Scrape a single page into a variant record and merge (optionally into an existing folder)."""
            req_eza_flag, req_step_i = parse_variant_from_url(url)
            ok, html, final_url = goto_ok(url)
            if not ok or not html:
                return None, None, None, False, None

            # NEW: If this is the base variant, force UI into PRE-EZA and re-capture HTML
            if not req_eza_flag:
                ensure_pre_eza_mode()
                try:
                    html = page.content()
                except Exception:
                    pass

            unit_fields, variant_record = scrape_variant_from_html(html, final_url or url, variant={
                "key": build_variant_key(req_eza_flag, req_step_i),
                "eza": req_eza_flag,
                "step": req_step_i,
            })

            # override key for transformation/foreign forms if needed
            if variant_key_override:
                variant_record["key"] = variant_key_override

            # variant-level identity + label for readability
            this_form_id = unit_fields.get("unit_id")
            this_display_name = unit_fields.get("display_name")
            variant_record["form_id"] = this_form_id
            # Prefer current rarity/type; fallback to hints
            variant_record["rarity"] = unit_fields.get("rarity") or rarity_hint
            variant_record["type"] = unit_fields.get("type")
            variant_record["display_name"] = this_display_name
            variant_record["variant_label"] = build_variant_label(
                display_name=this_display_name,
                form_id=this_form_id,
                family_base_id=family_base_id,
                eza=variant_record.get("eza"),
                step=variant_record.get("step"),
            )

            # label Super EZA if applicable (UI-driven)
            if variant_record.get("eza") and variant_record.get("step") is not None and eza_max_step_hint is not None:
                # Super EZA if the UI exposes a final step of 8 (UR) or 4 (LR);
                # mark only that final step as the Super step
                variant_record["is_super_eza"] = (variant_record["step"] == eza_max_step_hint and eza_max_step_hint in (4, 8))

            # folder + merge
            folder = force_folder or ensure_unit_folder(unit_fields)
            merged = merge_variant_into_unit_json(folder, unit_fields, variant_record)

            # update index (per form id)
            index_add_variant(index,
                              unit_fields.get("unit_id") or "unknown",
                              folder,
                              unit_fields.get("display_name") or merged.get("display_name") or "Unknown",
                              unit_fields.get("rarity") or merged.get("rarity"),
                              unit_fields.get("type") or merged.get("type"),
                              variant_record.get("key"))

            logging.info("Saved %s (%s) -> %s",
                         unit_fields.get("unit_id"), variant_record.get("key"), folder)
            return unit_fields.get("unit_id"), merged.get("rarity") or unit_fields.get("rarity"), folder, True, html

        # -------- canonical neighbor resolution + discovery --------
        def _extract_card_int_id(url: str) -> Optional[int]:
            m = CARD_ID_IN_HREF_RE.search(url)
            return int(m.group(1)) if m else None

        def _url_for_id(base_clean_url: str, nid: int) -> str:
            p = urlparse(base_clean_url)
            path = re.sub(r"/cards/\d+", f"/cards/{nid}", p.path)
            return urlunparse((p.scheme, p.netloc, path, "", "", ""))

        def resolve_canonical_and_discover_steps(base_clean_url: str, rarity_hint: Optional[str]) -> Tuple[
            str, List[int]]:
            """
            Try base form first; if no EZA, probe neighbor +1 then -1 (DokkanInfo sometimes
            puts the working EZA UI on an adjacent id). We ONLY accept EZA if the page shows
            hard evidence (release block or EZA stats headers).
            """
            candidates: List[str] = [base_clean_url]
            cid = _extract_card_int_id(base_clean_url)
            if cid is not None:
                candidates += [
                    _url_for_id(base_clean_url, cid + 1),
                    _url_for_id(base_clean_url, cid - 1),
                ]

            tried: Set[str] = set()
            for cand in candidates:
                if cand in tried:
                    continue
                tried.add(cand)

                # Force EZA view to load potential EZA-only UI
                step1_url = make_variant_url(cand, eza=True, step=1)
                ok1, html1, _ = goto_ok(step1_url)
                if not ok1 or not html1:
                    continue

                soup1 = BeautifulSoup(html1, "lxml")

                # HARD EVIDENCE gate
                if not has_eza_evidence(soup1):
                    logging.info("No EZA evidence found for %s (skipping EZA).", cand)
                    continue

                # Safe step discovery
                steps = discover_eza_steps_safe(soup1, rarity_hint=rarity_hint)
                if steps:
                    if cand != base_clean_url:
                        logging.info("Canonical base for form resolved to %s (from %s)", cand, base_clean_url)
                    logging.info("Discovered EZA steps %s for %s", steps, cand)
                    return cand, steps

            logging.info("No EZA found for %s", base_clean_url)
            return base_clean_url, []

        def has_eza_stats_headers(soup: BeautifulSoup) -> bool:
            """True if the Stats table shows EZA columns."""
            for th in soup.find_all("th"):
                txt = (th.get_text(" ", strip=True) or "").upper()
                # Typical headers we already map in _parse_stats_table_dom
                if "EZA" in txt or "EZA B." in txt or "EZA 100%" in txt:
                    return True
            return False

        def has_eza_release_block(soup: BeautifulSoup) -> bool:
            """True if the page exposes an 'EZA Release Date' block."""
            _, _, eza_rel_dt = _parse_release_dom(soup)
            return bool(eza_rel_dt)

        def has_eza_evidence(soup: BeautifulSoup) -> bool:
            """Final gate: we only consider EZA if there is hard evidence on the page."""
            return has_eza_release_block(soup) or has_eza_stats_headers(soup)

        def discover_eza_steps_safe(soup: BeautifulSoup, rarity_hint: Optional[str]) -> List[int]:
            """
            Read the EZA step dropdown ONLY if we have EZA evidence.
            Backfill 1..max. If there's only a single visible value, respect rarity_hint
            to extend to the expected cap (UR=8, LR=4).
            """
            if not has_eza_evidence(soup):
                return []

            steps = discover_eza_steps_from_dropdown(soup)
            if steps:
                return list(range(1, max(steps) + 1))

            # very rare: single tag visible (no list rendered)
            single = soup.select_one("div.multiselect__tags span.multiselect__single")
            if single:
                v = (single.get_text(strip=True) or "").strip()
                if v.isdigit():
                    cur = int(v)
                    if rarity_hint and rarity_hint.upper() == "UR":
                        return list(range(1, max(cur, 8) + 1))
                    if rarity_hint and rarity_hint.upper() == "LR":
                        return list(range(1, max(cur, 4) + 1))
                    return list(range(1, cur + 1))

            return []

        def discover_family_ids_bfs(start_html: Optional[str], start_id: str) -> List[str]:
            """
            BFS across the 'tile strip' so we don't miss transformations/variations shown only on sub-pages.
            """
            family: Set[str] = {start_id}
            queue: List[str] = []
            seen_pages: Set[str] = set()

            # seed from start_html if available
            if start_html:
                for rid in extract_ids_from_col5_images(start_html):
                    if rid not in family:
                        family.add(rid)
                        queue.append(rid)

            while queue and len(family) < MAX_FAMILY_SIZE:
                rid = queue.pop(0)
                url = normalize_to_base_url(f"{BASE}/cards/{rid}")
                if url in seen_pages:
                    continue
                seen_pages.add(url)
                ok, html, fin = goto_ok(make_variant_url(url, eza=False, step=None))
                if not ok or not html:
                    continue
                more = extract_ids_from_col5_images(html)
                for mid in more:
                    if mid not in family and len(family) < MAX_FAMILY_SIZE:
                        family.add(mid)
                        queue.append(mid)
            return sorted(family)

        def scrape_all_variants_for_base(base_clean_url: str, global_processed: Set[str]):
            """
            Per family:
              1) scrape base (eza=false) => create folder
              2) resolve canonical base for EZA dropdown + discover steps => scrape 1..max steps
              3) BFS discover transformations/variations => scrape each as variants into SAME folder
              returns (base_id, processed_ids_set, rarity)
            """
            # If this base id already processed (as part of another family), skip
            base_id = extract_character_id_from_url(base_clean_url) or ""
            if base_id in global_processed:
                logging.info("Skipping %s; already processed in another family.", base_id)
                return None, set(), None

            # NEW: hard skip if we've already scraped this family on disk or index
            if SKIP_EXISTING and base_id in existing_ids:
                logging.info("Skipping %s; already exists in index/disk.", base_id)
                global_processed.add(base_id)
                return None, set(), None

            # 1) base first
            base_url = make_variant_url(base_clean_url, eza=False, step=None)
            cid, rarity, folder, ok, html_base = scrape_one_variant(base_url, rarity_hint=None, family_base_id=None)
            if not cid or not folder:
                return None, set(), None

            # Track as existing now to avoid repeats later in the crawl
            existing_ids.add(cid)

            # Mark base as processed
            processed_ids: Set[str] = {cid}

            # 2) EZA steps (UI-driven) — write into same folder
            soup_base = BeautifulSoup(html_base, "lxml") if html_base else None
            steps, eza_max_step = discover_eza_steps_on_page_soup(soup_base, rarity_hint=rarity)

            # If the PRE-EZA/EZA toggle exists but steps weren't parsed, open the same card with eza=true to read the dropdown
            if (not steps) and soup_base:
                has_toggle = bool(
                    soup_base.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "PRE-EZA") and
                    soup_base.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "EZA")
                )
                if has_toggle:
                    ok_eza, html_eza, _ = goto_ok(make_variant_url(base_clean_url, eza=True, step=1))
                    if ok_eza and html_eza:
                        steps, eza_max_step = discover_eza_steps_on_page_soup(BeautifulSoup(html_eza, "lxml"), rarity_hint=rarity)

            for st in steps:
                step_url = make_variant_url(base_clean_url, eza=True, step=st)
                scrape_one_variant(step_url, rarity_hint=rarity, force_folder=folder,
                                   variant_key_override=f"eza_step_{st}", family_base_id=cid, eza_max_step_hint=eza_max_step)
                time.sleep(SLEEP_BETWEEN_CARDS)

            # 3) Family discovery (transformations/variations)
            family_ids = discover_family_ids_bfs(html_base, cid)

            # Scrape each related id (including base again in list, but we skip it)
            for rid in family_ids:
                if rid == cid:
                    continue
                if rid in processed_ids:
                    continue
                related_base = normalize_to_base_url(f"{BASE}/cards/{rid}")

                # related base (as variant)
                rcid, rrarity, _, rok, rhtml = scrape_one_variant(
                    make_variant_url(related_base, eza=False, step=None),
                    rarity_hint=None,
                    force_folder=folder,
                    variant_key_override=build_form_variant_key(rid, eza=False, step=None),
                    family_base_id=cid
                )
                if rcid:
                    processed_ids.add(rcid)

                # EZA steps for related (UI-driven)
                soup_rel = BeautifulSoup(rhtml, "lxml") if rhtml else None
                r_steps, r_eza_max_step = discover_eza_steps_on_page_soup(soup_rel, rarity_hint=rrarity)

                # If toggle exists but no steps parsed, open related page with eza=true
                if (not r_steps) and soup_rel:
                    has_toggle_rel = bool(
                        soup_rel.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "PRE-EZA") and
                        soup_rel.find("b", string=lambda s: isinstance(s, str) and s.strip().upper() == "EZA")
                    )
                    if has_toggle_rel:
                        ok_reza, html_reza, _ = goto_ok(make_variant_url(related_base, eza=True, step=1))
                        if ok_reza and html_reza:
                            r_steps, r_eza_max_step = discover_eza_steps_on_page_soup(BeautifulSoup(html_reza, "lxml"), rarity_hint=rrarity)

                for st in r_steps:
                    scrape_one_variant(
                        make_variant_url(related_base, eza=True, step=st),
                        rarity_hint=rrarity,
                        force_folder=folder,
                        variant_key_override=build_form_variant_key(rid, eza=True, step=st),
                        family_base_id=cid,
                        eza_max_step_hint=r_eza_max_step
                    )
                    time.sleep(SLEEP_BETWEEN_CARDS)

            # mark all processed in global set so index-mode won't double-process
            global_processed.update(processed_ids)
            return cid, processed_ids, rarity

        # -------- Execution modes --------
        processed_global: Set[str] = set()

        if SEED_URLS:
            logging.info("Seed mode: %d URL(s) — base → dropdown steps; includes transformations.", len(SEED_URLS))
            for base_any in SEED_URLS:
                base_clean = normalize_to_base_url(base_any)
                base_id_for_seed = extract_character_id_from_url(base_clean) or ""
                if SKIP_EXISTING and base_id_for_seed in existing_ids:
                    logging.info("Seed skip %s; already exists.", base_id_for_seed)
                    continue
                base_cid, family_ids, rarity = scrape_all_variants_for_base(base_clean, processed_global)
            browser.close()
            logging.info("Run completed. Log file: %s", log_path)
            return

        # -------- Index crawl mode --------
        bases_saved = 0
        total_saved = 0
        current_index_url = INDEX_URL
        pages_done = 0

        while pages_done < MAX_PAGES:
            try:
                logging.info("Opening index page: %s", current_index_url)
                page.goto(current_index_url, wait_until="domcontentloaded", timeout=TIMEOUT)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(700)
            except PWTimeoutError as e:
                logging.warning("Index page load timeout: %s", e)
                break

            container_sel = "div.row.d-flex.flex-wrap.justify-content-center"
            try:
                card_hrefs = page.eval_on_selector_all(
                    f'{container_sel} a.col-auto[href^="/cards/"]',
                    "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
                )
            except Exception:
                card_hrefs = []

            links = []
            seen_href = set()
            for h in card_hrefs:
                if not h or not h.startswith("/cards/"): continue
                if h in seen_href: continue
                seen_href.add(h)
                links.append(urljoin(BASE, h))

            if not links:
                logging.info("No more cards found in container on this page.")
                next_url = build_next_index_url(current_index_url)
                if next_url == current_index_url:
                    logging.info("Next URL equals current URL; stopping.")
                    break
                current_index_url = next_url
                pages_done += 1
                continue

            logging.info("Found %d card links on this page.", len(links))

            for i, card_url in enumerate(links, start=1):
                base_clean = normalize_to_base_url(card_url)
                base_id = extract_character_id_from_url(base_clean) or ""

                # Global skip for existing
                if SKIP_EXISTING and base_id in existing_ids:
                    logging.info("Index skip %s; already exists in index/disk.", base_id)
                    continue

                base_cid, processed_ids, rarity = scrape_all_variants_for_base(base_clean, processed_global)

                # Update counters
                if base_cid:
                    total_saved += 1
                    existing_ids.add(base_cid)
                    if COUNT_MODE == "bases":
                        bases_saved += 1

                # stop conditions
                if COUNT_MODE == "total" and total_saved >= MAX_NEW_CARDS:
                    logging.info("Reached MAX_NEW_CARDS=%d (total). Stopping.", MAX_NEW_CARDS)
                    browser.close()
                    logging.info("Run completed. Log file: %s", log_path)
                    return
                if COUNT_MODE == "bases" and bases_saved >= MAX_NEW_CARDS:
                    logging.info("Reached MAX_NEW_CARDS=%d (bases). Stopping.", MAX_NEW_CARDS)
                    browser.close()
                    logging.info("Run completed. Log file: %s", log_path)
                    return

                time.sleep(SLEEP_BETWEEN_CARDS)

            next_url = build_next_index_url(current_index_url)
            if next_url == current_index_url:
                logging.info("Next URL equals current URL after processing; stopping.")
                break
            current_index_url = next_url
            pages_done += 1

        browser.close()
        logging.info("Run completed. Log file: %s", log_path)

if __name__ == "__main__":
    main()
