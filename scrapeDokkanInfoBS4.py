# scrapeDokkanInfo_play_bs4_index_eza_detect.py
# Playwright (headed) + BeautifulSoup + Transformations + sequential EZA + DOM EZA toggle detection
#
# Flow per form:
#   1) base (eza=false, with storage cleared) -> capture CANONICAL base URL
#   2) if base shows PRE-EZA/EZA toggle: try regular steps in order (stop at first failure)
#   3) if toggle exists: try Super EZA last (LR step=4 / UR step=8)
#
# Guards:
#   - Clear localStorage/sessionStorage before every navigation (prevents tab "stickiness")
#   - Use canonical base URL for all ?eza=true&step=X requests
#   - Final-URL param guard (if site strips eza/step, skip)
#   - HTTP ok guard (non-OK => skip)
#
# Usage: python scrapeDokkanInfo_play_bs4_index_eza_detect.py

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
INDEX_URL = f"{BASE}/cards?sort=open_at"

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

TIMEOUT = 60_000
SLEEP_BETWEEN_CARDS = 0.6
MAX_PAGES = 200
MAX_NEW_CARDS = 50

# ---- Seed test ----
SEED_URLS: List[str] = [
    "https://dokkaninfo.com/cards/1014761",  # your test LR
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

def is_variant_already_indexed(index: Dict[str, dict], char_id: str, variant_key: str) -> bool:
    if char_id not in index:
        return False
    meta = index[char_id]
    if variant_key == "base":
        return bool(meta.get("folder"))
    variants = meta.get("variants") or {}
    return variant_key in variants

def upsert_variant_in_index(index: Dict[str, dict], char_id: str, base_display_name: str,
                            display_name_with_type: str, display_name_with_type_bracketed: str,
                            rarity: Optional[str], type_token_upper: Optional[str],
                            variant_key: str, folder: Path, url: str, eza: bool, step: Optional[int], is_super: bool):
    if char_id not in index:
        index[char_id] = {
            "url": url,
            "display_name": base_display_name,
            "display_name_with_type": display_name_with_type,
            "display_name_with_type_bracketed": display_name_with_type_bracketed,
            "rarity": rarity,
            "type": type_token_upper,
            "folder": None,
            "variants": {},
            "saved_at": datetime.utcnow().isoformat() + "Z",
        }
    if variant_key == "base":
        index[char_id]["folder"] = str(folder)
    else:
        index[char_id].setdefault("variants", {})
        human_label = "Super EZA" if is_super else f"EZA step {step}" if step is not None else "EZA"
        index[char_id]["variants"][variant_key] = {
            "url": url,
            "folder": str(folder),
            "eza": eza,
            "step": step,
            "is_super_eza": is_super,
            "label": human_label,
            "saved_at": datetime.utcnow().isoformat() + "Z",
        }
    save_index(index)

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
    for sub in tiles[1:]:  # skip first tile entirely
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

def regular_eza_steps_for_rarity(rarity: Optional[str]) -> List[int]:
    if not rarity:
        return []
    r = rarity.upper()
    if r == "LR":
        return [1, 2, 3]
    if r == "UR":
        return [1, 2, 3, 4, 5, 6, 7]
    return []

def build_variant_key(eza: bool, step: Optional[int]) -> str:
    if not eza:
        return "base"
    if step is None:
        return "eza"
    return f"eza_step_{step}"

def label_variant_suffix_from_flags(variant_key: str, is_super: bool, step: Optional[int]) -> str:
    if variant_key == "base":
        return ""
    if is_super:
        return " - Super EZA"
    if variant_key.startswith("eza_step_") and step is not None:
        return f" - EZA step {step}"
    if variant_key == "eza":
        return " - EZA"
    return " - " + variant_key

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
        b = soup.find("b", string=re.compile(rf"^\s*{re.escape(b_label)}\s*$", re.IGNORECASE))
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
    if not passive_effect:
        return passive_effect, {"can_transform": False, "condition": None}, {"can_exchange": False, "condition": None}
    clauses = [c.strip() for c in re.split(r"\s*;\s*", passive_effect) if c.strip()]
    keep: List[str] = []
    transform_clauses: List[str] = []
    exchange_clauses: List[str] = []
    for c in clauses:
        low = c.lower()
        if re.search(r"\breversible\s+exchange\b", low):
            exchange_clauses.append(c); continue
        if re.search(r"\btransforms?\b", low) or "transformation" in low:
            transform_clauses.append(c); continue
        keep.append(c)
    def pick_condition(cands: List[str]) -> Optional[str]:
        if not cands: return None
        prioritized = [x for x in cands if re.search(r"\b(when|starting|from the|turn|team|entry|once only)\b", x, re.IGNORECASE)]
        chosen_pool = prioritized if prioritized else cands
        return _condense_spaces(max(chosen_pool, key=len))
    def normalize_transform(text: str) -> str:
        text = re.sub(r"\bTransformation\b\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bTransforms\s+Transforms\b", "Transforms", text, flags=re.IGNORECASE)
        text = re.sub(r"^\s*(Transforms|Transformation)\s*", "", text, flags=re.IGNORECASE)
        return _condense_spaces(text)
    def normalize_exchange(text: str) -> str:
        text = re.sub(r"(?i)\b(Reversible Exchange)\b(?:\s+\1\b)+", r"\1", text)
        m = re.search(r"(?is)(meets\s+up\s+with.*?reversible\s+exchange.*)$", text)
        if not m:
            m = re.search(r"(?is)(reversible\s+exchange.*)$", text)
        if m:
            text = m.group(1)
        text = re.sub(r"^\s*(and|or|,|;)\s*", "", text, flags=re.IGNORECASE)
        return _condense_spaces(text)
    transform_condition_raw = pick_condition(transform_clauses)
    exchange_condition_raw = pick_condition(exchange_clauses)
    cleaned_effect = "; ".join(keep).strip()
    transformation = {"can_transform": bool(transform_condition_raw), "condition": None}
    if transform_condition_raw:
        transformation["condition"] = normalize_transform(transform_condition_raw)
    reversible_exchange = {"can_exchange": bool(exchange_condition_raw), "condition": None}
    if exchange_condition_raw:
        reversible_exchange["condition"] = normalize_exchange(exchange_condition_raw)
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

# ------------ EZA detection (toggle on base) -------------
def eza_toggle_exists_on_base(soup: BeautifulSoup) -> bool:
    # Robustly detect the two-tab selector PRE-EZA / EZA
    for row in soup.find_all("div", class_=re.compile(r"\brow\b")):
        classes = " ".join(row.get("class") or [])
        if "cursor-pointer" not in classes:
            continue
        labels = [b.get_text(strip=True).upper() for b in row.find_all("b")]
        if ("PRE-EZA" in labels and "EZA" in labels) or ("PRE EZA" in labels and "EZA" in labels):
            return True
    return False

# ------------ Assets downloader -------------
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

# ------------ Scraping core -------------
def scrape_card_from_html(page_html: str, page_url: str, variant: Dict[str, object]) -> Dict[str, object]:
    soup = BeautifulSoup(page_html, "lxml")
    page_text = soup.get_text("\n", strip=True)
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

    # Passive
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

    categories = parse_categories_from_soup(soup)

    stats_textual = _parse_stats_textual(sections.get("Stats") or [], page_text)
    stats_dom = _parse_stats_table_dom(soup)
    stats = {**stats_textual, **stats_dom}

    rel_dt_dom, rel_tz_dom, eza_rel_dom = _parse_release_dom(soup)
    if rel_dt_dom:
        release_date, tz = rel_dt_dom, rel_tz_dom
    else:
        release_date, tz = _parse_release(page_text)

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

    rarity = detect_rarity_from_dom(soup, image_urls)
    type_token = detect_type_token_from_dom(soup)
    type_token_upper = type_token.upper() if type_token else None

    type_icon = None
    for url in image_urls:
        if "cha_type_icon_" in url.lower():
            type_icon = url.split("/")[-1]
            break

    obtain_type = parse_obtain_type(soup)

    h1 = soup.select_one("h1")
    base_display_name = (
        h1.get_text(strip=True)
        if (h1 and h1.get_text(strip=True))
        else (soup.title.string.strip() if (soup.title and soup.title.string) else "")
    )
    page_title = soup.title.string.strip() if soup.title and soup.title.string else ""

    prefix_parts = []
    if rarity: prefix_parts.append(rarity)
    if type_token_upper: prefix_parts.append(f"[{type_token_upper}]")
    prefix = " ".join(prefix_parts)
    display_name_with_type = f"{prefix} {base_display_name}".strip() if prefix else base_display_name
    display_name_with_type_bracketed = f"{prefix} [{base_display_name}]".strip() if prefix else f"[{base_display_name}]"

    char_id = extract_character_id_from_url(page_url)

    domains = parse_domains(soup)
    standby_skill = parse_standby_skill(soup)
    finish_skills = parse_finish_skills(soup)

    # EZA toggle only checked on base pages (but harmless to store here too)
    eza_toggle = eza_toggle_exists_on_base(soup)

    meta = {
        "page_title": page_title,
        "display_name": base_display_name,
        "display_name_with_type": display_name_with_type,
        "display_name_with_type_bracketed": display_name_with_type_bracketed,
        "character_id": char_id,
        "release_date": release_date,
        "timezone": tz,
        "eza_release_date": eza_rel_dom,

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
        "stats": stats,
        "domains": domains,

        "source_url": page_url,
        "rarity_detected": rarity,
        "type_token": type_token,
        "type_token_upper": type_token_upper,
        "type_icon_filename": type_icon,
        "obtain_type": obtain_type,
        "assets": assets_rel_paths,

        "variant": {
            "key": variant.get("key", "base"),
            "eza": bool(variant.get("eza")),
            "step": variant.get("step"),
            "is_super_eza": False,  # set after rarity known
            "eza_toggle_on_base": eza_toggle,   # <— base-page toggle presence
        },
    }
    return meta

def write_card_outputs_and_update_index(meta: Dict[str, object], index: Dict[str, dict]) -> None:
    base_display_name = meta.get("display_name") or "Unknown Card"
    display_name_with_type = meta.get("display_name_with_type") or base_display_name
    display_name_with_type_bracketed = meta.get("display_name_with_type_bracketed") or f"[{base_display_name}]"
    rarity = meta.get("rarity_detected")
    type_token_upper = meta.get("type_token_upper")
    char_id = meta.get("character_id") or "unknown"

    variant = meta.get("variant") or {}
    variant_key = variant.get("key", "base")
    is_super = bool(variant.get("is_super_eza"))
    step = variant.get("step")

    suffix = label_variant_suffix_from_flags(variant_key, is_super=is_super, step=step)

    folder_name = sanitize_filename(f"{display_name_with_type_bracketed} - {char_id}{suffix}")
    card_dir = OUTROOT / folder_name
    card_dir.mkdir(parents=True, exist_ok=True)

    (card_dir / "METADATA.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    src = meta.get("source_url") or ""
    (card_dir / "ATTRIBUTION.txt").write_text(
        "Data and image assets collected from DokkanInfo.\n"
        f"Source page: {src}\n"
        "Site: https://dokkaninfo.com\n\n"
        "Notes:\n"
        "- Personal/educational use.\n"
        "- Respect the site's Terms and original owners' rights.\n"
        "- If you share output, credit: “Data/images via dokkaninfo.com”.\n",
        encoding="utf-8",
    )

    upsert_variant_in_index(
        index=index,
        char_id=char_id,
        base_display_name=base_display_name,
        display_name_with_type=display_name_with_type,
        display_name_with_type_bracketed=display_name_with_type_bracketed,
        rarity=rarity,
        type_token_upper=type_token_upper,
        variant_key=variant_key,
        folder=card_dir,
        url=meta.get("source_url") or "",
        eza=bool(variant.get("eza")),
        step=step,
        is_super=is_super,
    )
    logging.info("Saved %s (%s) -> %s", char_id, "Super EZA" if is_super else variant_key, card_dir)

# ------------ Main -------------
def main():
    log_path = setup_logging()
    logging.info("Starting DokkanInfo scraper (headed) — Transformations + sequential EZA + base toggle detection + clean storage")
    OUTROOT.mkdir(parents=True, exist_ok=True)
    ASSETS_ROOT.mkdir(parents=True, exist_ok=True)

    index = load_index()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        # Clear local/session storage BEFORE any site script runs
        page.add_init_script("try{localStorage.clear();sessionStorage.clear();}catch(e){}")

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

        def scrape_one(url: str, rarity_hint: Optional[str] = None, discover_forms: bool = False):
            """
            Scrape a single variant if not already saved.
            Returns (char_id, rarity, base_has_toggle, related_form_ids, canonical_base_url, success_bool).
            """
            req_eza_flag, req_step_i = parse_variant_from_url(url)
            variant_key = build_variant_key(req_eza_flag, req_step_i)
            cid = extract_character_id_from_url(url) or "unknown"

            if is_variant_already_indexed(index, cid, variant_key):
                logging.info("Skip %s %s: already indexed.", cid, variant_key)
                return cid, None, None, [], normalize_to_base_url(url), False

            ok, html, final_url = goto_ok(url)
            if not ok or not html:
                logging.info("Skipping %s (%s) due to non-OK load.", cid, variant_key)
                return None, None, None, [], None, False

            fin_eza_flag, fin_step_i = parse_variant_from_url(final_url or "")
            if req_eza_flag != fin_eza_flag or req_step_i != fin_step_i:
                logging.info("Final URL params mismatch for %s -> requested eza=%s,step=%s but final eza=%s,step=%s; skipping.",
                             url, req_eza_flag, req_step_i, fin_eza_flag, fin_step_i)
                return None, None, None, [], None, False

            canonical_base = normalize_to_base_url(final_url or url)

            meta = scrape_card_from_html(html, final_url or url, variant={
                "key": variant_key,
                "eza": req_eza_flag,
                "step": req_step_i,
                "is_super_eza": False,
            })

            rarity = meta.get("rarity_detected") or rarity_hint
            super_step = super_eza_step_for_rarity(rarity)
            is_super = bool(req_eza_flag and req_step_i is not None and super_step is not None and req_step_i == super_step)
            meta["variant"]["is_super_eza"] = is_super

            write_card_outputs_and_update_index(meta, index)

            base_has_toggle = bool(meta["variant"].get("eza_toggle_on_base"))
            related_ids = extract_ids_from_col5_images(html) if discover_forms else []
            return meta.get("character_id"), rarity, base_has_toggle, related_ids, canonical_base, True

        def scrape_all_variants_for_base(initial_base_url: str):
            """
            Order: base -> steps ascending (if base toggle exists) -> Super EZA (if base toggle exists).
            Always begin from eza=false for each form, then use the CANONICAL base URL for steps.
            """
            # Base first
            base_url = make_variant_url(initial_base_url, eza=False, step=None)
            cid, rarity, has_toggle, related_ids, canonical_base, ok = scrape_one(base_url, discover_forms=True)
            if not cid or not canonical_base:
                return None, [], None

            if canonical_base != initial_base_url:
                logging.info("Canonical base for form resolved to %s (from %s)", canonical_base, initial_base_url)

            if has_toggle:
                # Regular steps in order; stop at first failure
                steps = regular_eza_steps_for_rarity(rarity)
                logging.info("Attempting regular EZA steps %s for %s", steps, canonical_base)
                for st in steps:
                    step_url = make_variant_url(canonical_base, eza=True, step=st)
                    _c2, _, _tog2, _, _canon2, ok2 = scrape_one(step_url, rarity_hint=rarity)
                    if not ok2:
                        logging.info("Regular EZA step %s failed or missing for %s; stopping further steps.", st, canonical_base)
                        break
                    time.sleep(SLEEP_BETWEEN_CARDS)

                # Super EZA LAST (only if toggle exists at all)
                sstep = super_eza_step_for_rarity(rarity)
                if sstep is not None:
                    logging.info("Attempting Super EZA (step=%s) for %s", sstep, canonical_base)
                    super_url = make_variant_url(canonical_base, eza=True, step=sstep)
                    _c3, _, _tog3, _, _canon3, ok3 = scrape_one(super_url, rarity_hint=rarity)
                    if not ok3:
                        logging.info("Super EZA not available (or failed) at step=%s for %s", sstep, canonical_base)
            else:
                logging.info("No PRE-EZA/EZA toggle on base for %s — skipping all EZA steps.", canonical_base)

            return cid, related_ids, rarity

        if SEED_URLS:
            logging.info("Seed mode: %d URL(s) — base → regular EZAs → Super EZA (last). Includes transformations.", len(SEED_URLS))
            visited_forms: Set[str] = set()

            for base_any in SEED_URLS:
                base_clean = normalize_to_base_url(base_any)

                # Base + its EZAs sequentially (canonical base applied inside)
                base_cid, related_ids, rarity = scrape_all_variants_for_base(base_clean)

                # Each transformation: start from eza=false, then steps; canonical base handled per form
                for rid in related_ids or []:
                    if rid in visited_forms:
                        continue
                    visited_forms.add(rid)
                    form_base = normalize_to_base_url(f"{BASE}/cards/{rid}")
                    scrape_all_variants_for_base(form_base)

            browser.close()
            logging.info("Run completed. Log file: %s", log_path)
            return

        # -------- Index crawl (sequential order + canonical base) --------
        new_cards_saved = 0
        current_index_url = INDEX_URL
        pages_done = 0

        while pages_done < MAX_PAGES and new_cards_saved < MAX_NEW_CARDS:
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
                if new_cards_saved >= MAX_NEW_CARDS:
                    logging.info("Reached MAX_NEW_CARDS=%d; stopping crawl.", MAX_NEW_CARDS)
                    break

                base_clean = normalize_to_base_url(card_url)
                base_cid, related_ids, rarity = scrape_all_variants_for_base(base_clean)
                if base_cid:
                    new_cards_saved += 1

                for rid in related_ids or []:
                    form_base = normalize_to_base_url(f"{BASE}/cards/{rid}")
                    c_form, _, _ = scrape_all_variants_for_base(form_base)
                    if c_form:
                        new_cards_saved += 1

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
