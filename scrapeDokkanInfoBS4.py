# scrapeDokkanInfo_play_bs4_index_start.py
# Playwright (headed) + BeautifulSoup
# - Starts from index: https://dokkaninfo.com/cards?sort=open_at
# - Collects card links per page from container:
#     div.row.d-flex.flex-wrap.justify-content-center
# - When there are **no more** cards in that container:
#     * If URL has **no page=** param, assume page 1 and go to **&page=2**
#     * Else, increment page=N -> **page=N+1** and navigate
# - Stops when a page yields **no cards at all**, or after MAX_NEW_CARDS are saved
# - Skips IDs already in output/cards/CARDS_INDEX.json
# - Scrapes each new card
# - From header row, reads each div.col-5 image src to extract related IDs (SKIPS FIRST TILE)
#   and opens https://dokkaninfo.com/cards/<ID> directly (also skip if already indexed)
# - NO asset downloading in this version
# - RARITY: detected from
#     div.card-icon-item.card-icon-item-rarity.card-info-above-thumb img[src*="cha_rare_..."]
# - TYPE: detected from
#     div.row.justify-content-center.align-items-center.padding-top-bottom-10.border.border-2.border-<type>.bg-<type>
#     Take the **last** matching token among border-*/bg-* where suffix in {str, teq, int, agl, phy}
# - Display name (plain): "<RARITY> [<TYPE>] <Original H1/Title>"
# - Display name (bracketed for folder): "<RARITY> [<TYPE>] [<Original H1/Title>]"
# - Leader Skill full capture; Passive Skill never starts with "Basic effect(s):"
# - Maintains a persistent index at output/cards/CARDS_INDEX.json

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup, NavigableString, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------ Config -------------
BASE = "https://dokkaninfo.com"
INDEX_URL = f"{BASE}/cards?sort=open_at"

OUTROOT = Path("output/cards")
INDEX_PATH = OUTROOT / "CARDS_INDEX.json"
LOGDIR = Path("output/logs")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

TIMEOUT = 60_000
SLEEP_BETWEEN_CARDS = 0.6
MAX_PAGES = 200            # safety stop to avoid infinite loops
MAX_NEW_CARDS = 10         # <-- stop after saving this many *new* cards

HEADERS = [
    "Leader Skill",
    "Super Attack",
    "Ultra Super Attack",
    "Passive Skill",
    "Active Skill",
    "Activation Condition(s)",
    "Link Skills",
    "Categories",
    "Stats",
]

CATEGORY_BLACKLIST_TOKENS = {
    "background", "icon", "rarity", "element", "eza", "undefined",
    "venatus", "show more", "links", "categories",
}
EXT_FILE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp)$", re.IGNORECASE)

# Extract numeric ID from href (/cards/1031961) or from image src (...card_4031970_...)
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
    """
    From the header row, for each div.col-5, look at its <img src="...card_<ID>_...">
    and return a list of those <ID> strings (deduped, in order).
    IMPORTANT: skip the FIRST .col-5 entirely.
    """
    soup = BeautifulSoup(page_html, "lxml")

    required = {"row", "cursor-pointer", "unselectable", "border", "border-2", "border-dark", "margin-top-bottom-5"}
    header_div = None
    for div in soup.find_all("div"):
        cls = set(div.get("class") or [])
        if required.issubset(cls):
            header_div = div
            break

    if not header_div:
        logging.info("Header row not found; no .col-5 IDs to extract.")
        return []

    tiles = header_div.find_all("div", class_=lambda v: v and "col-5" in v.split())
    if not tiles:
        return []

    ids: List[str] = []
    seen: Set[str] = set()

    for sub in tiles[1:]:  # SKIP FIRST TILE
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
    """
    If url has no page= param -> add page=2; else increment it.
    Preserve all other query params (like sort).
    """
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
    leader = re.sub(
        r'("Exploding Rage"\s*Category\s+Ki\s*\+\d+\s+and\s+HP,\s*ATK\s*&\s*DEF\s*\+\d+%)\s*\1',
        r"\1",
        leader,
        flags=re.IGNORECASE,
    )
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

def _group_passive_lines(lines: List[str]) -> str:
    if not lines:
        return ""

    lines = [ln for ln in lines if ln not in HEADERS]
    lines = [ln for ln in lines if not re.fullmatch(r"Basic effect\(s\):?", ln, flags=re.IGNORECASE)]

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
        for pat in leading_patterns:
            if re.search(pat, s, flags=re.IGNORECASE):
                return True
            # also treat "Basic effect(s):" as NOT-leading and strip it out
        return False

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
    # FIXED: previously had a typo " " ".join(block)
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

def _parse_stats(block: List[str], page_text: str) -> Dict[str, object]:
    stats: Dict[str, object] = {}
    m_cost = re.search(r"\bCost\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_cost: stats["Cost"] = int(m_cost.group(1))
    m_max = re.search(r"\bMax\s*Lv\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_max: stats["Max Lv"] = int(m_max.group(1))
    m_sa = re.search(r"\bSA\s*Lv\s*:\s*(\d+)", page_text, flags=re.IGNORECASE)
    if m_sa: stats["SA Lv"] = int(m_sa.group(1))

    def parse_row(key: str) -> Optional[Dict[str, int]]:
        pat = re.compile(rf"^{key}\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)$", flags=re.IGNORECASE)
        for ln in block:
            m = pat.match(ln)
            if m:
                return {
                    "Base Min": int(m.group(1).replace(",", "")),
                    "Base Max": int(m.group(2).replace(",", "")),
                    "55%": int(m.group(3).replace(",", "")),
                    "100%": int(m.group(4).replace(",", "")),
                }
        return None

    for key in ["HP", "ATK", "DEF"]:
        row = parse_row(key)
        if row: stats[key] = row
    return stats

def _parse_release(page_text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(
        r"Release Date\s+([0-9/.\-]+)\s+([0-9: ]+[APMapm]{2})\s+([A-Z]{2,4})",
        page_text,
        flags=re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)} {m.group(2)}", m.group(3)
    return None, None

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

# ------------ Rarity & Type detection -------------
def detect_rarity_from_dom(soup: BeautifulSoup, image_urls_fallback: List[str]) -> Optional[str]:
    rarity_map = {
        "lr": "LR",
        "ur": "UR",
        "ssr": "SSR",
        "sr": "SR",
        "r": "R",
        "n": "N",
    }

    node = soup.select_one("div.card-icon-item.card-icon-item-rarity.card-info-above-thumb img[src]")
    if node:
        src = (node.get("src") or "").lower()
        m = re.search(r"cha_rare(?:_sm)?_(lr|ur|ssr|sr|r|n)\.png", src)
        if m:
            key = m.group(1).lower()
            return rarity_map.get(key)

    for url in image_urls_fallback or []:
        low = url.lower()
        m = re.search(r"cha_rare(?:_sm)?_(lr|ur|ssr|sr|r|n)\.png", low)
        if m:
            key = m.group(1).lower()
            return rarity_map.get(key)
    return None

def detect_type_token_from_dom(soup: BeautifulSoup) -> Optional[str]:
    """
    Find the row:
      div.row.justify-content-center.align-items-center.padding-top-bottom-10.border.border-2.border-<type>.bg-<type>
    and return the **last** matching type token among class suffixes (border-*/bg-*)
    where suffix in {str, teq, int, agl, phy}.
    """
    candidates = soup.select("div.row.justify-content-center.align-items-center.padding-top-bottom-10.border.border-2")
    if not candidates:
        return None

    cls_list = candidates[0].get("class") or []
    type_found = None
    for cls in cls_list:
        if cls.startswith("border-") or cls.startswith("bg-"):
            suffix = cls.split("-", 1)[-1].strip().lower()
            if suffix in TYPE_SET:
                type_found = suffix  # last one wins
    return type_found

# ------------ Scraping core -------------
def scrape_card_from_html(page_html: str, page_url: str) -> Dict[str, object]:
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
            r"Ultra Super Attack\s+([\s\S]*?)\s+(Passive Skill|Active Skill|Link Skills|Categories|Stats)",
            page_text,
            flags=re.IGNORECASE,
        )
        if mU:
            block = [ln.strip() for ln in mU.group(1).splitlines() if ln.strip()]
            un, ue = _clean_super_like(block)
            ultra_name = ultra_name or un
            ultra_effect = ultra_effect or ue

    passive_block = sections.get("Passive Skill") or []
    passive_name = passive_block[0] if passive_block else None
    passive_effect_lines = passive_block[1:] if len(passive_block) > 1 else []
    passive_effect = _group_passive_lines(passive_effect_lines)

    active_name, active_effect = _clean_active(sections.get("Active Skill") or [])
    activation_conditions = _clean_activation(sections.get("Activation Condition(s)") or [])
    link_skills = _clean_links(sections.get("Link Skills") or [])

    categories = parse_categories_from_soup(soup)
    stats = _parse_stats(sections.get("Stats") or [], page_text)
    release_date, tz = _parse_release(page_text)

    # Collect image URLs (not downloading in this version)
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

    # Rarity and Type (DOM-first)
    rarity = detect_rarity_from_dom(soup, image_urls)
    type_token = detect_type_token_from_dom(soup)
    type_token_upper = type_token.upper() if type_token else None

    # Type icon filename (if needed later)
    type_icon = None
    for url in image_urls:
        if "cha_type_icon_" in url.lower():
            type_icon = url.split("/")[-1]
            break

    h1 = soup.select_one("h1")
    base_display_name = (
        h1.get_text(strip=True)
        if (h1 and h1.get_text(strip=True))
        else (soup.title.string.strip() if (soup.title and soup.title.string) else "")
    )
    page_title = soup.title.string.strip() if soup.title and soup.title.string else ""

    # Compose display names
    prefix_parts = []
    if rarity:
        prefix_parts.append(rarity)
    if type_token_upper:
        prefix_parts.append(f"[{type_token_upper}]")
    prefix = " ".join(prefix_parts)
    display_name_with_type = f"{prefix} {base_display_name}".strip() if prefix else base_display_name
    display_name_with_type_bracketed = f"{prefix} [{base_display_name}]".strip() if prefix else f"[{base_display_name}]"

    char_id = extract_character_id_from_url(page_url)

    meta = {
        "page_title": page_title,
        "display_name": base_display_name,
        "display_name_with_type": display_name_with_type,
        "display_name_with_type_bracketed": display_name_with_type_bracketed,
        "character_id": char_id,
        "release_date": release_date,
        "timezone": tz,
        "leader_skill": leader_skill,
        "super_attack": {"name": super_name, "effect": super_effect},
        "ultra_super_attack": {"name": ultra_name, "effect": ultra_effect},
        "passive_skill": {"name": passive_name, "effect": passive_effect},
        "active_skill": {
            "name": active_name,
            "effect": active_effect,
            "activation_conditions": activation_conditions,
        },
        "link_skills": link_skills,
        "categories": categories,
        "stats": stats,
        "source_url": page_url,
        "rarity_detected": rarity,
        "type_token": type_token,
        "type_token_upper": type_token_upper,
        "type_icon_filename": type_icon,
        "image_urls": image_urls,
    }
    return meta

def write_card_outputs_and_update_index(meta: Dict[str, object], index: Dict[str, dict]) -> None:
    base_display_name = meta.get("display_name") or "Unknown Card"
    display_name_with_type = meta.get("display_name_with_type") or base_display_name
    display_name_with_type_bracketed = meta.get("display_name_with_type_bracketed") or f"[{base_display_name}]"
    rarity = meta.get("rarity_detected")
    type_token_upper = meta.get("type_token_upper")
    char_id = meta.get("character_id") or "unknown"

    # Folder name uses bracketed format
    folder_name = sanitize_filename(f"{display_name_with_type_bracketed} - {char_id}")
    card_dir = OUTROOT / folder_name
    card_dir.mkdir(parents=True, exist_ok=True)

    # Compose PAGE_TEXT for quick view
    parts = []
    def add_line(k, v):
        if v:
            parts.append(f"{k}: {v}" if isinstance(v, str) else f"{k}: {json.dumps(v, ensure_ascii=False)}")

    add_line("leader_skill", meta.get("leader_skill"))
    add_line("super_attack", meta.get("super_attack"))
    add_line("ultra_super_attack", meta.get("ultra_super_attack"))
    add_line("passive_skill", meta.get("passive_skill"))
    add_line("active_skill", meta.get("active_skill"))
    add_line("link_skills", meta.get("link_skills"))
    add_line("categories", meta.get("categories"))
    add_line("stats", meta.get("stats"))
    add_line("rarity_detected", rarity)
    add_line("type_token", meta.get("type_token"))

    page_text = "\n".join(parts)
    (card_dir / "PAGE_TEXT.txt").write_text(page_text, encoding="utf-8")

    # Full metadata
    (card_dir / "METADATA.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Attribution
    src = meta.get("source_url") or ""
    (card_dir / "ATTRIBUTION.txt").write_text(
        "Data and image asset links collected from DokkanInfo.\n"
        f"Source page: {src}\n"
        "Site: https://dokkaninfo.com\n\n"
        "Notes:\n"
        "- Personal/educational use.\n"
        "- Respect the site's Terms and original owners' rights.\n"
        "- If you share output, credit: “Data/images via dokkaninfo.com”.\n",
        encoding="utf-8",
    )

    # Update index
    if char_id and char_id != "unknown":
        index[char_id] = {
            "url": meta.get("source_url"),
            "display_name": base_display_name,
            "display_name_with_type": display_name_with_type,
            "display_name_with_type_bracketed": display_name_with_type_bracketed,
            "rarity": rarity,
            "type": type_token_upper,
            "folder": str(card_dir),
            "saved_at": datetime.utcnow().isoformat() + "Z",
        }
        save_index(index)
        logging.info("Index updated for ID %s", char_id)

# ------------ Main orchestration -------------
def main():
    log_path = setup_logging()
    logging.info("Starting DokkanInfo scraper (headed) — index paging, cap after MAX_NEW_CARDS, skip already indexed, parse related IDs (skip first tile), bracketed folder name")
    OUTROOT.mkdir(parents=True, exist_ok=True)

    # Load persistent index and seed seen_ids from it
    index = load_index()
    seen_ids: Set[str] = set(index.keys())
    if seen_ids:
        logging.info("Loaded %d existing IDs from index; they will be skipped.", len(seen_ids))

    new_cards_saved = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headed
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        current_index_url = INDEX_URL
        pages_done = 0

        while pages_done < MAX_PAGES:
            if new_cards_saved >= MAX_NEW_CARDS:
                logging.info("Reached MAX_NEW_CARDS=%d; stopping crawl.", MAX_NEW_CARDS)
                break

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
                if not h or not h.startswith("/cards/"):
                    continue
                if h in seen_href:
                    continue
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

            # ---- Process cards on this page ----
            for i, card_url in enumerate(links, start=1):
                if new_cards_saved >= MAX_NEW_CARDS:
                    logging.info("Reached MAX_NEW_CARDS=%d; stopping crawl.", MAX_NEW_CARDS)
                    break

                url_id = extract_character_id_from_url(card_url)
                if url_id and url_id in seen_ids:
                    logging.info("Page card %d/%d: ID %s already indexed — skipping open.", i, len(links), url_id)
                    continue

                logging.info("Page card %d/%d -> %s", i, len(links), card_url)
                try:
                    page.goto(card_url, wait_until="domcontentloaded", timeout=TIMEOUT)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.25)")
                    page.wait_for_timeout(800)
                except PWTimeoutError as e:
                    logging.warning("Card load timeout: %s", e)
                    continue

                page_html = page.content()
                meta = scrape_card_from_html(page_html, card_url)

                char_id = meta.get("character_id")
                if char_id and char_id in seen_ids:
                    logging.info("Card %s already scraped; skipping save.", char_id)
                else:
                    if char_id:
                        seen_ids.add(char_id)
                    write_card_outputs_and_update_index(meta, index)
                    new_cards_saved += 1
                    logging.info("Scraped card ID %s (new_cards_saved=%d)", char_id or "unknown", new_cards_saved)

                # Related IDs from header .col-5 (skip first tile)
                related_ids = extract_ids_from_col5_images(page_html)
                if related_ids:
                    logging.info("Found %d related IDs (skipping first tile): %s", len(related_ids), ", ".join(related_ids))
                else:
                    logging.info("No related IDs found on this page.")

                # Visit each related card directly
                for rid in related_ids:
                    if new_cards_saved >= MAX_NEW_CARDS:
                        logging.info("Reached MAX_NEW_CARDS=%d; stopping crawl.", MAX_NEW_CARDS)
                        break
                    if rid in seen_ids:
                        logging.info("Related ID %s already indexed; skipping.", rid)
                        continue

                    rel_url = f"{BASE}/cards/{rid}"
                    logging.info("Opening related URL directly: %s", rel_url)
                    try:
                        page.goto(rel_url, wait_until="domcontentloaded", timeout=TIMEOUT)
                        page.wait_for_timeout(700)
                    except PWTimeoutError as e:
                        logging.warning("Related page load timeout for %s: %s", rel_url, e)
                        continue

                    rel_html = page.content()
                    rel_meta = scrape_card_from_html(rel_html, rel_url)
                    rel_id_final = rel_meta.get("character_id") or rid
                    if rel_id_final in seen_ids:
                        logging.info("ID %s already scraped after load; skipping save.", rel_id_final)
                        continue
                    seen_ids.add(rel_id_final)
                    write_card_outputs_and_update_index(rel_meta, index)
                    new_cards_saved += 1
                    logging.info("Scraped related card ID %s (new_cards_saved=%d)", rel_id_final, new_cards_saved)

                    time.sleep(SLEEP_BETWEEN_CARDS)

                time.sleep(SLEEP_BETWEEN_CARDS)

            if new_cards_saved >= MAX_NEW_CARDS:
                logging.info("Reached MAX_NEW_CARDS=%d; stopping crawl.", MAX_NEW_CARDS)
                break

            # After finishing this page of cards, advance to next index page
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
