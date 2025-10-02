# scrapeDokkanInfo_textparse_v2_4.py
# DokkanInfo scraper (text-driven parsing + robust DOM strategies for Categories)
# Python 3.9 compatible

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------ Config -------------
BASE = "https://dokkaninfo.com"
INDEX_URL = f"{BASE}/cards?sort=open_at"
OUTROOT = Path("output/cards")
LOGDIR = Path("output/logs")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS_DL = {"User-Agent": USER_AGENT, "Referer": BASE}
TIMEOUT = 60_000
LIMIT_CARDS = 2
SLEEP_BETWEEN_CARDS = 0

HEADLESS = False
SLOW_MO_MS = 200
ENABLE_TRACE = True

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

def detect_rarity_and_type_from_images(image_urls: List[str]) -> Tuple[Optional[str], Optional[str]]:
    rarity = None
    patterns = {
        "LR": ["cha_rare_sm_lr", "cha_rare_lr", "/lr."],
        "UR": ["cha_rare_sm_ur", "cha_rare_ur"],
        "SSR": ["cha_rare_sm_ssr", "cha_rare_ssr"],
        "SR": ["cha_rare_sm_sr", "cha_rare_sr"],
        "R": ["cha_rare_sm_r", "cha_rare_r"],
        "N": ["cha_rare_sm_n", "cha_rare_n"],
    }
    for url in image_urls:
        low = url.lower()
        for label, needles in patterns.items():
            if any(n in low for n in needles):
                rarity = label
                break
        if rarity:
            break

    type_icon = None
    for url in image_urls:
        if "cha_type_icon_" in url:
            type_icon = Path(urlparse(url).path).name
            break

    logging.debug("Rarity detected: %s, type icon: %s", rarity, type_icon)
    return rarity, type_icon

def download_assets(urls: List[str], dest_dir: Path) -> List[str]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    saved: List[str] = []
    for url in urls:
        try:
            parsed = urlparse(url)
            path = Path(parsed.path)
            subdir = dest_dir / Path(*[p for p in path.parts[:-1] if p not in ("/", "")])
            subdir.mkdir(parents=True, exist_ok=True)
            target = subdir / path.name

            if target.exists() and target.stat().st_size > 0:
                saved.append(str(target))
                continue

            with requests.get(url, headers=HEADERS_DL, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(target, "wb") as f:
                    for chunk in r.iter_content(65536):
                        if chunk:
                            f.write(chunk)
            saved.append(str(target))
        except Exception as e:
            logging.warning("Asset failed: %s -> %s", url, e)
    return saved

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

def _clean_leader(block: List[str]) -> Optional[str]:
    if not block:
        return None
    leader = block[0].strip()
    # Drop immediate duplication of an identical sentence
    parts = [p.strip() for p in re.split(r'(?<=[.])\s+', leader) if p.strip()]
    seen = set()
    dedup = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            dedup.append(p)
    leader = " ".join(dedup)
    # Specific common duplication on this site
    leader = re.sub(
        r'("Exploding Rage"\s*Category\s+Ki\s*\+\d+\s+and\s+HP,\s*ATK\s*&\s*DEF\s*\+\d+%)\s*\1',
        r"\1",
        leader,
        flags=re.IGNORECASE,
    )
    return leader

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
    # Remove headers if any snuck in
    lines = [ln for ln in lines if ln not in HEADERS]
    # Normalize "Basic effect(s)"
    lines = [("Basic effect(s):" if re.fullmatch(r"Basic effect\(s\)", ln, flags=re.IGNORECASE) else ln) for ln in lines]
    # Ensure "Activates the Entrance Animation..." leads the effect block
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
        r"^Basic effect\(s\):",
    ]
    def is_leading(s: str) -> bool:
        for pat in leading_patterns:
            if re.search(pat, s, flags=re.IGNORECASE):
                return True
        return False

    groups: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if is_leading(ln) and cur:
            groups.append(cur); cur = [ln]
        else:
            if not cur: cur = [ln]
            else: cur.append(ln)
    if cur: groups.append(cur)

    out_parts: List[str] = []
    for g in groups:
        g = [x for x in g if x and x not in HEADERS]
        if not g: continue
        clause = " ".join(g)
        clause = _condense_spaces(clause)
        clause = re.sub(r"^(Basic effect\(s\))\s*:?(\s*)", r"\1:\2", clause, flags=re.IGNORECASE)
        clause = re.sub(r"^(For every [^.]+?)(?!:)\s", r"\1: ", clause, flags=re.IGNORECASE)
        out_parts.append(clause)

    effect = "; ".join(out_parts)
    effect = re.sub(r"\s*;\s*", "; ", effect)
    return effect.strip()

def _clean_active(block: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not block:
        return None, None
    name = block[0]
    body = []
    for ln in block[1:]:
        if ln in HEADERS or re.fullmatch(r"Link Skills", ln, re.IGNORECASE):
            break
        body.append(ln)
    effect = "; ".join([_condense_spaces(b) for b in body if b])
    effect = _condense_spaces(effect)
    return (name or None), (effect or None)

def _clean_activation(block: List[str]) -> Optional[str]:
    if not block:
        return None
    text = " ".join(block)
    text = _condense_spaces(text)
    for h in HEADERS:
        text = text.replace(h, "")
    return text.strip() or None

def _clean_links(block: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for ln in block or []:
        s = _condense_spaces(ln)
        if not s: continue
        if s in seen: continue
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

# ------------ Main -------------
def main():
    log_path = setup_logging()
    logging.info("Starting DokkanInfo scraper (non-headless)")

    OUTROOT.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        logging.info("Launching Chromium (headless=%s, slow_mo=%sms)", HEADLESS, SLOW_MO_MS)
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        context = browser.new_context(user_agent=USER_AGENT, locale="en-US", viewport={"width": 1400, "height": 900})
        page = context.new_page()

        def _browser_console(msg):
            try:
                t = msg.type() if callable(getattr(msg, "type", None)) else getattr(msg, "type", None)
                text = msg.text() if callable(getattr(msg, "text", None)) else getattr(msg, "text", None)
                logging.debug("BROWSER %s: %s", t, text)
            except Exception as e:
                logging.debug("BROWSER console log skipped (%s)", e)
        page.on("console", _browser_console)

        trace_path = None
        try:
            if ENABLE_TRACE:
                trace_path = LOGDIR / f"trace-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
                logging.info("Tracing enabled -> %s", trace_path)
                try:
                    context.tracing.start(screenshots=True, snapshots=True, sources=False)
                except Exception as e:
                    logging.warning("Tracing start failed: %s", e)
        except Exception as e:
            logging.warning("Tracing init failed: %s", e)

        try:
            logging.info("Opening index: %s", INDEX_URL)
            page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT)
            page.wait_for_timeout(1200)

            hrefs = page.eval_on_selector_all(
                'a.col-auto[href^="/cards/"]',
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
            )
            links = [urljoin(BASE, h) for h in hrefs if h and h.startswith("/cards/")]
            logging.info("Found %d card links on screen", len(links))
            logging.debug("First 10 links: %s", links[:10])

            if not links:
                raise RuntimeError("No card anchors found matching a.col-auto[href^='/cards/'] on the index.")

            for i, card_url in enumerate(links[:LIMIT_CARDS], start=1):
                logging.info("Processing card %d/%d -> %s", i, min(LIMIT_CARDS, len(links)), card_url)
                page.goto(card_url, wait_until="domcontentloaded", timeout=TIMEOUT)
                page.wait_for_timeout(1500)

                # Screenshot
                shot_dir = LOGDIR / "screens"
                shot_dir.mkdir(parents=True, exist_ok=True)
                shot_file = shot_dir / f"card-{i}.png"
                try:
                    img_bytes = page.screenshot(full_page=True)
                    shot_file.write_bytes(img_bytes)
                    logging.info("Saved page screenshot: %s", shot_file)
                except Exception as e:
                    logging.warning("Screenshot failed: %s", e)

                # ---- Sources ----
                page_text = page.inner_text("body")
                page_html = page.content()

                image_urls = page.eval_on_selector_all(
                    "img",
                    "els => els.map(e => e.getAttribute('src')).filter(Boolean)",
                )
                abs_urls = []
                seen_urls = set()
                for s in image_urls:
                    try:
                        u = urljoin(page.url, s)
                        if u not in seen_urls:
                            seen_urls.add(u); abs_urls.append(u)
                    except Exception:
                        continue
                image_urls = abs_urls
                logging.info("Found %d images", len(image_urls))

                # ---- Parse TEXT sections ----
                sections = _split_sections(page_text)

                # Leader
                leader_skill = _clean_leader(sections.get("Leader Skill") or [])

                # Super / Ultra from text blocks
                super_name, super_effect = _clean_super_like(sections.get("Super Attack") or [])
                ultra_name, ultra_effect = _clean_super_like(sections.get("Ultra Super Attack") or [])

                # --- Fallbacks to guarantee Super/Ultra ---
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

                # Passive
                passive_block = sections.get("Passive Skill") or []
                passive_name = passive_block[0] if passive_block else None
                passive_effect_lines = passive_block[1:] if len(passive_block) > 1 else []
                passive_effect = _group_passive_lines(passive_effect_lines)

                # Active + Activation
                active_name, active_effect = _clean_active(sections.get("Active Skill") or [])
                activation_conditions = _clean_activation(sections.get("Activation Condition(s)") or [])

                # Link Skills
                link_skills = _clean_links(sections.get("Link Skills") or [])

                # -------------- Categories (robust DOM strategies) --------------
                # Strategy 1: <a href="/categories/..."><img alt="..."></a>
                cats1 = page.eval_on_selector_all(
                    'a[href*="/categories/"] img',
                    'els => els.map(e => e.getAttribute("alt") || e.getAttribute("title") || "").filter(Boolean)',
                )
                logging.debug("Categories strategy1 (a[href*='/categories/'] img): %s", cats1)

                # Strategy 2: label sprites anywhere: img[src*="/card_category/label/"]
                cats2 = page.eval_on_selector_all(
                    'img[src*="/card_category/label/"]',
                    'els => els.map(e => e.getAttribute("alt") || e.getAttribute("title") || "").filter(Boolean)',
                )
                logging.debug("Categories strategy2 (img[src*='/card_category/label/']): %s", cats2)

                # Strategy 3 (fallback): between "Categories" and next header, collect image alts/titles + anchor text
                cats3 = page.evaluate(
                    """(HEADERS) => {
                        const out = [];
                        const push = (v) => { if (v && String(v).trim()) out.push(String(v).trim()); };
                        const all = Array.from(document.querySelectorAll('body *'));
                        const textOf = el => (el && (el.textContent || '').trim()) || '';
                        const isHeaderText = (txt) => HEADERS.includes((txt || '').trim());

                        let catEl = null;
                        for (const el of all) {
                          if (textOf(el) === 'Categories') { catEl = el; break; }
                        }
                        if (!catEl) return [];

                        let start = false, nextHeader = null;
                        for (const el of all) {
                          if (el === catEl) { start = true; continue; }
                          if (!start) continue;
                          if (isHeaderText(textOf(el))) { nextHeader = el; break; }
                        }

                        const between = [];
                        start = false;
                        for (const el of all) {
                          if (el === catEl) { start = true; continue; }
                          if (!start) continue;
                          if (nextHeader && el === nextHeader) break;
                          between.push(el);
                        }

                        between.forEach(el => {
                          if ((el.tagName || '').toUpperCase() === 'IMG') {
                            const src = el.getAttribute('src') || '';
                            if (/\\/card_category\\/label\\//i.test(src)) {
                              push(el.getAttribute('alt') || '');
                              push(el.getAttribute('title') || '');
                            }
                          }
                          if ((el.tagName || '').toUpperCase() === 'A') {
                            const href = el.getAttribute('href') || '';
                            if (/\\/categories\\//i.test(href)) {
                              push(textOf(el));
                            }
                          }
                        });
                        return out;
                    }""",
                    HEADERS,
                )
                logging.debug("Categories strategy3 (between header): %s", cats3)

                # Merge (priority: 1, then 2, then 3), then clean/dedup
                merged_cats = []
                seen_cat = set()
                for pool in (cats1, cats2, cats3):
                    for c in pool or []:
                        s = (c or "").strip()
                        if not s: continue
                        if s in seen_cat: continue
                        seen_cat.add(s)
                        merged_cats.append(s)

                categories = _clean_categories_python(merged_cats)
                logging.info("Categories merged/cleaned (%d): %s", len(categories), categories)

                # Stats + release + rarity/type
                stats = _parse_stats(sections.get("Stats") or [], page_text)
                release_date, tz = _parse_release(page_text)
                rarity, type_icon = detect_rarity_and_type_from_images(image_urls)

                # Names/titles
                try:
                    h1 = page.text_content("h1") or ""
                    display_name = h1.strip() if h1.strip() else (page.title() or "").strip()
                except Exception:
                    display_name = (page.title() or "").strip()
                page_title = page.title()

                # Folder & writes
                prefix = f"{rarity} " if rarity else ""
                folder_name = sanitize_filename(f"{prefix}{display_name or 'Unknown Card'}")
                card_dir = OUTROOT / folder_name
                assets_dir = card_dir / "assets"
                card_dir.mkdir(parents=True, exist_ok=True)

                (card_dir / "page.html").write_text(page_html, encoding="utf-8")
                (card_dir / "PAGE_TEXT.txt").write_text(page_text, encoding="utf-8")
                logging.info("Saved page sources to %s", card_dir)

                meta = {
                    "page_title": page_title,
                    "display_name": display_name,
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
                    "source_url": card_url,
                    "rarity_detected": rarity,
                    "type_icon_filename": type_icon,
                    "image_urls": image_urls,
                }
                (card_dir / "METADATA.json").write_text(
                    json.dumps(meta, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                logging.info("Wrote METADATA.json")

                saved = download_assets(image_urls, assets_dir)
                logging.info("Saved %d assets into %s", len(saved), assets_dir)

                (card_dir / "ATTRIBUTION.txt").write_text(
                    "Data and image asset links collected from DokkanInfo.\n"
                    f"Source page: {card_url}\n"
                    "Site: https://dokkaninfo.com\n\n"
                    "Notes:\n"
                    "- Personal/educational use.\n"
                    "- Respect the site's Terms and original owners' rights.\n"
                    "- If you share output, credit: “Data/images via dokkaninfo.com”.\n",
                    encoding="utf-8",
                )
                logging.info("Wrote attribution file")

                time.sleep(SLEEP_BETWEEN_CARDS)

        except PWTimeoutError as e:
            logging.exception("Playwright timeout: %s", e)
        except Exception as e:
            logging.exception("Unexpected error: %s", e)
        finally:
            if ENABLE_TRACE:
                try:
                    if hasattr(context.tracing, "export"):
                        context.tracing.stop()
                        try:
                            context.tracing.export(path=str(trace_path))
                            logging.info("Saved trace: %s", trace_path)
                        except Exception as ee:
                            logging.warning("Trace export failed: %s", ee)
                    else:
                        context.tracing.stop(path=str(trace_path))
                        logging.info("Saved trace (stop with path): %s", trace_path)
                except Exception as te:
                    logging.warning("Tracing stop failed: %s", te)
            browser.close()
            logging.info("Browser closed. Log file: %s", log_path)


if __name__ == "__main__":
    main()
