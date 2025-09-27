# scrape_card_assets_playwright.py
# Non-headless with detailed logging and optional Playwright trace
#
# Setup (Windows, PowerShell):
#   python -m venv .venv
#   . .\.venv\Scripts\Activate.ps1
#   pip install playwright requests
#   python -m playwright install chromium
#
# Run:
#   python scrape_card_assets_playwright.py

import json
import re
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

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
LIMIT_CARDS = 1
SLEEP_BETWEEN_CARDS = 1.0

# Playwright UI behavior
HEADLESS = False        # << non-headless
SLOW_MO_MS = 200        # slow motion so you can see it work

# Collect a Playwright trace.zip you can send me (set to True if helpful)
ENABLE_TRACE = True

# Exact labels as rendered on the page
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

def setup_logging() -> Path:
    LOGDIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = LOGDIR / f"run-{stamp}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # file handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # console handler (concise)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    # reset handlers if re-run in same session
    for h in list(logger.handlers):
        logger.removeHandler(h)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.info("Logging to %s", log_path)
    return log_path

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

def main():
    log_path = setup_logging()
    logging.info("Starting DokkanInfo scraper (non-headless)")

    OUTROOT.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        logging.info("Launching Chromium (headless=%s, slow_mo=%sms)", HEADLESS, SLOW_MO_MS)
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        context = browser.new_context(user_agent=USER_AGENT, locale="en-US", viewport={"width": 1400, "height": 900})
        page = context.new_page()

        # capture console logs from the page
        def _browser_console(msg):
            try:
                # In Playwright Python, .type and .text are attributes, not callables
                logging.debug("BROWSER %s: %s", getattr(msg, "type", None), getattr(msg, "text", None))
            except Exception as e:
                logging.debug("BROWSER console log skipped (%s)", e)

        page.on("console", _browser_console)

        trace_path = None
        try:
            if ENABLE_TRACE:
                trace_path = LOGDIR / f"trace-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
                logging.info("Tracing enabled -> %s", trace_path)
                context.tracing.start(screenshots=True, snapshots=True, sources=False)

            logging.info("Opening index: %s", INDEX_URL)
            page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT)
            page.wait_for_timeout(1200)

            hrefs = page.eval_on_selector_all(
                'a.col-auto[href^="/cards/"]',
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
            )
            links = [urljoin(BASE, h) for h in hrefs if h.startswith("/cards/")]

            logging.info("Found %d card links on screen", len(links))
            logging.debug("First 10 links: %s", links[:10])

            if not links:
                raise RuntimeError("No card anchors found matching a.col-auto[href^='/cards/'] on the index.")

            for i, card_url in enumerate(links[:LIMIT_CARDS], start=1):
                logging.info("Processing card %d/%d -> %s", i, min(LIMIT_CARDS, len(links)), card_url)
                page.goto(card_url, wait_until="domcontentloaded", timeout=TIMEOUT)
                page.wait_for_timeout(1500)

                shot_dir = LOGDIR / "screens"
                shot_dir.mkdir(parents=True, exist_ok=True)
                shot_file = shot_dir / f"card-{i}.png"
                try:
                    # Return bytes and write them ourselves (avoids kwarg `path=`)
                    img_bytes = page.screenshot(full_page=True)
                    shot_file.write_bytes(img_bytes)
                    logging.info("Saved page screenshot: %s", shot_file)
                except Exception as e:
                    logging.warning("Screenshot failed (writing bytes fallback already tried): %s", e)

                # ---- DOM-ORDER SECTION SLICER (browser context) ----
                logging.debug("Evaluating DOM to extract sections...")
                data = page.evaluate(
                    """(headers) => {
                        const isVisible = (el) => {
                          const style = window.getComputedStyle(el);
                          return style.display !== 'none' &&
                                 style.visibility !== 'hidden' &&
                                 (el.offsetParent !== null || style.position === 'fixed');
                        };

                        const all = Array.from(document.querySelectorAll('body *')).filter(isVisible);
                        const page_title   = document.title.trim();
                        const display_name = (document.querySelector('h1')?.textContent || page_title || '').trim();

                        const headerPos = [];
                        all.forEach((el, idx) => {
                          const t = el.textContent.trim();
                          if (headers.includes(t)) headerPos.push({label: t, index: idx});
                        });

                        function sliceBetween(aIdx, bIdx){
                          const els = all.slice(aIdx + 1, bIdx === -1 ? undefined : bIdx);
                          const frag = document.createElement('div');
                          els.forEach(e => frag.appendChild(e.cloneNode(true)));
                          return {
                            text: frag.innerText.replace(/\\n{2,}/g,'\\n').trim(),
                            html: frag.innerHTML
                          };
                        }

                        const sections = {};
                        for (let i=0; i<headerPos.length; i++){
                          const cur = headerPos[i];
                          const next = headerPos[i+1];
                          const endIdx = next ? next.index : -1;
                          sections[cur.label] = sliceBetween(cur.index, endIdx);
                        }

                        const G = (k) => sections[k] ? sections[k].text : null;
                        const H = (k) => sections[k] ? sections[k].html : "";

                        function splitNameDetails(text){
                          if (!text) return {name:null, details:null};
                          const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
                          if (lines.length === 0) return {name:null, details:null};
                          const name = lines[0];
                          const details = lines.slice(1).join(' ').replace(/\\s+/g,' ').trim() || null;
                          return {name, details};
                        }

                        // --- Super Attack robust parser ---
                        function parseSuper(superHtml){
                          const tmp = document.createElement('div');
                          tmp.innerHTML = superHtml || "";

                          // remove embedded Ultra header if present
                          const nodes = tmp.querySelectorAll('*');
                          let cut = null;
                          nodes.forEach(n => {
                            const t = n.textContent.trim();
                            if (t === 'Ultra Super Attack' && !cut) cut = n;
                          });
                          if (cut){
                            let parent = cut.parentNode;
                            cut.remove();
                            while (parent && parent.nextSibling) {
                              parent.parentNode.removeChild(parent.nextSibling);
                            }
                          }

                          const rawLines = tmp.innerText.split('\\n').map(s => s.trim()).filter(Boolean);

                          const seen = new Set();
                          const lines = [];
                          rawLines.forEach(l => {
                            if (/^\\d+\\s*%$/.test(l)) return;
                            if (/SA\\s*Lv/i.test(l)) return;
                            if (!seen.has(l)) { lines.push(l); seen.add(l); }
                          });

                          let name = null, effect = null;
                          if (lines.length > 0){
                            name = lines[0];
                            const eff = lines.slice(1).filter(l => l !== name);
                            effect = eff.join(' ').replace(/\\s+/g, ' ').trim() || null;
                          }

                          if (effect) {
                            effect = effect
                              .replace(/\\s*Raises ATK & DEF\\s*Causes/gi, ' Raises ATK & DEF; Causes')
                              .replace(/\\s*Raises ATK & DEF\\s*$/i, ' Raises ATK & DEF');
                          }

                          return { name, effect };
                        }

                        // --- Active Skill robust parser ---
                        function parseActive(activeHtml, activationText){
                          const tmp = document.createElement('div');
                          tmp.innerHTML = activeHtml || "";

                          Array.from(tmp.querySelectorAll('*')).forEach(el => {
                            const t = el.textContent.trim();
                            if (t === 'Activation Condition(s)') el.remove();
                          });

                          const lines = tmp.innerText.split('\\n').map(s => s.trim()).filter(Boolean);

                          let name = null, effect = null;
                          if (lines.length > 0){
                            name = lines[0];

                            const seen = new Set();
                            const effLines = [];
                            lines.slice(1).forEach(l => {
                              if (!seen.has(l)) { effLines.append ? effLines.append(l) : effLines.push(l); seen.add(l); }
                            });

                            effect = effLines.join(' ').replace(/\\s+/g,' ').trim() || null;
                            if (effect && name && effect.startsWith(name)) {
                              effect = effect.slice(name.length).trim();
                            }
                          }

                          let activation = (activationText || '').split('\\n').map(s => s.trim()).filter(Boolean);
                          const aSeen = new Set(); const aOut = [];
                          activation.forEach(l => { if (!aSeen.has(l)) { aSeen.add(l); aOut.push(l); } });
                          const activation_clean = aOut.join(' ').replace(/\\s+/g,' ').trim() || null;

                          return { name, effect, activation: activation_clean };
                        }

                        function pickNames(sectionHtml, mode){
                          const tmp = document.createElement('div');
                          tmp.innerHTML = sectionHtml || '';
                          const out = [];
                          const seen = new Set();

                          const nodes = tmp.querySelectorAll('a,button,span,div');
                          nodes.forEach(n => {
                            const t = n.textContent.trim();
                            if (!t || t.length > 40 || !/[A-Za-z]/.test(t)) return;

                            const a = n.closest('a');
                            const href = a?.getAttribute('href') || '';

                            if (mode === 'links') {
                              if (!/\\/links?\\//.test(href)) return;
                            } else if (mode === 'cats') {
                              if (!/\\/categor(y|ies)\\//.test(href)) return;
                            } else return;

                            if (/(ATK|DEF|Ki|\\+|%|Total:|Links:|Categories:|\\.png|\\d)/.test(t)) return;
                            if (!seen.has(t)) { out.push(t); seen.add(t); }
                          });

                          if (out.length === 0){
                            tmp.textContent.split('\\n').map(s => s.trim()).forEach(t => {
                              if (!t || t.length > 40) return;
                              if (/(ATK|DEF|Ki|\\+|%|Total:|Links:|Categories:|\\.png|\\d)/.test(t)) return;
                              if (!seen.has(t)) { out.push(t); seen.add(t); }
                            });
                          }
                          return out;
                        }

                        function parseStats(text){
                          const stats = {};
                          if (!text) return stats;
                          ['HP','ATK','DEF','Cost','Max Lv','SA Lv'].forEach(k => {
                            const m = new RegExp('(^|\\n)\\s*' + k.replace(' ','\\s+') + '\\s*[:\\n]\\s*([0-9,]+)','i').exec(text);
                            if (m) stats[k] = m[2].replace(/,/g,'');
                          });
                          return stats;
                        }

                        const leader_skill = G('Leader Skill');
                        const superParsed = parseSuper(H('Super Attack'));
                        const ultraRaw = splitNameDetails(G('Ultra Super Attack'));
                        const passiveRaw = splitNameDetails(G('Passive Skill'));
                        const activeParsed = parseActive(H('Active Skill'), G('Activation Condition(s)'));

                        const link_skills = pickNames(H('Link Skills'), 'links');
                        const categories  = pickNames(H('Categories'), 'cats');
                        const stats = parseStats(G('Stats') || '');

                        // Release Date
                        const bodyText = document.body.innerText;
                        let releaseDate = null, timezone = null;
                        const m = /Release Date\\s+([0-9/.-]+)\\s+([0-9: ]+[APMapm]{2})\\s+([A-Z]{2,4})/.exec(bodyText);
                        if (m){ releaseDate = m[1] + ' ' + m[2]; timezone = m[3]; }

                        const image_urls = Array.from(document.images)
                          .filter(isVisible)
                          .map(img => img.getAttribute('src'))
                          .filter(Boolean)
                          .map(s => new URL(s, location.origin).href);

                        return {
                          display_name, page_title,
                          release_date: releaseDate, timezone,
                          leader_skill,

                          super_attack_name: superParsed.name,
                          super_attack_effect: superParsed.effect,

                          ultra_super_attack_name: ultraRaw.name,
                          ultra_super_attack_effect: ultraRaw.details,

                          passive_skill_name: passiveRaw.name,
                          passive_skill_effect: passiveRaw.details,

                          active_skill_name: activeParsed.name,
                          active_skill_effect: activeParsed.effect,
                          activation_conditions: activeParsed.activation,

                          link_skills, categories, stats,
                          image_urls,
                          page_text: document.body.innerText.trim(),
                          page_html: document.documentElement.outerHTML
                        };
                    }""",
                    HEADERS,
                )

                # Log quick stats about extraction
                logging.info(
                    "Extracted sections -> name:%s | super:%s | ultra:%s | passive:%s | active:%s",
                    (data.get("display_name") or "")[:60],
                    f"{bool(data.get('super_attack_name'))}",
                    f"{bool(data.get('ultra_super_attack_name'))}",
                    f"{bool(data.get('passive_skill_name'))}",
                    f"{bool(data.get('active_skill_name'))}",
                )
                logging.debug("Link skills (%d): %s", len(data.get("link_skills") or []), data.get("link_skills"))
                logging.debug("Categories (%d): %s", len(data.get("categories") or []), data.get("categories"))
                logging.debug("Stats: %s", data.get("stats"))

                # ---- Write outputs ----
                image_urls = list(dict.fromkeys(data["image_urls"]))
                logging.info("Found %d images", len(image_urls))

                rarity, type_icon = detect_rarity_and_type_from_images(image_urls)
                display_name = data.get("display_name") or "Unknown Card"
                prefix = f"{rarity} " if rarity else ""
                folder_name = sanitize_filename(f"{prefix}{display_name}")

                card_dir = OUTROOT / folder_name
                assets_dir = card_dir / "assets"
                card_dir.mkdir(parents=True, exist_ok=True)

                # Page sources
                (card_dir / "page.html").write_text(data["page_html"], encoding="utf-8")
                (card_dir / "PAGE_TEXT.txt").write_text(data["page_text"], encoding="utf-8")
                logging.info("Saved page sources to %s", card_dir)

                # Clean metadata
                meta = {
                    "page_title": data.get("page_title"),
                    "display_name": display_name,
                    "release_date": data.get("release_date"),
                    "timezone": data.get("timezone"),
                    "leader_skill": data.get("leader_skill"),
                    "super_attack": {
                        "name": data.get("super_attack_name"),
                        "effect": data.get("super_attack_effect"),
                    },
                    "ultra_super_attack": {
                        "name": data.get("ultra_super_attack_name"),
                        "effect": data.get("ultra_super_attack_effect"),
                    },
                    "passive_skill": {
                        "name": data.get("passive_skill_name"),
                        "effect": data.get("passive_skill_effect"),
                    },
                    "active_skill": {
                        "name": data.get("active_skill_name"),
                        "effect": data.get("active_skill_effect"),
                        "activation_conditions": data.get("activation_conditions"),
                    },
                    "link_skills": data.get("link_skills") or [],
                    "categories": data.get("categories") or [],
                    "stats": data.get("stats") or {},
                    "source_url": card_url,
                    "rarity_detected": rarity,
                    "type_icon_filename": type_icon,
                    "image_urls": image_urls,
                }
                meta_path = card_dir / "METADATA.json"
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                logging.info("Wrote METADATA.json")

                # Assets
                saved = download_assets(image_urls, assets_dir)
                logging.info("Saved %d assets into %s", len(saved), assets_dir)

                # Attribution
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
                    context.tracing.stop()  # stop without args
                    try:
                        context.tracing.export(path=str(trace_path))
                        logging.info("Saved trace: %s", trace_path)
                    except Exception as ee:
                        logging.warning("Trace export failed: %s", ee)
                except Exception as te:
                    logging.warning("Tracing stop failed: %s", te)
            browser.close()
            logging.info("Browser closed. Log file: %s", log_path)


if __name__ == "__main__":
    main()
