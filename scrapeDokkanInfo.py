# collect_dokkan_debug_bundle_v2.py
# One-shot diagnostic bundle for DokkanInfo cards (first link on "Newest" page).
# Always saves:
#   - page.html, PAGE_TEXT.txt
#   - screenshot.png
#   - debug.json (rich: sections, parsing steps, anchors, stats, images, env)
#   - console.json, network.json
#   - trace.zip (Playwright trace) [compatible with older/newer versions]
#   - network.har (if your Playwright supports HAR)
#
# Setup:
#   python -m venv .venv
#   . .\.venv\Scripts\Activate.ps1
#   pip install playwright requests
#   python -m playwright install chromium
#
# Run:
#   python collect_dokkan_debug_bundle_v2.py
#
# It will print:
#   DEBUG_BUNDLE: output\debug\dokkan_debug-YYYYmmdd-HHMMSS.zip
# Send me that ZIP.

import json
import logging
import platform
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

from playwright.sync_api import TimeoutError as PWTimeoutError, sync_playwright

BASE = "https://dokkaninfo.com"
INDEX_URL = f"{BASE}/cards?sort=open_at"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HEADLESS = False
SLOW_MO_MS = 200
TIMEOUT = 60_000

OUTROOT = Path("output/debug")
OUTROOT.mkdir(parents=True, exist_ok=True)

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

@dataclass
class EnvInfo:
    python: str
    system: str
    release: str
    machine: str
    playwright: str

def get_env_info() -> EnvInfo:
    try:
        import playwright  # type: ignore
        pw_ver = getattr(playwright, "__version__", "unknown")
    except Exception:
        pw_ver = "unavailable"
    return EnvInfo(
        python=sys.version.replace("\n", " "),
        system=platform.system(),
        release=platform.release(),
        machine=platform.machine(),
        playwright=pw_ver,
    )

def css_path_js() -> str:
    # returns a JS function as string
    return r"""
    (function(el){
      if (!el) return null;
      const parts = [];
      while (el && el.nodeType === Node.ELEMENT_NODE && el !== document.body) {
        let selector = el.nodeName.toLowerCase();
        if (el.id) { selector += '#' + el.id; parts.unshift(selector); break; }
        let sib = 1, prev = el;
        while ((prev = prev.previousElementSibling) != null) {
          if (prev.nodeName === el.nodeName) sib++;
        }
        selector += `:nth-of-type(${sib})`;
        parts.unshift(selector);
        el = el.parentElement;
      }
      return parts.join(' > ') || null;
    })
    """

def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"debug-run-{stamp}.log"
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

def safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True
                      )
    path.write_text(text or "", encoding="utf-8")

def safe_dump_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = OUTROOT / f"dokkan_debug-{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir = run_dir / "logs"
    setup_logging(log_dir)

    env = get_env_info()
    logging.info(
        "Env: Python=%s | Playwright=%s | OS=%s %s (%s)",
        env.python.split()[0], env.playwright, env.system, env.release, env.machine
    )

    console_events: List[Dict[str, str]] = []
    responses: List[Dict[str, str]] = []
    failures: List[Dict[str, str]] = []

    with sync_playwright() as p:
        # HAR support (if available on your version)
        context_kwargs = dict(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
        )
        har_path: Optional[Path] = run_dir / "network.har"
        try:
            browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
            context = browser.new_context(**context_kwargs, record_har_path=str(har_path), record_har_content="embed")
            logging.info("HAR recording enabled -> %s", har_path)
        except TypeError:
            browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
            context = browser.new_context(**context_kwargs)
            har_path = None
            logging.info("HAR recording not supported on this Playwright; continuing without it.")

        page = context.new_page()

        # listeners
        def on_console(msg):
            try:
                console_events.append({
                    "type": str(getattr(msg, "type", None)),
                    "text": str(getattr(msg, "text", None)),
                })
                logging.debug("BROWSER %s: %s", getattr(msg, "type", None), getattr(msg, "text", None))
            except Exception:
                pass

        def on_response(resp):
            try:
                req = resp.request
                url = resp.url
                status = resp.status
                rtype = getattr(req, "resource_type", lambda: "unknown")()
                ct = resp.headers.get("content-type", "")
                if "dokkaninfo.com" in url or status >= 400:
                    responses.append({
                        "url": url, "status": str(status), "type": rtype, "content_type": ct
                    })
            except Exception:
                pass

        def on_failed(req):
            try:
                failures.append({"url": req.url, "method": req.method, "type": req.resource_type})
                logging.debug("REQUEST FAILED: %s", req.url)
            except Exception:
                pass

        page.on("console", on_console)
        page.on("response", on_response)
        page.on("requestfailed", on_failed)

        # tracing (compatible with multiple Playwright versions)
        trace_zip = run_dir / "trace.zip"
        tracing_started = False
        try:
            context.tracing.start(screenshots=True, snapshots=True, sources=False)
            tracing_started = True
            logging.info("Tracing started.")
        except Exception as e:
            logging.info("Tracing not started (%s)", e)

        page_html_text = ""
        page_plain_text = ""
        debug_obj: dict = {}

        try:
            # Index
            logging.info("Opening index: %s", INDEX_URL)
            page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=TIMEOUT)
            page.wait_for_timeout(1200)
            hrefs = page.eval_on_selector_all(
                'a.col-auto[href^="/cards/"]',
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
            )
            links = [urljoin(BASE, h) for h in hrefs if h.startswith("/cards/")]
            logging.info("Found %d card links on screen", len(links))
            if not links:
                raise RuntimeError("No card anchors found matching a.col-auto[href^='/cards/'] on the index.")

            # First card
            target = links[0]
            logging.info("Opening first card: %s", target)
            page.goto(target, wait_until="domcontentloaded", timeout=TIMEOUT)
            page.wait_for_timeout(1500)

            # Screenshot ASAP
            try:
                (run_dir / "screenshot.png").write_bytes(page.screenshot(full_page=True))
                logging.info("Saved screenshot: %s", run_dir / "screenshot.png")
            except Exception as e:
                logging.warning("Screenshot failed: %s", e)

            # HTML + Text ASAP
            try:
                page_html_text = page.evaluate("() => document.documentElement.outerHTML") or ""
            except Exception:
                page_html_text = page.content()
            try:
                page_plain_text = page.evaluate("() => document.body.innerText") or ""
            except Exception:
                page_plain_text = ""
            safe_write_text(run_dir / "page.html", page_html_text)
            safe_write_text(run_dir / "PAGE_TEXT.txt", page_plain_text)

            # ===== FIXED evaluate: pass ONE object argument =====
            debug_obj = page.evaluate(
                """({ headers, cssPathFn }) => {
                  const cssPath = eval(cssPathFn);

                  const isVisible = (el) => {
                    const s = getComputedStyle(el);
                    return s.display !== 'none' && s.visibility !== 'hidden' &&
                           (el.offsetParent !== null || s.position === 'fixed');
                  };
                  const depthOf = (el) => { let d=0; for(let n=el; n && n !== document.body; n=n.parentElement) d++; return d; };
                  const isHeadingish = (el) => {
                    if (!isVisible(el)) return false;
                    const tn = el.tagName ? el.tagName.toUpperCase() : '';
                    if (/^H[1-6]$/.test(tn)) return true;
                    const role = el.getAttribute('role') || '';
                    if (/^heading$/i.test(role)) return true;
                    const cls = el.className ? String(el.className) : '';
                    if (/(title|header|heading)/i.test(cls)) return true;
                    return false;
                  };

                  const page_title = document.title.trim();
                  const display_name = (document.querySelector('h1')?.textContent || page_title || '').trim();

                  const allEls = Array.from(document.querySelectorAll('body *')).filter(isVisible);
                  const headerNodes = [];
                  for (const el of allEls) {
                    const txt = el.textContent.trim();
                    if (!headers.includes(txt)) continue;
                    if (!isHeadingish(el)) continue;
                    headerNodes.push({
                      label: txt, depth: depthOf(el),
                      tag: el.tagName.toLowerCase(),
                      css_path: cssPath(el),
                      el
                    });
                  }
                  if (headerNodes.length === 0) {
                    for (const el of allEls) {
                      const txt = el.textContent.trim();
                      if (headers.includes(txt)) headerNodes.push({
                        label: txt, depth: depthOf(el),
                        tag: el.tagName.toLowerCase(), css_path: cssPath(el), el
                      });
                    }
                  }

                  function nextHeaderIndex(i) {
                    const curDepth = headerNodes[i].depth;
                    for (let j=i+1; j<headerNodes.length; j++){
                      if (headerNodes[j].depth <= curDepth) return j;
                    }
                    return -1;
                  }

                  function collectBetween(startEl, endElExclusive){
                    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT, {
                      acceptNode(node){
                        const s = getComputedStyle(node);
                        if (s.display === 'none' || s.visibility === 'hidden') return NodeFilter.FILTER_REJECT;
                        if (node.offsetParent === null && s.position !== 'fixed') return NodeFilter.FILTER_REJECT;
                        return NodeFilter.FILTER_ACCEPT;
                      }
                    });
                    const frag = document.createElement('div');
                    let started = false;
                    while(walker.nextNode()){
                      const node = walker.currentNode;
                      if (node === startEl) { started = true; continue; }
                      if (!started) continue;
                      if (endElExclusive && node === endElExclusive) break;
                      frag.appendChild(node.cloneNode(true));
                    }
                    const text = frag.innerText.replace(/\n{2,}/g,'\n').trim();
                    const html = frag.innerHTML;
                    return { text, html, html_len: html.length };
                  }

                  const sections = {};
                  const headers_debug = [];
                  const raw_sections = {};
                  for (let i=0; i<headerNodes.length; i++) {
                    const cur = headerNodes[i];
                    const j = nextHeaderIndex(i);
                    const endEl = j === -1 ? null : headerNodes[j].el;
                    const slice = collectBetween(cur.el, endEl);
                    sections[cur.label] = slice;
                    headers_debug.push({
                      label: cur.label,
                      depth: cur.depth,
                      tag: cur.tag,
                      css_path: cur.css_path,
                      html_len: slice.html_len,
                      text_preview: slice.text.slice(0, 200)
                    });
                    raw_sections[cur.label] = { text: slice.text, html: slice.html };
                  }

                  const G = (k) => sections[k] ? sections[k].text : '';
                  const H = (k) => sections[k] ? sections[k].html : '';

                  const dropNoise = (line) => {
                    if (!line) return false;
                    if (/^\d+\s*%$/.test(line)) return false;
                    if (/SA\s*Lv/i.test(line)) return false;
                    if (/^\s*$/.test(line)) return false;
                    return true;
                  };
                  const dedupLines = (arr) => {
                    const seen = new Set(); const out=[];
                    arr.forEach(l => { if (l && !seen.has(l)) { seen.add(l); out.push(l); }});
                    return out;
                  };
                  const tidyEffect = (s) => s
                    .replace(/\s*Raises ATK & DEF\s*Causes/gi, ' Raises ATK & DEF; Causes')
                    .replace(/\s+;/g,';')
                    .replace(/;\s+/g,'; ')
                    .replace(/\s+/g,' ')
                    .trim();

                  function splitNameEffectFromHtml(html, cutNextHeaderLabel){
                    const tmp = document.createElement('div');
                    tmp.innerHTML = html || "";
                    if (cutNextHeaderLabel) {
                      const all = Array.from(tmp.querySelectorAll('*'));
                      const nested = all.find(n => n.textContent.trim() === cutNextHeaderLabel);
                      if (nested) {
                        while (nested.nextSibling) nested.nextSibling.remove();
                        nested.remove();
                      }
                    }
                    let raw = tmp.innerText.split('\n').map(s => s.trim());
                    const html_lines_raw = raw.slice();
                    raw = raw.filter(dropNoise);
                    const after_noise = raw.slice();
                    const dedup = dedupLines(raw);
                    if (!dedup.length) return { name:null, effect:null, _debug: { html_lines_raw, after_noise, dedup } };
                    const name = dedup[0];
                    const eff = dedup.slice(1).join(' ').trim() || null;
                    return { name, effect: eff ? tidyEffect(eff) : null, _debug: { html_lines_raw, after_noise, dedup } };
                  }

                  function parseSuper(superHtml, superTextRaw){
                    const htmlParsed = splitNameEffectFromHtml(superHtml, 'Ultra Super Attack');
                    const super_html_debug = htmlParsed._debug;
                    if (htmlParsed.name || htmlParsed.effect) return {
                      ...htmlParsed,
                      _debug: { mode: 'html', super_html_debug }
                    };

                    const lines0 = (superTextRaw || '').split('\n').map(s => s.trim());
                    const stopIdx = lines0.findIndex(l => l === 'Ultra Super Attack');
                    const before = (stopIdx >= 0 ? lines0.slice(0, stopIdx) : lines0);
                    const after_noise = before.filter(dropNoise);
                    const dedup = dedupLines(after_noise);
                    const name = dedup[0] || null;
                    const eff = name ? tidyEffect(dedup.slice(1).join(' ')) : null;
                    return {
                      name, effect: eff || null,
                      _debug: { mode: 'text-fallback', stop_idx: stopIdx, lines_raw: lines0, before_stop: before, after_noise, dedup }
                    };
                  }

                  function parseActive(activeHtml, activationText){
                    const tmp = document.createElement('div');
                    tmp.innerHTML = activeHtml || "";
                    Array.from(tmp.querySelectorAll('*')).forEach(el => {
                      if (el.textContent.trim() === 'Activation Condition(s)') el.remove();
                    });
                    let lines = tmp.innerText.split('\n').map(s => s.trim());
                    const raw_lines = lines.slice();
                    lines = lines.filter(dropNoise);
                    const after_noise = lines.slice();
                    lines = dedupLines(lines);
                    let name = null, effect = null;
                    if (lines.length){
                      name = lines[0];
                      effect = tidyEffect(lines.slice(1).join(' '));
                      if (effect && name && effect.startsWith(name)) effect = effect.slice(name.length).trim();
                    }
                    let act_lines = (activationText || '').split('\n').map(s=>s.trim());
                    const act_raw = act_lines.slice();
                    act_lines = act_lines.filter(dropNoise);
                    const act_dedup = dedupLines(act_lines);
                    const activation = act_dedup.join(' ').trim() || null;
                    return { name, effect, activation, _debug: {
                      active_raw_lines: raw_lines,
                      active_after_noise: after_noise,
                      activation_raw_lines: act_raw,
                      activation_dedup: act_dedup
                    }};
                  }

                  function pickAnchors(sectionHtml, mode){
                    const tmp = document.createElement('div');
                    tmp.innerHTML = sectionHtml || '';
                    const out=[]; const seen=new Set();
                    tmp.querySelectorAll('a, button, span, div').forEach(n => {
                      const a = n.closest('a');
                      const href = a?.getAttribute('href') || '';
                      const t = (n.textContent||'').trim();
                      if (!t || t.length > 40 || !/[A-Za-z]/.test(t)) return;
                      if (mode === 'links' && !/\/links?\//.test(href)) return;
                      if (mode === 'cats'  && !/\/categor(y|ies)\//.test(href)) return;
                      if (/(ATK|DEF|Ki|\+|%|Total:|Links:|Categories:|\.png|\d)/.test(t)) return;
                      if (['background','icon','rarity','element','EZA','undefined','Venatus'].includes(t)) return;
                      if (!seen.has(t+href)) { seen.add(t+href); out.push({ text:t, href }) }
                    });
                    return out;
                  }

                  function parseStatsFromSection(html, text){
                    const tmp = document.createElement('div');
                    tmp.innerHTML = html || '';
                    const table_rows = [];
                    const stats_out = {};
                    tmp.querySelectorAll('tr').forEach(tr => {
                      const row = Array.from(tr.querySelectorAll('th,td')).map(c => c.textContent.trim());
                      if (row.length) table_rows.push(row);
                    });
                    const lines = (text||'').split('\n').map(s=>s.trim()).filter(Boolean);
                    const grab = (label) => {
                      const line = lines.find(l => new RegExp('^'+label+'\\b','i').test(l));
                      if (!line) return null;
                      const nums = line.match(/\d+/g) || [];
                      if (!nums.length) return null;
                      return {
                        base_min: nums[0] || null,
                        base_max: nums[1] || null,
                        '55%':    nums[2] || null,
                        '100%':   nums[3] || null,
                      };
                    };
                    const hp  = grab('HP');  if (hp)  stats_out.HP  = hp;
                    const atk = grab('ATK'); if (atk) stats_out.ATK = atk;
                    const def = grab('DEF'); if (def) stats_out.DEF = def;
                    const mCost = /\bCost\s*[:\n]\s*(\d+)/i.exec(text || '');    if (mCost) stats_out.Cost = mCost[1];
                    const mMax  = /\bMax\s*Lv\s*[:\n]\s*(\d+)/i.exec(text || ''); if (mMax)  stats_out['Max Lv'] = mMax[1];
                    const mSA   = /\bSA\s*Lv\s*[:\n]\s*(\d+)/i.exec(text || '');  if (mSA)   stats_out['SA Lv'] = mSA[1];
                    return { table_rows, text_lines: lines, parsed: stats_out };
                  }

                  const superHtml = H('Super Attack');
                  const superTextRaw = G('Super Attack');
                  const superParsed = parseSuper(superHtml, superTextRaw);

                  const out = {
                    url: location.href,
                    page_title,
                    display_name,
                    headers_found: headers_debug,
                    raw_sections,
                    leader: (function(){
                      const lp = splitNameEffectFromHtml(H('Leader Skill'), null);
                      return { name: lp.name, effect: lp.effect, _debug: lp._debug };
                    })(),
                    super_debug: {
                      present: Boolean(superHtml),
                      html_len: (superHtml || '').length,
                      text_len: (superTextRaw || '').length,
                      parsed: superParsed
                    },
                    ultra_debug: (function(){
                      const up = splitNameEffectFromHtml(H('Ultra Super Attack'), null);
                      return { name: up.name, effect: up.effect, _debug: up._debug };
                    })(),
                    passive_debug: (function(){
                      const pp = splitNameEffectFromHtml(H('Passive Skill'), null);
                      return { name: pp.name, effect: pp.effect, _debug: pp._debug };
                    })(),
                    active_debug: (function(){
                      const ap = parseActive(H('Active Skill'), G('Activation Condition(s)'));
                      return { name: ap.name, effect: ap.effect, activation: ap.activation, _debug: ap._debug };
                    })(),
                    link_skills_anchors: pickAnchors(H('Link Skills'), 'links'),
                    categories_anchors: pickAnchors(H('Categories'), 'cats'),
                    stats_debug: parseStatsFromSection(H('Stats'), G('Stats') || ''),
                    release_debug: (function(){
                      const bodyText = document.body.innerText;
                      const md = /Release Date\s+([0-9/.-]+)\s+([0-9: ]+[APMapm]{2})\s+([A-Z]{2,4})/.exec(bodyText);
                      return md ? { matched: true, date: md[1], time: md[2], tz: md[3] } : { matched: false };
                    })(),
                    image_urls: Array.from(new Set(Array.from(document.images).filter(isVisible).map(img => new URL(img.getAttribute('src'), location.origin).href))),
                    page_text_preview: (document.body.innerText || '').slice(0, 1200)
                  };
                  return out;
                }""",
                {"headers": HEADERS, "cssPathFn": css_path_js()},
            )

            safe_dump_json(run_dir / "debug.json", {
                "env": env.__dict__,
                "source_url": debug_obj.get("url"),
                "page_title": debug_obj.get("page_title"),
                "display_name": debug_obj.get("display_name"),
                "headers_found": debug_obj.get("headers_found"),
                "raw_sections": debug_obj.get("raw_sections"),
                "leader": debug_obj.get("leader"),
                "super_debug": debug_obj.get("super_debug"),
                "ultra_debug": debug_obj.get("ultra_debug"),
                "passive_debug": debug_obj.get("passive_debug"),
                "active_debug": debug_obj.get("active_debug"),
                "link_skills_anchors": debug_obj.get("link_skills_anchors"),
                "categories_anchors": debug_obj.get("categories_anchors"),
                "stats_debug": debug_obj.get("stats_debug"),
                "release_debug": debug_obj.get("release_debug"),
                "image_urls": debug_obj.get("image_urls"),
                "page_text_preview": debug_obj.get("page_text_preview"),
            })

        except PWTimeoutError as e:
            logging.exception("Playwright timeout: %s", e)
        except Exception as e:
            logging.exception("Unexpected error: %s", e)
        finally:
            # persist console/network
            safe_dump_json(run_dir / "console.json", console_events)
            safe_dump_json(run_dir / "network.json", {
                "responses": responses[:1000],
                "failures": failures[:1000],
            })

            # tracing finalize (old/new API compatible)
            if tracing_started:
                try:
                    if hasattr(context.tracing, "export"):
                        context.tracing.stop()
                        try:
                            context.tracing.export(path=str(trace_zip))
                            logging.info("Saved trace (export): %s", trace_zip)
                        except Exception as ee:
                            logging.warning("Trace export failed: %s", ee)
                    else:
                        context.tracing.stop(path=str(trace_zip))
                        logging.info("Saved trace (stop with path): %s", trace_zip)
                except Exception as te:
                    logging.warning("Tracing save failed: %s", te)

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    # zip everything for upload
    out_zip = OUTROOT / f"dokkan_debug-{stamp}.zip"
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in run_dir.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(run_dir))
    print(f"DEBUG_BUNDLE: {out_zip}")

if __name__ == "__main__":
    main()
