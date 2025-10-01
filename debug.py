# build_debug_package.py
# Creates a single zip with everything I need to diagnose parsing.
# Stdlib only (works on Python 3.9). No Playwright required.

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

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

ROOT = Path(".")
CARDS_DIR = ROOT / "output" / "cards"
LOGS_DIR = ROOT / "output" / "logs"
DEBUG_DIR = ROOT / "output" / "debug_bundles"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def pick_latest_card_dir():
    if not CARDS_DIR.exists():
        return None
    # newest by mtime
    dirs = [p for p in CARDS_DIR.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)

def slice_section(page_text, start_label, next_labels):
    if not page_text:
        return None
    # find line-boundary matches for labels
    def find_pos(label):
        m = re.search(rf"(?mi)^\s*{re.escape(label)}\s*$", page_text)
        return m.start() if m else None

    start = find_pos(start_label)
    if start is None:
        return None

    # start of content is end of start label line
    start_line_end = re.search(r"(?m)^\s*"+re.escape(start_label)+r"\s*$", page_text)
    content_start = start_line_end.end() if start_line_end else (start + len(start_label))

    # find the earliest next header after content_start
    end_positions = []
    for lab in next_labels:
        m = re.search(rf"(?mi)^\s*{re.escape(lab)}\s*$", page_text[content_start:])
        if m:
            end_positions.append(content_start + m.start())
    content_end = min(end_positions) if end_positions else len(page_text)

    block = page_text[content_start:content_end].strip("\n")
    # normalize whitespace, but keep newlines
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    text = "\n".join(lines).strip()
    return {
        "label": start_label,
        "raw": block,
        "lines": lines,
        "first_line": lines[0] if lines else None
    }

def collect_files(card_dir):
    files = {}
    # required-ish
    files["page_text"] = card_dir / "PAGE_TEXT.txt"
    files["page_html"] = card_dir / "page.html"
    files["metadata"] = card_dir / "METADATA.json"

    # best-effort attachments
    # latest run log
    if LOGS_DIR.exists():
        logs = sorted(LOGS_DIR.glob("run-*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if logs:
            files["run_log"] = logs[0]
        # screenshot
        screen_dir = LOGS_DIR / "screens"
        if screen_dir.exists():
            shots = sorted(screen_dir.glob("card-*.png"), key=lambda p: p.stat().st_mtime, reverse=True)
            if shots:
                files["screenshot"] = shots[0]
        # trace zip (might be trace-*.zip or inside logs root)
        traces = sorted(LOGS_DIR.glob("trace-*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if traces:
            files["trace"] = traces[0]

    return {k: v for k, v in files.items() if v and v.exists()}

def make_diagnostics(card_dir, files):
    diag = {
        "card_folder": card_dir.name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headers": HEADERS,
        "slices": {},
        "metadata": None,
        "notes": [
            "Each slice below is cut from PAGE_TEXT.txt between the named header and the next header.",
            "Use this to check where text bleeds between sections or where a header wasn’t found."
        ]
    }

    page_text = files.get("page_text").read_text(encoding="utf-8") if files.get("page_text") else ""
    meta_path = files.get("metadata")
    if meta_path:
        try:
            diag["metadata"] = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as e:
            diag["metadata_error"] = f"Failed to load METADATA.json: {e}"

    # Build next-labels map
    nexts = {}
    for i, h in enumerate(HEADERS):
        nexts[h] = HEADERS[i+1:]

    for h in HEADERS:
        diag["slices"][h] = slice_section(page_text, h, nexts[h])

    # quick “what we expect” checks (non-fatal)
    exp = {
        "leader_skill_should_not_repeat": True,
        "super_attack_should_exist": True,
        "ultra_super_attack_should_exist": True,
        "passive_name_should_be": "Terrifying Surge of Ki",
        "active_name_should_be": "Pure Heart and Wrathful Power"
    }
    diag["expectations"] = exp

    # convenient comparisons from slices
    def first_line(label):
        s = diag["slices"].get(label) or {}
        return s.get("first_line")

    diag["quick_check"] = {
        "leader_first_line": first_line("Leader Skill"),
        "super_first_line": first_line("Super Attack"),
        "ultra_first_line": first_line("Ultra Super Attack"),
        "passive_first_line": first_line("Passive Skill"),
        "active_first_line": first_line("Active Skill"),
        "activation_raw": (diag["slices"].get("Activation Condition(s)") or {}).get("raw"),
    }

    return diag

def add_file(zf, path, arcname):
    if path and path.exists():
        zf.write(path, arcname)

def main():
    ap = argparse.ArgumentParser(description="Build a single debug bundle for DokkanInfo parsing.")
    ap.add_argument("--card", help="Exact card folder name under output/cards (default: newest)", default=None)
    args = ap.parse_args()

    if args.card:
        card_dir = CARDS_DIR / args.card
        if not card_dir.exists():
            print(f"[ERROR] Card dir not found: {card_dir}")
            return
    else:
        card_dir = pick_latest_card_dir()
        if not card_dir:
            print("[ERROR] No card directories found under output/cards")
            return

    files = collect_files(card_dir)
    if not files:
        print(f"[ERROR] No files found to bundle under {card_dir}")
        return

    diag = make_diagnostics(card_dir, files)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_zip = DEBUG_DIR / f"dokkan_debug_bundle-{stamp}.zip"

    with ZipFile(out_zip, "w", compression=ZIP_DEFLATED) as z:
        # diagnostics JSON
        z.writestr("diagnostics.json", json.dumps(diag, ensure_ascii=False, indent=2))

        # include raw files
        add_file(z, files.get("metadata"), f"{card_dir.name}/METADATA.json")
        add_file(z, files.get("page_text"), f"{card_dir.name}/PAGE_TEXT.txt")
        add_file(z, files.get("page_html"), f"{card_dir.name}/page.html")
        add_file(z, files.get("run_log"), "logs/latest-run.log")
        add_file(z, files.get("screenshot"), "logs/screenshot.png")
        add_file(z, files.get("trace"), "logs/trace.zip")

    print(f"DEBUG_BUNDLE: {out_zip}")

if __name__ == "__main__":
    main()
