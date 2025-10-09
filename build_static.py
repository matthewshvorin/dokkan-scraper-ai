# build_static.py
import shutil
from pathlib import Path
from datetime import datetime

# --- import your app pieces ---
from cards_site import (
    BASE_DIR, OUTPUT_DIR, ASSETS_ROOT, load_all_units,
    to_unit_summary, to_unit_detail, to_light_unit, compute_facets,
    INDEX_HTML, DETAIL_HTML, TEAM_HTML, FINDER_HTML
)
from flask import render_template_string

DIST = BASE_DIR / "dist"

def clean_dist():
    if DIST.exists():
        shutil.rmtree(DIST)
    DIST.mkdir(parents=True, exist_ok=True)

def copy_assets():
    # copies output/assets/** -> dist/assets/**
    if not ASSETS_ROOT.exists():
        print("[WARN] No assets found at", ASSETS_ROOT)
        return
    dst = DIST / "assets"
    shutil.copytree(ASSETS_ROOT, dst, dirs_exist_ok=True)
    print("[OK] assets ->", dst)

def write_file(relpath: str, html: str):
    out = DIST / relpath
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print("[OK]", relpath)

def build_all():
    print("[i] Loading units…")
    metas = load_all_units()  # uses /output/cards/**/METADATA.json
    cards = [to_unit_summary(m) for m in metas]
    light = [to_light_unit(m) for m in metas]

    # ---- Home ----
    top_cats, top_links = compute_facets(cards)
    home_html = render_template_string(
        INDEX_HTML, cards=cards, total=len(cards),
        top_cats=top_cats, top_links=top_links
    )
    write_file("index.html", home_html)

    # ---- Team Builder ----
    team_html = render_template_string(TEAM_HTML, units=light)
    write_file("team/index.html", team_html)

    # ---- Finder (new multi-select / ATK&DEF parsing version) ----
    finder_html = render_template_string(FINDER_HTML, units=light)
    write_file("finder/index.html", finder_html)

    # ---- Unit detail pages ----
    for m in metas:
        u = to_unit_detail(m)
        html = render_template_string(DETAIL_HTML, u=u)
        write_file(f"unit/{u['unit_id']}/index.html", html)

    # optional: a simple 404 for direct hits
    (DIST / "404.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>Not found</title>"
        "<style>body{font:16px system-ui;padding:40px}</style>"
        "<h1>Not found</h1><p>Try the <a href='/'>home page</a>.</p>", encoding="utf-8"
    )

def main():
    print("[i] Static build starting", datetime.now().isoformat(timespec="seconds"))
    clean_dist()
    build_all()
    copy_assets()
    print("[i] Done →", DIST)

if __name__ == "__main__":
    main()
