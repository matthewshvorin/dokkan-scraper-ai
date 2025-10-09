#!/usr/bin/env python3
import json, shutil
from pathlib import Path
from contextlib import contextmanager

# import your app & helpers
import cards_site as site
from flask import render_template_string

ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
API  = DIST / "api"

@contextmanager
def ctx():
    with site.app.app_context():
        yield

def ensure_clean_dir(p: Path):
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

def main():
    ensure_clean_dir(DIST)
    (DIST / "unit").mkdir(parents=True, exist_ok=True)
    (DIST / "team").mkdir(parents=True, exist_ok=True)
    (DIST / "finder").mkdir(parents=True, exist_ok=True)
    API.mkdir(parents=True, exist_ok=True)

    # copy assets (expects your repo has output/assets/**)
    if site.ASSETS_ROOT.exists():
        shutil.copytree(site.ASSETS_ROOT, DIST / "assets", dirs_exist_ok=True)
    else:
        print(f"[WARN] Assets folder not found at {site.ASSETS_ROOT}")

    with ctx():
        # load data once
        units_meta = site.load_all_units()
        cards = [site.to_unit_summary(m) for m in units_meta]
        top_cats, top_links = site.compute_facets(cards)
        lights = [site.to_light_unit(m) for m in units_meta]

        # HOME
        html = render_template_string(site.INDEX_HTML,
                                      cards=cards,
                                      total=len(cards),
                                      top_cats=top_cats,
                                      top_links=top_links)
        (DIST / "index.html").write_text(html, encoding="utf-8")

        # UNIT DETAIL PAGES
        for m in units_meta:
            unit_id = str(m.get("unit_id") or m.get("form_id") or "")
            detail = site.to_unit_detail(m)
            html = render_template_string(site.DETAIL_HTML, u=detail)
            out_dir = DIST / "unit" / unit_id
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "index.html").write_text(html, encoding="utf-8")

        # TEAM BUILDER
        html = render_template_string(site.TEAM_HTML, units=lights)
        (DIST / "team" / "index.html").write_text(html, encoding="utf-8")

        # LEADER FINDER
        html = render_template_string(site.FINDER_HTML, units=lights)
        (DIST / "finder" / "index.html").write_text(html, encoding="utf-8")

        # JSON endpoints (static)
        (API / "units.json").write_text(json.dumps(lights), encoding="utf-8")
        for m in units_meta:
            unit_id = str(m.get("unit_id") or m.get("form_id") or "")
            (DIST / "api" / f"{unit_id}.json").write_text(
                json.dumps(site.to_unit_detail(m)), encoding="utf-8"
            )

    # Optional: long-cache headers via Netlify _headers
    headers = DIST / "_headers"
    headers.write_text(
        "/assets/*\n  Cache-Control: public, max-age=31536000, immutable\n",
        encoding="utf-8",
    )
    print(f"Built → {DIST}")

if __name__ == "__main__":
    main()
