# Dokkan Scraper & AI

Windows-friendly toolkit for:

- Scraping (educational/archival) from **dokkaninfo.com**
- Structuring a **local Dokkan database**
- A fast **web UI** to browse units, build teams, and find leaders
- Optional hooks for an **AI Q&A** layer

> Ships a local viewer (`cards_site.py`). Bring your own scraped data (see “Data layout”).

---

## Features

**Home / Unit Browser**
- Search by name, ID, type, rarity, categories, links
- Sort: newest, name, rarity, type
- Facets for top categories and link skills
- Mechanics flags (EZA, S-EZA, Transform, Exchange, Standby, Active, Revival)
- Favorites (localStorage)
- Dark/Light theme, art mode switcher

**Unit Detail**
- Forms rail (Base / Transform / Exchange / Giant / Standby)
- EZA / S-EZA steps
- Leader, Super/Ultra, Passive (line-aware), Active/Standby blocks
- Stats table (Base / 55% / 100%)
- Links & Categories chips
- Deep-link params: `?form=…&mode=…&step=…`

**Team Builder**
- Team board + picker side-by-side (responsive)
- Suggest Best Leader (coverage + boost + synergy)
- Min Boost filter (Any / 200%+ / 220%+)
- Auto-fill best, lock slots, swap mode, drag & drop
- Duplicate prevention toggle
- Summary (pairwise shared links, averages)
- Presets (save/load/delete via localStorage)
- Share links & JSON export

**Leader Finder**
- Pick up to two targets, see leaders that cover both
- Min Boost filter (Any / 200%+ / 220%+)
- Shows each leader’s % for each target
- Links to Details and “Use as Leader” in Team Builder
- Shareable deep links

---

## Project layout

```
.
├─ cards_site.py            # Flask app (entry point)
├─ output/
│  ├─ cards/
│  │  └─ 1234567/
│  │     └─ METADATA.json   # one per unit
│  └─ assets/
│     └─ dokkaninfo.com/... # mirrored art paths used by the UI
├─ requirements.txt
└─ README.md
```

### Data layout (minimal fields used by the UI)

- **Unit metadata**: `output/cards/<unit_id>/METADATA.json`
- **Assets root**: `output/assets/dokkaninfo.com/...`

```json
{
  "unit_id": "1234567",
  "display_name": "[Title] Name",
  "variants": [
    {
      "key": "base",                      // or form_... / eza_step_X
      "rarity": "LR|UR|SSR",
      "type": "AGL|TEQ|INT|STR|PHY",
      "obtain_type": "Summon|Event|...",
      "release_date": "2024-05-01 00:00:00",
      "timezone": "UTC",

      "assets": [
        "dokkaninfo.com/assets/global/en/character/card/1234567/1234567.png"
      ],
      "assets_index": {
        "card_art": [
          {"category":"card_art","subtype":"full_card","path":"dokkaninfo.com/.../1234567/1234567.png"},
          {"category":"card_art","subtype":"character","path":"dokkaninfo.com/.../character/card/1234567/card_1234567_character.png"}
        ]
      },

      "kit": {
        "display_name": "[Form] Name",
        "leader_skill": "\"Successors\", \"Fused Fighters\" or \"Pure Saiyans\" Category Ki +3, HP +200% and ATK & DEF +170%, plus an additional HP, ATK & DEF +50% for characters who also belong to the \"Gifted Warriors\" or \"Fusion\" Category",
        "super_attack": {"name":"...","effect":"..."},
        "ultra_super_attack": {"name":"...","effect":"..."},
        "passive_skill": {
          "effect": "...",
          "lines": [{"text":"ATK & DEF +200%","context":"start of turn"}]
        },
        "active_skill": {"name":"...","effect":"...","activation_conditions":"..."},
        "standby_skill": {"name":"...","effect":"..."},
        "link_skills": ["Link A","Link B"],
        "categories": ["Cat A","Cat B"],
        "stats": {
          "HP":{"Base Max":10000,"55%":15000,"100%":20000},
          "ATK":{"Base Max":8000,"55%":12000,"100%":16000},
          "DEF":{"Base Max":4000,"55%":8000,"100%":12000}
        },
        "transformation":{"can_transform":true},
        "reversible_exchange":{"can_exchange":false},
        "giant_form":{"can_transform":false},
        "revival":{"can_revive":false}
      },

      "eza": false,
      "is_super_eza": false,
      "step": 0
    }
  ],
  "source_base_url": "https://dokkaninfo.com/.../1234567"
}
```

---

## Windows quickstart

```powershell
# 1) Python 3.9+ installed (Add to PATH checked)

# 2) Create venv
cd path\to\project
py -3.9 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3) Install deps
pip install -r requirements.txt
# requirements.txt:
# Flask>=2.3,<3.0

# 4) Put data under:
# output/cards/<unit_id>/METADATA.json
# output/assets/dokkaninfo.com/...

# 5) Run
python .\cards_site.py

# 6) Open
# http://127.0.0.1:5000

# 7) Reload data cache after changing files
# append ?reload=1
# http://127.0.0.1:5000/?reload=1
```

---

## Deploying

This is a **Flask** server. Use a Python host:

- Render / Railway / Fly.io / Heroku (container) / VPS
- Start command: `python cards_site.py`
- Include the `output/` data folder in your deploy image or mount it

> Netlify is static. Flask won’t run there unless you add a separate API layer or pre-render pages.

---


## Tips

- If images 404, verify paths under:
  ```
  output/assets/dokkaninfo.com/...
  ```
- `release_date` accepts common formats (`%Y-%m-%d %H:%M:%S`, `%m/%d/%Y`, …)
- Favorites & presets live in browser `localStorage`
- `Ctrl/Cmd + K` focuses the global search

---

## Legal

- Unofficial. Not affiliated with Bandai Namco or dokkaninfo.
- Scraping is educational/archival. Respect robots.txt, rate limits, and laws.
- All art and trademarks belong to their owners.

---

## Contributing

PRs welcome:
- Data adapters (scraper → `METADATA.json`)
- UI/UX polish, mobile, perf
- Filters, mechanics handling

Avoid committing scraped assets you don’t have rights to distribute.

---

## Roadmap

- Multi-unit leader finder (3+ targets)
- Static export (“freeze”) mode
- Full-text search index
- Portable offline bundle

---

## Health check

- All units: `GET /api/units`
- Single unit: `GET /api/unit/<unit_id>`
- Asset probe: `GET /__exists?rel=<path>` → `{ "exists": true|false }`
