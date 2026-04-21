# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Web app that plots French consular voters from the Plateau-Mont-Royal arrondissement (Montreal, QC) on a Google Map, with filtering by postal code area.

## Dataset

**File:** `plateau_mont_royal_2026.csv` (~8,338 records)

**Columns:** Adresse, Adresse complète, Nom de naissance, Nom d'usage, Prénoms, Date de naissance, AGE, Lieu de naissance, Pays de naissance, Code postal et ville, CODE_POSTAL, ARRONDISSEMENT, 3 zip (first 3 characters of postal code), Adresse électronique, Latitude, Longitude

**Key details:**
- Encoding includes French accented characters (UTF-8)
- Date format is DD/MM/YYYY
- AGE is a float (e.g., 77.0)
- Latitude/Longitude columns exist but are unpopulated
- "Pays de naissance" is blank for France-born residents; only filled for other countries
- "3 zip" is derived from CODE_POSTAL (first 3 chars, e.g., H2W)
- All records have ARRONDISSEMENT = "Plateau-Mont-Royal"

## Architecture

Two-phase approach: Python geocoding script (runs once) + single-file HTML/JS frontend.

- `geocode.py` — Batch geocodes addresses from the CSV, outputs `geocoded.json`. Supports Google Geocoding API and Nominatim (OSM). Uses only Python stdlib (no pip install).
- `index.html` — Single-file web app using Google Maps JS API + MarkerClusterer. Color-coded markers by zip3, filter sidebar, name search.
- `geocoded.json` — Generated data file loaded by the frontend (gitignored).
- `geocode_cache.json` — Intermediate geocoding cache for resume support (gitignored).

## Usage

### 1. Geocode addresses (run once)

```bash
# Using Nominatim (free, ~2 hours)
python3 geocode.py --provider nominatim

# Using Google (faster, ~5 min, requires API key)
python3 geocode.py --provider google --api-key YOUR_KEY

# Dry run (preview without API calls)
python3 geocode.py --provider nominatim --dry-run
```

The script supports resume — if interrupted, re-run and it continues from where it stopped.

### 2. Run the web app

```bash
python3 -m http.server 8000
# Open http://localhost:8000
```

Enter your Google Maps JavaScript API key in the sidebar on first load (saved to localStorage).

### Google Cloud API Key Setup

1. Go to Google Cloud Console, create/select a project
2. Enable **Maps JavaScript API** (required for map display)
3. Optionally enable **Geocoding API** (only if using `--provider google`)
4. Create an API key, restrict to localhost referrer

## Privacy

This dataset contains personally identifiable information (names, addresses, dates of birth, email addresses). Handle with care and do not publish or share externally. The `geocoded.json` output excludes email and DOB to minimize PII exposure.
