#!/usr/bin/env python3
"""Batch geocode addresses from the Plateau-Mont-Royal voter registry CSV.

Supports Google Geocoding API and Nominatim (OSM) as providers.
Results are cached incrementally so interrupted runs can resume.

Usage:
    python3 geocode.py --provider google --api-key YOUR_KEY
    python3 geocode.py --provider nominatim
    python3 geocode.py --fix-missing --provider nominatim
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

CSV_FILE = "plateau_mont_royal_2026.csv"
CACHE_FILE = "geocode_cache.json"
FIXES_CACHE_FILE = "geocode_fixes.json"
OUTPUT_FILE = "geocoded.json"

# Plateau-Mont-Royal bounding box (loose) for sanity-checking results
BOUNDS = {"lat_min": 45.50, "lat_max": 45.55, "lng_min": -73.62, "lng_max": -73.55}


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=1)


def normalize_zip3(code_postal):
    """Extract the 3-character postal code prefix from CODE_POSTAL."""
    cp = code_postal.strip().upper()
    if len(cp) >= 3 and cp[0] == "H" and cp[1] == "2":
        return cp[:3]
    return None


def strip_apartment(addr):
    """Remove apartment/unit numbers from an address for better geocoding.

    Handles patterns like:
      "312 - 5529 AV PAPINEAU"  -> "5529 AV PAPINEAU"
      "1202-30 BOULEVARD ..."   -> "30 BOULEVARD ..."
      "14 - 5209 RUE DROLET"   -> "5209 RUE DROLET"
      "30 LAURIER OUEST APT 9" -> "30 LAURIER OUEST"
      "5255 RIVARD APT 35"     -> "5255 RIVARD"
    """
    # Remove "APT xxx" or "APP xxx" or "APPT xxx" or "APP.xxx" suffix (case-insensitive)
    addr = re.sub(r'\s+APPT?\s*\.?\s*\S+', '', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\s+APT\s*\.?\s*\S+', '', addr, flags=re.IGNORECASE)
    # Remove "appartement xxx" or "app. xxx" suffix
    addr = re.sub(r',?\s+appartement\s+\S+', '', addr, flags=re.IGNORECASE)
    addr = re.sub(r',?\s+app\.\s*\S+', '', addr, flags=re.IGNORECASE)
    # Remove "SUITE xxx" suffix
    addr = re.sub(r',?\s+SUITE\s+\S+', '', addr, flags=re.IGNORECASE)
    # Remove "UNITE xxx" or "UNITE xxx-" prefix like "UNITE 3-3825, AVENUE..."
    m_unite = re.match(r'^UNITE\s+\S+\s*-?\s*(\d+.*)$', addr, re.IGNORECASE)
    if m_unite:
        addr = m_unite.group(1)

    # Remove leading apartment prefix: "312 - 5529 AV ..." or "1202-30 BOULEVARD ..."
    # Also handles letter prefixes: "A-4407 RUE", "B-4053 RUE", "C2-4230 RUE", "D5-5005 RUE"
    # Also: "APP 203-2525 RUE", "APT 107 5051 RUE", "APPT 309- 4350 AV"
    m = re.match(r'^(?:APP(?:T)?\s*\.?\s*\S+\s*-?\s*)(\d+.*)$', addr, re.IGNORECASE)
    if not m:
        m = re.match(r'^(?:APT\s+\S+\s+)(\d+.*)$', addr, re.IGNORECASE)
    if not m:
        m = re.match(r'^[A-Za-z]\d*\s*-\s*(\d+.*)$', addr)
    if not m:
        m = re.match(r'^\d+[A-Za-z]?\s*-\s*(\d+.*)$', addr)
    if m:
        addr = m.group(1)

    # Remove leading letter+digits glued to address: "A3852 RUE DROLET" -> "3852 RUE DROLET"
    m = re.match(r'^[A-Za-z]\d*[A-Za-z]?\d*\s*-?\s*(\d+\s+(?:RUE|AV|AVENUE|BOUL|BOULEVARD|BLD|BD|CHEMIN|ROUTE).*)$', addr, re.IGNORECASE)
    if m:
        addr = m.group(1)

    return addr.strip()


def clean_address(addr):
    """Deep-clean an address for geocoding: strip apartments, fix abbreviations, typos."""
    addr = strip_apartment(addr)

    # Remove trailing ", Montréal, QC H2J" style suffixes already in the address
    addr = re.sub(r',\s*Montr[ée]al.*$', '', addr, flags=re.IGNORECASE)

    # Remove "CANADA" suffix
    addr = re.sub(r'\s+CANADA$', '', addr, flags=re.IGNORECASE)

    # Expand directional abbreviations at end of street name
    addr = re.sub(r'\bE$', 'Est', addr)
    addr = re.sub(r'\bO$', 'Ouest', addr)
    addr = re.sub(r'\bO,', 'Ouest,', addr)

    # Expand street type abbreviations
    addr = re.sub(r'\bBOUL\.?\b', 'Boulevard', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bBLD\.?\b', 'Boulevard', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bBD\.?\b', 'Boulevard', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bBld\b', 'Boulevard', addr)
    addr = re.sub(r'\bAV\b', 'Avenue', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bRLE\b', 'Ruelle', addr, flags=re.IGNORECASE)

    # Fix "EST" at wrong position: "948 EST BOUL. ST JOSEPH" -> "948 Boulevard Saint-Joseph Est"
    m = re.match(r'^(\d+)\s+EST\s+(.+)', addr, re.IGNORECASE)
    if m:
        addr = f"{m.group(1)} {m.group(2)} Est"

    # Expand ST-/ST /SAINT- to Saint-
    addr = re.sub(r'\bST-', 'Saint-', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bST\s+', 'Saint-', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bSTE-', 'Sainte-', addr, flags=re.IGNORECASE)

    # Fix common typos
    addr = re.sub(r'JENRI-JULIEN', 'Henri-Julien', addr, flags=re.IGNORECASE)
    addr = re.sub(r'HERVE JULIEN', 'Henri-Julien', addr, flags=re.IGNORECASE)
    addr = re.sub(r'HENRI JULIEN', 'Henri-Julien', addr, flags=re.IGNORECASE)
    addr = re.sub(r'WAUERLY|WAWERLY|WARELY|WAVERLY STREET', 'Waverly', addr, flags=re.IGNORECASE)
    addr = re.sub(r'GILOFORD|GILOFRD', 'Gilford', addr, flags=re.IGNORECASE)
    addr = re.sub(r'SAINT-JOESPH|SAINT-JOSPEH|SAINT-JOSEPPH|ST-JOSEPPH|ST-JOSPEH', 'Saint-Joseph', addr, flags=re.IGNORECASE)
    addr = re.sub(r'SAINT-GÉGOIRE|SAINT-GEGOIRE|saint-gregoire', 'Saint-Grégoire', addr)
    addr = re.sub(r'LAUNAUDIERE|LANAIDIÈRE|LA LANAUDIERE|LANAUDIERE(?!`)', 'Lanaudière', addr, flags=re.IGNORECASE)
    addr = re.sub(r'LAURIMIER', 'Laurier', addr, flags=re.IGNORECASE)
    addr = re.sub(r'DELORIMIER', 'De Lorimier', addr, flags=re.IGNORECASE)
    addr = re.sub(r'PAPINEAU', 'Papineau', addr, flags=re.IGNORECASE)

    # Fix more typos from the data
    addr = re.sub(r'BUILLION|BUILLON|BOULLIONS|BULLION', 'Bullion', addr, flags=re.IGNORECASE)
    addr = re.sub(r'DE BORDAUX|DE BORBEAUX|DE BORDEAUX', 'de Bordeaux', addr, flags=re.IGNORECASE)
    addr = re.sub(r'CHRITOPHE-COLOMB|CHRISTOPHE COLLOMB', 'Christophe-Colomb', addr, flags=re.IGNORECASE)
    addr = re.sub(r'PARTHENEAIS|PARTHENAIS', 'Parthenais', addr, flags=re.IGNORECASE)
    addr = re.sub(r'CALRK', 'Clark', addr, flags=re.IGNORECASE)
    addr = re.sub(r'FBRE|FAVRE', 'Fabre', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bROLET\b', 'Drolet', addr, flags=re.IGNORECASE)
    addr = re.sub(r'DROLLET', 'Drolet', addr, flags=re.IGNORECASE)
    addr = re.sub(r'MONTANA\b', 'Mentana', addr, flags=re.IGNORECASE)
    addr = re.sub(r'MERITIAN', 'Mentana', addr, flags=re.IGNORECASE)
    addr = re.sub(r'NAUDIERE', 'Lanaudière', addr, flags=re.IGNORECASE)
    addr = re.sub(r'LANAUDIARE', 'Lanaudière', addr, flags=re.IGNORECASE)
    addr = re.sub(r'SAINT-DOMNIQUE', 'Saint-Dominique', addr, flags=re.IGNORECASE)
    addr = re.sub(r'SAINT-HURBAIN', 'Saint-Urbain', addr, flags=re.IGNORECASE)
    addr = re.sub(r'GRAN-PRÉ', 'Grand-Pré', addr, flags=re.IGNORECASE)
    addr = re.sub(r'GRANDPRÉ', 'Grand-Pré', addr, flags=re.IGNORECASE)
    addr = re.sub(r'EVNUE|AENUE', 'Avenue', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\bBERRY\b', 'Berri', addr, flags=re.IGNORECASE)
    addr = re.sub(r'CHARRIER', 'Carrier', addr, flags=re.IGNORECASE)
    addr = re.sub(r'BARETTE', 'Barrette', addr, flags=re.IGNORECASE)
    addr = re.sub(r'TERASSE', 'Terrasse', addr, flags=re.IGNORECASE)
    addr = re.sub(r'GUINDON', 'Guindon', addr, flags=re.IGNORECASE)
    addr = re.sub(r'BIBEAU', 'Bibeau', addr, flags=re.IGNORECASE)
    addr = re.sub(r'GARNEAU', 'Garnier', addr, flags=re.IGNORECASE)
    addr = re.sub(r'RIVARD SAINT', 'Rivard', addr, flags=re.IGNORECASE)
    addr = re.sub(r'COLONIALE AVENUE', 'Avenue Coloniale', addr, flags=re.IGNORECASE)

    # Fix "saint Jospeh" (typo without dash)
    addr = re.sub(r'saint Jospeh', 'Saint-Joseph', addr, flags=re.IGNORECASE)

    # Fix missing space: "4266RUE" -> "4266 RUE", "4671RUE" -> "4671 RUE", "913AV" -> "913 AV"
    addr = re.sub(r'^(\d+)(RUE|AV|AVENUE|BOUL|BOULEVARD|rue|ru)\b', r'\1 \2', addr)

    # Fix "ru " -> "rue ", "vrue" -> "rue"
    addr = re.sub(r'\bru\b', 'rue', addr)
    addr = re.sub(r'\bvrue\b', 'rue', addr, flags=re.IGNORECASE)

    # Add "Rue" prefix for bare street names: "4878 CLARK" -> "4878 Rue Clark"
    # Only if address is just "number + name" with no street type
    if re.match(r'^\d+\s+[A-Z][a-zA-ZÀ-ÿ\'-]+$', addr) or re.match(r'^\d+\s+[A-Z][A-ZÀ-ÿ\'-]+$', addr):
        parts = addr.split(None, 1)
        if len(parts) == 2:
            addr = f"{parts[0]} Rue {parts[1]}"

    # "4383 HENRI-JULIEN" -> "4383 Avenue Henri-Julien" (known avenues)
    addr = re.sub(r'^(\d+)\s+(HENRI-JULIEN|Henri-Julien)$', r'\1 Avenue Henri-Julien', addr)

    # Bare "DE LA ROCHE", "DE MENTANA" etc -> add Rue
    addr = re.sub(r'^(\d+)\s+(DE\s+)', r'\1 Rue \2', addr, flags=re.IGNORECASE)
    # "ESPLANADE" -> "Avenue de l'Esplanade"
    addr = re.sub(r'^(\d+)\s+ESPLANADE$', r"\1 Avenue de l'Esplanade", addr, flags=re.IGNORECASE)
    addr = re.sub(r"^(\d+)\s+DE L'ESPLANADE$", r"\1 Avenue de l'Esplanade", addr, flags=re.IGNORECASE)
    # Bare "RACHEL EST" -> "Rue Rachel Est"
    addr = re.sub(r'^(\d+)\s+(RACHEL|MARIE-ANNE|LAURIER)\s+(EST|OUEST)$', r'\1 Rue \2 \3', addr, flags=re.IGNORECASE)

    # Fix "Hôtel-deVille" -> "Hôtel-de-Ville"
    addr = re.sub(r'Hôtel-deVille', "Hôtel-de-Ville", addr)
    addr = re.sub(r'HOTEL DE VILLE', "Hôtel-de-Ville", addr, flags=re.IGNORECASE)

    # Remove "MONTREAL (QC)" or "(QUEBEC)CANADA" suffixes
    addr = re.sub(r'\s*MONTR[EÉ]AL\s*\(?(?:QC|QUEBEC|QUÉBEC)?\)?\s*(?:CANADA)?', '', addr, flags=re.IGNORECASE)

    # Remove "CONDO xxx" suffix
    addr = re.sub(r'\s+CONDO\s+\d+', '', addr, flags=re.IGNORECASE)

    # Fix "DE ERABLES" -> "des Érables"
    addr = re.sub(r'DE(?:S)?\s+ERABLES|DES ÉRABLES', 'des Érables', addr, flags=re.IGNORECASE)

    # Remove "AP xxx" at end (short form)
    addr = re.sub(r'\s+AP\s+\d+$', '', addr, flags=re.IGNORECASE)

    # Remove suffix letter on street number: "5287B RUE" -> "5287 RUE"
    addr = re.sub(r'^(\d+)[A-Za-z]\s+', r'\1 ', addr)

    # Remove "ROUTE257" -> "Route 257"
    addr = re.sub(r'ROUTE(\d+)', r'Route \1', addr, flags=re.IGNORECASE)

    # Fix "18EME AVENUE" / "6EME AVENUE" -> "18e Avenue"
    addr = re.sub(r'(\d+)EME\s+AVENUE', r'\1e Avenue', addr, flags=re.IGNORECASE)

    # Fix "302 -B-5435 rue Saint-Denis" -> "5435 rue Saint-Denis"
    addr = re.sub(r'^\d+\s*-[A-Za-z]-(\d+)', r'\1', addr)

    # Fix "3952 3954 RUE" -> "3952 RUE" (double street numbers)
    addr = re.sub(r'^(\d+)\s+\d+\s+(RUE|AV|AVENUE|BOUL|BOULEVARD)', r'\1 \2', addr, flags=re.IGNORECASE)

    # Fix "5287 B RUE" -> "5287 RUE" (letter between number and street type)
    addr = re.sub(r'^(\d+)\s+[A-Z]\s+(RUE|AV|AVENUE|BOUL|BOULEVARD)', r'\1 \2', addr, flags=re.IGNORECASE)

    # Fix "4230 B1 RUE" -> "4230 RUE"
    addr = re.sub(r'^(\d+)\s+[A-Z]\d+\s+(RUE|AV|AVENUE|BOUL|BOULEVARD|rue)', r'\1 \2', addr)

    # Fix postal codes with dashes: "H2J-3K8" -> "H2J 3K8" (for later postal code matching)
    # Remove trailing numbers after street name (likely apt): "5161 RUE BERRI 409" -> "5161 RUE BERRI"
    addr = re.sub(r'\s+\d+$', '', addr)

    # Remove commas and extra spaces
    addr = re.sub(r',', ' ', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()

    # Remove "- UNITE xxx" suffix
    addr = re.sub(r'\s*-\s*UNITE\s+\d+$', '', addr, flags=re.IGNORECASE)

    return addr


def geocode_google(address_str, api_key):
    """Geocode using Google Geocoding API. Returns (lat, lng) or None."""
    params = urllib.parse.urlencode({
        "address": address_str,
        "key": api_key,
        "bounds": f"{BOUNDS['lat_min']},{BOUNDS['lng_min']}|{BOUNDS['lat_max']},{BOUNDS['lng_max']}",
    })
    url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data["status"] == "OK" and data["results"]:
            loc = data["results"][0]["geometry"]["location"]
            return (loc["lat"], loc["lng"])
        return None
    except (urllib.error.URLError, json.JSONDecodeError, KeyError, TimeoutError, OSError):
        return None


def geocode_nominatim(address_str):
    """Geocode using Nominatim (OpenStreetMap). Returns (lat, lng) or None."""
    params = urllib.parse.urlencode({
        "q": address_str,
        "format": "json",
        "limit": 1,
        "viewbox": f"{BOUNDS['lng_min']},{BOUNDS['lat_max']},{BOUNDS['lng_max']},{BOUNDS['lat_min']}",
        "bounded": 1,
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "election-melanie-geocoder/1.0"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data:
            return (float(data[0]["lat"]), float(data[0]["lon"]))
        return None
    except (urllib.error.URLError, json.JSONDecodeError, KeyError, IndexError, ValueError, TimeoutError, OSError):
        return None


def read_csv():
    """Read the CSV and return all rows as dicts."""
    rows = []
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def build_unique_addresses(rows):
    """Extract unique (address, postal_code) pairs for geocoding."""
    unique = {}
    for row in rows:
        addr = row["Adresse"].strip()
        cp = row["CODE_POSTAL"].strip()
        if not addr or not cp:
            continue
        key = f"{addr}|{cp}"
        if key not in unique:
            # Build the geocoding query string
            query = f"{addr}, {cp}, Montreal, QC, Canada"
            unique[key] = query
    return unique


def geocode_address(query, provider, api_key):
    """Geocode a single address using the chosen provider."""
    if provider == "google":
        return geocode_google(query, api_key)
    else:
        return geocode_nominatim(query)


def load_fixes_cache():
    if os.path.exists(FIXES_CACHE_FILE):
        with open(FIXES_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_fixes_cache(fixes):
    with open(FIXES_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(fixes, f, ensure_ascii=False, indent=1)


def fix_missing(args):
    """Re-geocode null/failed cache entries with apartment numbers stripped.

    Writes to a SEPARATE fixes file (geocode_fixes.json) so it can run
    safely in parallel with the main geocoding process. Use --merge after
    the main process finishes to combine results.
    """
    # Read main cache (read-only snapshot)
    cache = load_cache()
    if not cache:
        print("No cache file found. Run a normal geocode first.")
        return

    # Load existing fixes (for resume support)
    fixes = load_fixes_cache()
    already_fixed = len(fixes)

    # Find all null entries not yet in fixes
    null_keys = [k for k, v in cache.items() if v is None and k not in fixes]
    print(f"Found {len(null_keys) + already_fixed} null/failed entries in main cache")
    print(f"  {already_fixed} already in fixes cache, {len(null_keys)} remaining")

    if not null_keys:
        print("Nothing to fix!")
        return

    delay = 0.025 if args.provider == "google" else 1.1
    success = 0
    still_failed = 0

    print(f"\nRetrying {len(null_keys)} addresses with apartment numbers stripped ({args.provider})...")
    print(f"  Estimated time: ~{int(len(null_keys) * delay / 60)} minutes")
    print(f"  Writing fixes to {FIXES_CACHE_FILE} (safe to run in parallel)\n")

    for i, key in enumerate(null_keys):
        addr, cp = key.split("|", 1)
        cleaned = clean_address(addr)

        if cleaned != addr:
            query = f"{cleaned}, {cp}, Montreal, QC, Canada"
            print(f"  [{i+1}/{len(null_keys)}] {addr} -> {cleaned}")
        else:
            query = f"{addr}, {cp}, Montreal, QC, Canada"
            print(f"  [{i+1}/{len(null_keys)}] {addr} (unchanged, retrying)")

        result = geocode_address(query, args.provider, args.api_key)
        if result:
            fixes[key] = {"lat": result[0], "lng": result[1], "approximate": False}
            success += 1
        else:
            # Last resort: try postal code centroid
            fallback_query = f"{cp}, Montreal, QC, Canada"
            result = geocode_address(fallback_query, args.provider, args.api_key)
            if result:
                fixes[key] = {"lat": result[0], "lng": result[1], "approximate": True}
                success += 1
                print(f"    -> fallback to postal code centroid")
            else:
                fixes[key] = None
                still_failed += 1

        if (i + 1) % 100 == 0:
            save_fixes_cache(fixes)

        if i < len(null_keys) - 1:
            time.sleep(delay)

    save_fixes_cache(fixes)
    print(f"\nFix results: {success} fixed, {still_failed} still failed")
    print(f"Fixes saved to {FIXES_CACHE_FILE}")
    print(f"\nOnce the main process finishes, run:  python3 geocode.py --merge")


def merge_caches(args):
    """Merge fixes cache into main cache and regenerate output."""
    cache = load_cache()
    fixes = load_fixes_cache()

    if not fixes:
        print("No fixes cache found. Nothing to merge.")
        return

    merged = 0
    for key, value in fixes.items():
        if value is not None and (cache.get(key) is None):
            cache[key] = value
            merged += 1

    save_cache(cache)
    print(f"Merged {merged} fixes into main cache ({len(fixes)} total fixes, {merged} applied)")

    # Clean up fixes file
    os.remove(FIXES_CACHE_FILE)
    print(f"Removed {FIXES_CACHE_FILE}")

    # Regenerate output
    generate_output(cache)


def generate_output(cache):
    """Read CSV and generate geocoded.json from cache."""
    print(f"\nGenerating {OUTPUT_FILE}...")
    rows = read_csv()
    records = []
    geocoded_count = 0
    approximate_count = 0
    missing_count = 0

    for row in rows:
        addr = row["Adresse"].strip()
        cp = row["CODE_POSTAL"].strip()
        key = f"{addr}|{cp}"
        zip3 = normalize_zip3(cp)

        geo = cache.get(key)
        if geo and geo.get("lat"):
            records.append({
                "nom": row["Nom de naissance"].strip(),
                "prenom": row["Prénoms"].strip(),
                "adresse": addr,
                "code_postal": cp,
                "zip3": zip3 or "???",
                "lat": geo["lat"],
                "lng": geo["lng"],
                "approximate": geo.get("approximate", False),
            })
            geocoded_count += 1
            if geo.get("approximate"):
                approximate_count += 1
        else:
            missing_count += 1

    output = {
        "generated": datetime.now().isoformat(),
        "total_records": len(rows),
        "geocoded": geocoded_count,
        "approximate": approximate_count,
        "missing": missing_count,
        "records": records,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=1)

    print(f"\nDone!")
    print(f"  Total records:  {len(rows)}")
    print(f"  Geocoded:       {geocoded_count} ({geocoded_count/len(rows)*100:.1f}%)")
    print(f"  Approximate:    {approximate_count}")
    print(f"  Missing:        {missing_count}")
    print(f"  Output:         {OUTPUT_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Geocode voter registry addresses")
    parser.add_argument("--provider", choices=["google", "nominatim"], default="nominatim",
                        help="Geocoding provider (default: nominatim)")
    parser.add_argument("--api-key", help="Google Geocoding API key (required for google provider)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be geocoded without making API calls")
    parser.add_argument("--fix-missing", action="store_true",
                        help="Re-geocode only null/failed entries, stripping apartment numbers (parallel-safe)")
    parser.add_argument("--merge", action="store_true",
                        help="Merge fixes cache into main cache and regenerate output")
    args = parser.parse_args()

    if args.provider == "google" and not args.api_key:
        print("Error: --api-key is required when using --provider google", file=sys.stderr)
        sys.exit(1)

    # --merge mode: combine fixes into main cache
    if args.merge:
        merge_caches(args)
        return

    # --fix-missing mode: re-geocode null entries with apartment numbers stripped
    if args.fix_missing:
        fix_missing(args)
        return

    # Read CSV
    print(f"Reading {CSV_FILE}...")
    rows = read_csv()
    print(f"  {len(rows)} records loaded")

    # Build unique addresses
    unique_addrs = build_unique_addresses(rows)
    print(f"  {len(unique_addrs)} unique addresses to geocode")

    # Load cache
    cache = load_cache()
    already_cached = sum(1 for k in unique_addrs if k in cache)
    print(f"  {already_cached} already cached, {len(unique_addrs) - already_cached} remaining")

    if args.dry_run:
        remaining = [q for k, q in unique_addrs.items() if k not in cache]
        for q in remaining[:20]:
            print(f"  Would geocode: {q}")
        if len(remaining) > 20:
            print(f"  ... and {len(remaining) - 20} more")
        return

    # Geocode
    delay = 0.025 if args.provider == "google" else 1.1  # rate limiting
    to_geocode = [(k, q) for k, q in unique_addrs.items() if k not in cache]
    total = len(to_geocode)
    success = 0
    failed_keys = []

    print(f"\nGeocoding {total} addresses with {args.provider}...")
    print(f"  Estimated time: ~{int(total * delay / 60)} minutes\n")

    for i, (key, query) in enumerate(to_geocode):
        result = geocode_address(query, args.provider, args.api_key)
        if result:
            cache[key] = {"lat": result[0], "lng": result[1], "approximate": False}
            success += 1
        else:
            failed_keys.append(key)
            cache[key] = None  # mark as attempted

        # Progress
        if (i + 1) % 50 == 0 or i == total - 1:
            pct = (i + 1) / total * 100
            print(f"  [{i + 1}/{total}] {pct:.1f}% — {success} OK, {len(failed_keys)} failed")

        # Save cache periodically
        if (i + 1) % 100 == 0:
            save_cache(cache)

        if i < total - 1:
            time.sleep(delay)

    save_cache(cache)

    # Retry failed addresses with postal code fallback
    if failed_keys:
        print(f"\nRetrying {len(failed_keys)} failed addresses with postal code centroid...")
        for key in failed_keys:
            cp = key.split("|")[1]
            fallback_query = f"{cp}, Montreal, QC, Canada"
            result = geocode_address(fallback_query, args.provider, args.api_key)
            if result:
                cache[key] = {"lat": result[0], "lng": result[1], "approximate": True}
            time.sleep(delay)
        save_cache(cache)

    # Generate output
    generate_output(cache)


if __name__ == "__main__":
    main()
