"""
Phase 4 - Load Manhattan restaurants into DynamoDB table "yelp-restaurants".

Data sources:
  1. NYC DOHMH Restaurant Inspection Results (main dataset: ID, name, address, zip, coordinates, cuisine)
  2. UCSD Google Local 2021, New York metadata (rating + number of reviews)

Run in AWS CloudShell:
  python3 load_restaurants.py --survey      # list how many rated restaurants EVERY Manhattan cuisine has
  python3 load_restaurants.py --dry-run     # download + match + print stats, writes NOTHING
  python3 load_restaurants.py               # same, then writes to DynamoDB
"""

import argparse
import ast
import gzip
import json
import random
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from difflib import SequenceMatcher

import boto3

# ---------------- settings ----------------
REGION = "us-east-1"
TABLE_NAME = "yelp-restaurants"

# DOHMH cuisine label -> the lowercase value LF1 / the bot uses
CUISINES = {
    "Chinese": "chinese",
    "Italian": "italian",
    "Japanese": "japanese",
    "Indian": "indian",
    "Mexican": "mexican",
    "Thai": "thai",
}
PER_CUISINE = None         # None = keep ALL rated restaurants per cuisine (no 200 cap)
REQUIRE_RATING = True      # only store restaurants that matched a Google rating (so every item has Rating + NumberOfReviews)
RANDOM_SEED = 42           # makes the "random" selection repeatable

DOHMH_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
GOOGLE_META_URL = "https://mcauleylab.ucsd.edu/public_datasets/gdrive/googlelocal/meta-New_York.json.gz"

# Rough box around Manhattan, used to keep only nearby Google businesses in memory
MANHATTAN_BOX = {"lat_min": 40.68, "lat_max": 40.89, "lng_min": -74.03, "lng_max": -73.90}

MAX_MATCH_DISTANCE_M = 150     # Google place must be within 150 m of the DOHMH location
MIN_NAME_SIMILARITY = 0.75     # 0..1 similarity between normalized names
GRID = 0.002                   # grid cell size in degrees (~200 m) for fast nearby lookup

STOPWORDS = {"the", "restaurant", "restaurants", "inc", "llc", "corp", "corporation", "ltd", "co", "nyc", "ny"}


# ---------------- helpers ----------------

def normalize_name(name):
    name = (name or "").lower().replace("&", " and ")
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    words = [w for w in name.split() if w not in STOPWORDS]
    return " ".join(words)


def distance_m(lat1, lng1, lat2, lng2):
    # Good-enough flat approximation for short distances in NYC
    return (((lat1 - lat2) * 111_000) ** 2 + ((lng1 - lng2) * 84_000) ** 2) ** 0.5


def cell(lat, lng):
    return (int(lat / GRID), int(lng / GRID))


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def name_similarity(a, b):
    if not a or not b:
        return 0.0
    if len(a) >= 4 and len(b) >= 4 and (a in b or b in a):
        return 0.9
    return SequenceMatcher(None, a, b).ratio()


# ---------------- step 1: DOHMH ----------------

def fetch_dohmh(all_cuisines=False):
    """all_cuisines=True fetches every Manhattan cuisine (for --survey); otherwise only the ones in CUISINES."""
    cuisine_list = ",".join("'" + c + "'" for c in CUISINES)
    where = "boro='Manhattan'" if all_cuisines else f"boro='Manhattan' AND cuisine_description in({cuisine_list})"
    fields = "camis,dba,building,street,zipcode,phone,cuisine_description,latitude,longitude"
    restaurants = {}
    offset, page_size = 0, 50000

    while True:
        params = {
            "$select": fields,
            "$where": where,
            "$group": fields,
            "$order": "camis",
            "$limit": page_size,
            "$offset": offset,
        }
        url = DOHMH_URL + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=120) as response:
            rows = json.loads(response.read())
        print(f"  DOHMH page at offset {offset}: {len(rows)} rows")

        for row in rows:
            camis = row.get("camis")
            lat, lng = to_float(row.get("latitude")), to_float(row.get("longitude"))
            label = row.get("cuisine_description")
            if not camis or camis in restaurants or not lat or not lng or not label:
                continue  # skip duplicates and rows without real coordinates (some are 0)
            restaurants[camis] = {
                "camis": camis,
                "dba": (row.get("dba") or "").strip(),
                "building": (row.get("building") or "").strip(),
                "street": (row.get("street") or "").strip(),
                "zipcode": (row.get("zipcode") or "").strip(),
                "phone": (row.get("phone") or "").strip(),
                "cuisine": label if all_cuisines else CUISINES[label],
                "lat": lat,
                "lng": lng,
            }

        if len(rows) < page_size:
            break
        offset += page_size

    return list(restaurants.values())


# ---------------- step 2: Google Local ----------------

def parse_line(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return ast.literal_eval(line.decode("utf-8") if isinstance(line, bytes) else line)


def fetch_google_index():
    """Stream the New York metadata file (no disk needed) and index Manhattan-area businesses by grid cell."""
    index = defaultdict(list)
    kept = scanned = 0
    request = urllib.request.Request(GOOGLE_META_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=300) as response:
        with gzip.GzipFile(fileobj=response) as stream:
            for line in stream:
                scanned += 1
                if scanned % 50000 == 0:
                    print(f"  scanned {scanned:,} Google businesses, kept {kept:,} near Manhattan")
                try:
                    biz = parse_line(line)
                except Exception:
                    continue
                lat, lng = to_float(biz.get("latitude")), to_float(biz.get("longitude"))
                if lat is None or lng is None:
                    continue
                box = MANHATTAN_BOX
                if not (box["lat_min"] <= lat <= box["lat_max"] and box["lng_min"] <= lng <= box["lng_max"]):
                    continue
                if biz.get("avg_rating") is None:
                    continue
                index[cell(lat, lng)].append({
                    "gmap_id": biz.get("gmap_id"),
                    "name": biz.get("name") or "",
                    "norm": normalize_name(biz.get("name")),
                    "lat": lat,
                    "lng": lng,
                    "rating": biz.get("avg_rating"),
                    "reviews": biz.get("num_of_reviews") or 0,
                    "closed": "permanently closed" in str(biz.get("state") or "").lower(),
                })
                kept += 1
    print(f"  done: scanned {scanned:,}, kept {kept:,}")
    return index


# ---------------- step 3: match ----------------

def find_match(restaurant, index):
    target = normalize_name(restaurant["dba"])
    cx, cy = cell(restaurant["lat"], restaurant["lng"])
    best, best_score = None, 0.0
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for biz in index.get((cx + dx, cy + dy), []):
                if distance_m(restaurant["lat"], restaurant["lng"], biz["lat"], biz["lng"]) > MAX_MATCH_DISTANCE_M:
                    continue
                score = name_similarity(target, biz["norm"])
                if score > best_score:
                    best, best_score = biz, score
    return best if best_score >= MIN_NAME_SIMILARITY else None


# ---------------- step 4: build items ----------------

def build_item(r, match, timestamp):
    item = {
        "BusinessID": r["camis"],
        "Name": match["name"] if match else r["dba"].title(),
        "Address": f"{r['building']} {r['street'].title()}, New York, NY {r['zipcode']}".strip(),
        "ZipCode": r["zipcode"],
        "Coordinates": {"Latitude": Decimal(str(r["lat"])), "Longitude": Decimal(str(r["lng"]))},
        "Cuisine": r["cuisine"],
        "insertedAtTimestamp": timestamp,
    }
    if r["phone"]:
        item["Phone"] = r["phone"]
    if match:
        item["Rating"] = Decimal(str(match["rating"]))
        item["NumberOfReviews"] = int(match["reviews"])
        item["GoogleMapsID"] = match["gmap_id"]
    return item


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="do everything except write to DynamoDB")
    parser.add_argument("--survey", action="store_true", help="print rated-restaurant counts for every Manhattan cuisine")
    args = parser.parse_args()
    random.seed(RANDOM_SEED)

    print("Step 1/4: downloading Manhattan restaurants from NYC DOHMH...")
    restaurants = fetch_dohmh(all_cuisines=args.survey)
    print(f"  unique restaurants: {len(restaurants)}")

    print("Step 2/4: streaming Google Local New York metadata (takes a few minutes)...")
    google_index = fetch_google_index()

    print("Step 3/4: matching restaurants to Google ratings...")
    matched_by_cuisine, unmatched_by_cuisine = defaultdict(list), defaultdict(list)
    for r in restaurants:
        match = find_match(r, google_index)
        if match and match["closed"]:
            continue  # Google says it's permanently closed: skip it
        (matched_by_cuisine if match else unmatched_by_cuisine)[r["cuisine"]].append((r, match))

    if args.survey:
        labels = set(matched_by_cuisine) | set(unmatched_by_cuisine)
        ranked = sorted(labels, key=lambda c: len(matched_by_cuisine[c]), reverse=True)
        print(f"\n  {'DOHMH cuisine label':<45}{'with rating':>12}{'no rating':>11}")
        for label in ranked[:30]:
            print(f"  {label:<45}{len(matched_by_cuisine[label]):>12}{len(unmatched_by_cuisine[label]):>11}")
        print("\nSurvey only: nothing was written to DynamoDB.")
        return

    timestamp = datetime.now(timezone.utc).isoformat()
    items = []
    print(f"\n  {'cuisine':<10}{'matched':>9}{'unmatched':>11}{'selected':>10}{'with rating':>13}")
    for cuisine in dict.fromkeys(CUISINES.values()):   # each bot cuisine once, even if several labels map to it
        matched, unmatched = matched_by_cuisine[cuisine], unmatched_by_cuisine[cuisine]
        random.shuffle(matched)
        random.shuffle(unmatched)
        pool = matched if REQUIRE_RATING else matched + unmatched   # rated restaurants always come first
        selected = pool[:PER_CUISINE]
        with_rating = sum(1 for _, m in selected if m)
        print(f"  {cuisine:<10}{len(matched):>9}{len(unmatched):>11}{len(selected):>10}{with_rating:>13}")
        items.extend(build_item(r, m, timestamp) for r, m in selected)

    print(f"\n  TOTAL selected: {len(items)}")
    print("  Example item:", json.dumps(items[0], default=str, indent=2) if items else "none")

    if args.dry_run:
        print("\nDry run: nothing was written to DynamoDB.")
        return

    print(f"\nStep 4/4: writing {len(items)} items to DynamoDB table '{TABLE_NAME}'...")
    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
    with table.batch_writer(overwrite_by_pkeys=["BusinessID"]) as writer:
        for i, item in enumerate(items, 1):
            writer.put_item(Item=item)
            if i % 200 == 0:
                print(f"  wrote {i}")
    time.sleep(1)
    print(f"Done. Wrote {len(items)} items.")

    with open("restaurants_loaded.json", "w") as f:
        json.dump([{"BusinessID": it["BusinessID"], "Cuisine": it["Cuisine"]} for it in items], f)
    print("Saved restaurants_loaded.json (BusinessID + Cuisine) for the OpenSearch step later.")


if __name__ == "__main__":
    main()
