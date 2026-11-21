#!/usr/bin/env python3
"""Monthly extract of Overture Maps Places (TW, JP, GB, US).

    python extract.py [--release 2026-11-18.0] [--out site]

Keeps open places with a website and a confidence of at least 0.7, with only: id,
name, category, website, city, region, confidence. Output, one gzip NDJSON file per
market and category:

    <out>/<release>/<cc>/<category>.ndjson.gz
    <out>/latest.json      {"release", "generated", "markets": {cc: {category: count}}}

Data: Overture Maps Foundation, Places theme (CDLA-Permissive-2.0, plus CC0-1.0 and
Apache-2.0 records). See LICENSE-DATA.md.
"""
import argparse
import datetime as dt
import gzip
import json
import os
import re
import sys
import urllib.request

import duckdb

BUCKET = 'overturemaps-us-west-2'
LISTING = f'https://{BUCKET}.s3.amazonaws.com/?list-type=2&prefix=release/&delimiter=/'
MIN_CONFIDENCE = 0.7

# (xmin, ymin, xmax, ymax) boxes; rows are then kept only when the address country matches.
MARKETS = {
    'TW': [(119.3, 21.8, 122.1, 25.4), (118.1, 24.3, 118.6, 24.6), (119.9, 25.9, 120.6, 26.4)],
    'JP': [(122.9, 24.0, 146.0, 45.6)],
    'GB': [(-8.7, 49.8, 1.8, 60.9)],
    'US': [(-125.0, 24.0, -66.9, 49.5), (-170.0, 51.0, -129.0, 71.5), (-161.0, 18.9, -154.8, 22.3)],
}

SAFE = re.compile(r'[^a-z0-9_]')


def latest_release() -> str:
    with urllib.request.urlopen(LISTING, timeout=60) as res:
        body = res.read().decode()
    releases = re.findall(r'<Prefix>release/([0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+)/</Prefix>', body)
    if not releases:
        sys.exit('no Overture release found')
    return sorted(releases)[-1]


def category_file(category: str | None) -> str:
    return SAFE.sub('_', (category or 'uncategorized').lower())[:80] or 'uncategorized'


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--release')
    ap.add_argument('--out', default='site')
    ap.add_argument('--markets', default=','.join(MARKETS))
    args = ap.parse_args()
    release = args.release or latest_release()
    src = f"s3://{BUCKET}/release/{release}/theme=places/type=place/*"

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")
    index: dict = {'release': release, 'generated': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'), 'minConfidence': MIN_CONFIDENCE, 'markets': {}}
    for cc in args.markets.split(','):
        boxes = ' OR '.join(f'(bbox.xmin BETWEEN {a} AND {c} AND bbox.ymin BETWEEN {b} AND {d})' for a, b, c, d in MARKETS[cc])
        rows = con.execute(f"""
            SELECT id, names.primary AS name, basic_category AS category, websites[1] AS website,
                   addresses[1].locality AS city, addresses[1].region AS region, round(confidence, 2) AS confidence
            FROM read_parquet('{src}', hive_partitioning=1)
            WHERE ({boxes})
              AND addresses[1].country = ?
              AND confidence >= {MIN_CONFIDENCE}
              AND coalesce(operating_status, 'open') = 'open'
              AND websites[1] IS NOT NULL AND names.primary IS NOT NULL
            ORDER BY category, id
        """, [cc]).fetchall()
        counts: dict[str, int] = {}
        files: dict[str, list] = {}
        for rid, name, category, website, city, region, conf in rows:
            key = category_file(category)
            files.setdefault(key, []).append({'id': rid, 'name': name, 'category': category, 'website': website, 'city': city, 'region': region, 'confidence': conf})
        folder = os.path.join(args.out, release, cc.lower())
        os.makedirs(folder, exist_ok=True)
        for key, items in files.items():
            with gzip.open(os.path.join(folder, f'{key}.ndjson.gz'), 'wt', encoding='utf-8') as fh:
                for item in items:
                    fh.write(json.dumps(item, ensure_ascii=False, separators=(',', ':')) + '\n')
            counts[key] = len(items)
        index['markets'][cc.lower()] = dict(sorted(counts.items()))
        print(f'{cc}: {len(rows)} places in {len(files)} categories', flush=True)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, 'latest.json'), 'w', encoding='utf-8') as fh:
        json.dump(index, fh, ensure_ascii=False, separators=(',', ':'))


if __name__ == '__main__':
    main()
