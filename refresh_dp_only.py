"""
refresh_dp_only.py — Targeted Snowflake pull for APO Unit Investment & APO FPD$ only.

Downloads the existing DBFS blob, replaces only the DP records (d.dp) with
fresh Snowflake data, updates filter options (d.oo), and re-uploads.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from push_demand_plan import pe_labels_in_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DBFS_PATH          = "dbfs:/FileStore/ebp_dashboard/demand_plan_blob.json"
DATABRICKS_PROFILE = "4428761713917856"

SF_TABLE = "DA_DSM_SCANALYTICS_PROD.INTEGRATED.GLBL_DEMAND_PLAN_V"

DIV_SHORT_MAP = {
    "FOOTWEAR": "FW", "APPAREL": "AP", "EQUIPMENT & ACCESSORIES": "ACC",
    "EQUIPMENT": "ACC",
}

NIKE_SPORT_MAP = {
    "RUNNING": "Running", "TRAINING": "Training", "BASKETBALL": "Basketball",
    "GLOBAL FOOTBALL": "Global Football", "SPORTSWEAR": "Sportswear",
    "TENNIS": "Tennis", "GOLF": "Golf", "SKATE": "Skate",
    "AMERICAN FOOTBALL": "American Football", "BASEBALL": "Baseball",
    "SPECIALTY SPORTS": "Specialty Sports", "CRICKET": "Cricket",
}

DP_VALID_GATES = {"GCA", "MAR", "SCP1", "SCP2", "SCP3", "F1", "F2", "F3", "F4"}

MAPPED_SPORT_ORDER = [
    "Running", "Training", "Basketball", "Global Football", "Sportswear", "Kids",
    "Tennis", "Golf", "ACG", "Skate", "NikeSkims", "American Football", "Baseball",
    "Specialty Sports", "Cricket", "Streetwear", "FBAT", "Other",
]


def _normalize_season_sf(raw: str) -> str:
    s = str(raw).strip()
    if len(s) >= 6 and s[:4].isdigit() and s[4:].isalpha():
        return s[4:].upper() + s[:4]
    return s


def _normalize_brand_long(raw: str) -> str | None:
    u = (raw or "").strip().upper()
    if "JORDAN" in u: return "Jordan Brand"
    if "NIKE" in u: return "Nike Brand"
    return None


def _normalize_consumer(raw: str) -> str:
    u = (raw or "").strip().upper()
    if u in ("MENS", "MEN", "ADULT MALE"):   return "Mens"
    if u in ("WOMENS", "WOMEN", "ADULT FEMALE"): return "Womens"
    if u in ("KIDS", "KID", "YOUTH MALE", "YOUTH FEMALE", "YOUTH UNISEX",
             "TODDLER MALE", "TODDLER FEMALE", "TODDLER UNISEX",
             "INFANT MALE", "INFANT FEMALE", "INFANT UNISEX"):
        return "Kids"
    return "Other"


def _map_sport_nike(gsf, consumer, sub_brand):
    gsf_u = (gsf or "").strip().upper()
    con_u = (consumer or "").strip().upper()
    sb_u  = (sub_brand or "").strip().upper()
    if con_u in ("KIDS", "KID", "YOUTH MALE", "YOUTH FEMALE", "YOUTH UNISEX",
                 "TODDLER MALE", "TODDLER FEMALE", "TODDLER UNISEX",
                 "INFANT MALE", "INFANT FEMALE", "INFANT UNISEX"):
        return "Kids"
    if con_u not in ("MENS", "WOMENS", "MEN", "WOMEN", "ADULT MALE", "ADULT FEMALE", "ADULT UNISEX"):
        return None
    if "ACG" in sb_u:
        return "ACG"
    if "SKIMS" in sb_u or sb_u == "NIKESKIMS":
        return "NikeSkims"
    if gsf_u == "STREETWEAR":
        return None
    return NIKE_SPORT_MAP.get(gsf_u, "Other")


def _map_sport_jordan(gsf, consumer):
    con_u = (consumer or "").strip().upper()
    gsf_u = (gsf or "").strip().upper()
    if con_u in ("KIDS", "KID", "YOUTH MALE", "YOUTH FEMALE", "YOUTH UNISEX",
                 "TODDLER MALE", "TODDLER FEMALE", "TODDLER UNISEX",
                 "INFANT MALE", "INFANT FEMALE", "INFANT UNISEX"):
        return "Kids"
    if con_u not in ("MENS", "WOMENS", "MEN", "WOMEN", "ADULT MALE", "ADULT FEMALE", "ADULT UNISEX"):
        return None
    if gsf_u == "STREETWEAR": return "Streetwear"
    if gsf_u == "BASKETBALL": return "Basketball"
    if gsf_u == "GOLF":       return "Golf"
    return "FBAT"


def _map_sport(gsf, consumer, sub_brand, brand):
    if "JORDAN" in (brand or "").upper():
        return _map_sport_jordan(gsf, consumer)
    return _map_sport_nike(gsf, consumer, sub_brand)


def _safe_float(v) -> float:
    try:
        if v is None or pd.isna(v):
            return 0.0
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _season_sort_key(s: str):
    p = {"FA": 0, "HO": 1, "SP": 2, "SU": 3}
    if s and len(s) >= 6 and s[:2].upper() in p:
        return (s[2:], p[s[:2].upper()])
    return (s, 9)


def _build_dp_sql() -> str:
    """Query only the columns needed for DP/APO records."""
    return f"""
SELECT
    BUSINESS_SEASON_CD,
    MILESTONE_CD,
    BRAND_NAME,
    SUPER_GEOGRAPHY_CD,
    PRODUCT_PLANNING_DIVISION_NAME,
    CONSUMER_CONSTRUCT_GLOBAL_CONSUMER_OFFENSE_NAME AS CONSUMER,
    GLOBAL_SPORT_FOCUS,
    SUB_BRAND_DESC,
    FAMILY,
    SUB_FAMILY,
    SUM(DEMAND_PLAN_QTY_3_0)                            AS DP_QTY,
    SUM(GLOBAL_WHOLESALE_PRICE_USD * DEMAND_PLAN_QTY_3_0) AS WHS_DOLLARS
FROM {SF_TABLE}
WHERE TRY_TO_NUMBER(LEFT(BUSINESS_SEASON_CD, 4)) >= 2025
  AND DP_GLBLGEODUPFLTRIND = 'Y'
  AND SUPER_GEOGRAPHY_CD IN ('NA', 'EMEA', 'GC', 'APLA')
  AND BRAND_NAME IN ('Nike', 'Jordan')
GROUP BY
    BUSINESS_SEASON_CD,
    MILESTONE_CD,
    BRAND_NAME,
    SUPER_GEOGRAPHY_CD,
    PRODUCT_PLANNING_DIVISION_NAME,
    CONSUMER_CONSTRUCT_GLOBAL_CONSUMER_OFFENSE_NAME,
    GLOBAL_SPORT_FOCUS,
    SUB_BRAND_DESC,
    FAMILY,
    SUB_FAMILY
"""


def build_dp_records(df: pd.DataFrame) -> list[dict]:
    """Process Snowflake DataFrame → DP records for d.dp."""
    logger.info("Processing %d rows for DP records", len(df))
    dp_agg = defaultdict(lambda: [0.0, 0.0])

    for _, row in df.iterrows():
        season_raw = str(row.get("BUSINESS_SEASON_CD", "")).strip()
        season     = _normalize_season_sf(season_raw) if season_raw else ""
        if not season or season.upper() in ("TOTAL", "GRAND", "NAN"):
            continue

        brand_l = _normalize_brand_long(str(row.get("BRAND_NAME", "")))
        if not brand_l:
            continue

        milestone = str(row.get("MILESTONE_CD", "")).strip()
        if milestone not in DP_VALID_GATES:
            continue

        geo       = str(row.get("SUPER_GEOGRAPHY_CD", "")).strip()
        div_short = DIV_SHORT_MAP.get(
            str(row.get("PRODUCT_PLANNING_DIVISION_NAME", "")).strip(), ""
        )
        consumer  = str(row.get("CONSUMER", "")).strip()
        gsf       = str(row.get("GLOBAL_SPORT_FOCUS", "")).strip()
        sub_brand = str(row.get("SUB_BRAND_DESC", "")).strip()
        pf        = str(row.get("FAMILY", "")).strip()
        psf       = str(row.get("SUB_FAMILY", "")).strip()

        dp_qty = _safe_float(row.get("DP_QTY"))
        whs    = _safe_float(row.get("WHS_DOLLARS"))

        msp = _map_sport(gsf, consumer, sub_brand, brand_l)
        if not msp:
            continue

        dp_key = (season, milestone, brand_l, geo, div_short,
                  sub_brand, gsf, msp, pf, psf,
                  _normalize_consumer(consumer))
        a = dp_agg[dp_key]
        a[0] += dp_qty
        a[1] += whs

    dp = [
        {"s": s, "gt": gt, "b": b, "g": g, "d": d, "sb": sb, "sp": sp,
         "msp": msp, "pf": pf, "psf": psf, "con": con,
         "adp": round(vals[0], 2), "afpd": round(vals[1], 2)}
        for (s, gt, b, g, d, sb, sp, msp, pf, psf, con), vals
        in dp_agg.items()
    ]
    logger.info("  Built %d DP records", len(dp))
    return dp


def update_opdp_filter_options(op_recs, dp_recs):
    """Rebuild OPDP filter options from OP + new DP records."""
    seasons = set(); geos = set(); divs = set()
    sub_brands = set(); sports = set(); consumers = set()
    pdd_families = set(); pdd_sub_families = set()

    for r in op_recs:
        if r.get("s"):   seasons.add(r["s"])
        if r.get("g"):   geos.add(r["g"])
        if r.get("d"):   divs.add(r["d"])
        if r.get("sb"):  sub_brands.add(r["sb"])
        if r.get("msp"): sports.add(r["msp"])
        if r.get("con"): consumers.add(r["con"])
    for r in dp_recs:
        if r.get("s"):   seasons.add(r["s"])
        if r.get("g"):   geos.add(r["g"])
        if r.get("d"):   divs.add(r["d"])
        if r.get("sb"):  sub_brands.add(r["sb"])
        if r.get("msp"): sports.add(r["msp"])
        if r.get("pf"):  pdd_families.add(r["pf"])
        if r.get("psf"): pdd_sub_families.add(r["psf"])
        if r.get("con"): consumers.add(r["con"])

    fbat_set = {"American Football", "Global Football", "Baseball", "Training"}
    if sports & fbat_set:
        sports.add("FBAT")

    sp_order  = {s: i for i, s in enumerate(MAPPED_SPORT_ORDER)}
    con_order = {"Mens": 0, "Womens": 1, "Kids": 2, "Other": 3}
    return {
        "seasons":          sorted(seasons, key=_season_sort_key, reverse=True),
        "geos":             sorted(geos),
        "divs":             pe_labels_in_data(divs),
        "sub_brands":       sorted(sub_brands),
        "sports":           sorted(sports, key=lambda x: sp_order.get(x, 99)),
        "consumers":        sorted(consumers, key=lambda x: con_order.get(x, 99)),
        "pdd_families":     sorted(pdd_families),
        "pdd_sub_families": sorted(pdd_sub_families),
    }


def main():
    t0 = time.time()

    # Step 1: Download existing blob from DBFS
    logger.info("Downloading current blob from DBFS...")
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
        tmp_dl = tmp.name

    r = subprocess.run(
        ["databricks", "fs", "cp", DBFS_PATH, tmp_dl,
         "--overwrite", "--profile", DATABRICKS_PROFILE],
        capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        logger.error("Failed to download blob: %s", r.stderr)
        sys.exit(1)

    with open(tmp_dl, "r", encoding="utf-8") as f:
        blob = json.load(f)
    os.unlink(tmp_dl)
    logger.info("  Existing blob loaded: dp=%d, op=%d, u=%d, fr=%d, sro=%d",
                len(blob.get("dp", [])), len(blob.get("op", [])),
                len(blob.get("u", [])), len(blob.get("fr", [])),
                len(blob.get("sro", [])))

    # Step 2: Query Snowflake for DP records only
    logger.info("Querying Snowflake for DP/APO data only...")
    from snowflake_client import query_dataframe
    sql = _build_dp_sql()
    sf_df = query_dataframe(sql)
    logger.info("  Snowflake returned %d rows", len(sf_df))

    # Log distinct seasons to verify SP2027/SU2027
    seasons_in_sf = sorted(sf_df["BUSINESS_SEASON_CD"].dropna().unique().tolist())
    logger.info("  Seasons in Snowflake: %s", seasons_in_sf)

    milestones_in_sf = sorted(sf_df["MILESTONE_CD"].dropna().unique().tolist())
    logger.info("  Milestones in Snowflake: %s", milestones_in_sf)

    # Step 3: Build new DP records
    new_dp = build_dp_records(sf_df)

    dp_seasons = sorted({r["s"] for r in new_dp}, key=_season_sort_key, reverse=True)
    logger.info("  DP seasons after processing: %s", dp_seasons)

    # Step 4: Replace dp and update oo filter options in blob
    blob["dp"] = new_dp
    blob["oo"] = update_opdp_filter_options(blob.get("op", []), new_dp)
    blob["m"]["ts"]  = datetime.now(timezone.utc).isoformat()
    blob["m"]["src"] = "snowflake-dp+excel"

    # Step 5: Upload back to DBFS
    blob_json = json.dumps(blob, separators=(",", ":"))
    size_mb   = len(blob_json) / (1024 * 1024)
    logger.info("Updated blob size: %.2f MB", size_mb)

    logger.info("Uploading updated blob to DBFS...")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        tmp.write(blob_json)
        tmp_path = tmp.name
    try:
        r = subprocess.run(
            ["databricks", "fs", "cp", tmp_path, DBFS_PATH,
             "--overwrite", "--profile", DATABRICKS_PROFILE],
            capture_output=True, text=True, timeout=300,
        )
        if r.returncode != 0:
            logger.error("Upload failed: %s", r.stderr)
            sys.exit(1)
    finally:
        os.unlink(tmp_path)

    elapsed = time.time() - t0
    logger.info(
        "\n Done in %.0fs\n"
        "  New DP records : %d\n"
        "  DP seasons     : %s\n"
        "  Blob size      : %.2f MB",
        elapsed, len(new_dp), dp_seasons, size_mb,
    )


if __name__ == "__main__":
    main()
