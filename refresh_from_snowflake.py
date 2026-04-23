"""
refresh_from_snowflake.py — Combined ETL: Snowflake APO + OP Submit Excel → DBFS blob.

Replaces Demand Plan Dashboard Excel with direct Snowflake queries for:
  - UIF records (d.u)     : Unit Investment Flow (all gate-level data)
  - DP records  (d.dp)    : APO metrics for OPDP tab
  - FR records  (d.fr)    : Franchise Breakdown

Keeps OP Submit Excel as source for:
  - OP records  (d.op)    : OP plan metrics
  - SRO records (d.sro)   : Seasonal Revenue Overview

Run locally:  python refresh_from_snowflake.py
Override:     python refresh_from_snowflake.py --op-submit-xlsx "path"
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Ensure sibling ``push_demand_plan`` is importable (CLI runs from app directory)
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from push_demand_plan import get_uif_filter_options, get_sro_filter_options, pe_labels_in_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DBFS_PATH          = "dbfs:/FileStore/ebp_dashboard/demand_plan_blob.json"
DATABRICKS_PROFILE = "4428761713917856"

_DEFAULT_DIR = Path(Path(__file__).parent.parent)

# ── Shared constants (same as push_demand_plan.py) ──────────────────────────

GEO_NORMALIZE = {
    "NORTH AMERICA": "NA", "NORTH_AMERICA": "NA", "NA": "NA",
    "EUROPEAFRICA": "EMEA", "EUROPE AFRICA": "EMEA", "EMEA": "EMEA",
    "GREATER CHINA": "GC", "GC": "GC",
    "APLA": "APLA", "ASIA PACIFIC LATIN AMERICA": "APLA",
}

DIV_MAP = {
    "APPAREL DIVISION": "AP", "FOOTWEAR DIVISION": "FW",
    "EQUIPMENT DIVISION": "ACC",
}

DIV_SHORT_MAP = {
    "FOOTWEAR": "FW", "APPAREL": "AP", "EQUIPMENT & ACCESSORIES": "ACC",
    "EQUIPMENT": "ACC",
}

BRAND_SHORT = {"NIKE": "Nike", "JORDAN": "Jordan"}
BRAND_LONG  = {"NIKE": "Nike Brand", "JORDAN": "Jordan Brand"}

# Code gates (legacy) + APO/forecast milestones now returned by GLBL_DEMAND_PLAN_V
GATE_ORDER     = [
    "GCA", "MAR", "SCP1", "SCP2", "SCP3",
    "F1", "F2", "F3", "F4", "CURRENTFORECAST",
]
DP_VALID_GATES = {
    "GCA", "MAR", "SCP1", "SCP2", "SCP3", "F1", "F2", "F3", "F4",
    "POST_PROD_BRIEF", "POST_PROD_ALIGN", "PRE_PROD_FINAL",
    "GEO_STYLE", "GEO_STYLE_COLOR", "GBL_GEO_PIVOT", "POST_MPU-REVIEW",
}

NIKE_PRIMARY_SPORTS = [
    "Running", "Training", "Basketball", "Global Football", "Sportswear", "Kids",
]

MAPPED_SPORT_ORDER = [
    "Running", "Training", "Basketball", "Global Football", "Sportswear", "Kids",
    "Tennis", "Golf", "ACG", "Skate", "NikeSkims", "American Football", "Baseball",
    "Specialty Sports", "Cricket", "Streetwear", "FBAT", "Other",
]

NIKE_SPORT_MAP = {
    "RUNNING": "Running", "TRAINING": "Training", "BASKETBALL": "Basketball",
    "GLOBAL FOOTBALL": "Global Football", "SPORTSWEAR": "Sportswear",
    "TENNIS": "Tennis", "GOLF": "Golf", "SKATE": "Skate",
    "AMERICAN FOOTBALL": "American Football", "BASEBALL": "Baseball",
    "SPECIALTY SPORTS": "Specialty Sports", "CRICKET": "Cricket",
}

JORDAN_SPORT_MAP = {
    "STREETWEAR": "Streetwear", "BASKETBALL": "Basketball", "GOLF": "Golf",
    "AMERICAN FOOTBALL": "American Football", "GLOBAL FOOTBALL": "Global Football",
    "BASEBALL": "Baseball", "TRAINING": "Training",
}

# ── Snowflake table & scope ────────────────────────────────────────────────
SF_TABLE = "DA_DSM_SCANALYTICS_PROD.INTEGRATED.GLBL_DEMAND_PLAN_V"
# Business seasons with calendar year in BUSINESS_SEASON_CD (e.g. 2025SP, 2026FA)
MIN_BUSINESS_SEASON_CALENDAR_YEAR = 2025

# For default season: use single-column timestamp on F1 rows (set SNOWFLAKE_F1_SUBMIT_DTTM_COL
# to the exact name from DESCRIBE VIEW if needed). Tried in order after env override.
F1_SUBMIT_DTTM_CANDIDATES = [
    "DEMAND_PLAN_SUBMIT_DTTM",
    "DEMAND_PLAN_LAST_MODIFIED_DTTM",
    "DATA_REFRESH_DTTM",
    "DATA_AS_OF_DTTM",
    "SNAPSHOT_DTTM",
    "SRC_LOAD_DTTM",
    "BATCH_DTTM",
    "AS_OF_DTTM",
]

# Snowflake → dashboard column mapping
# Season: BUSINESS_SEASON_CD ('2026FA') → 'FA2026'
# Geo: SUPER_GEOGRAPHY_CD ('NA','EMEA','GC','APLA')
# Brand: BRAND_NAME ('Nike','Jordan')
# Division full: DIVISION_DESC ('FOOTWEAR DIVISION')
# Division short: PRODUCT_PLANNING_DIVISION_NAME ('FOOTWEAR')
# Consumer: CONSUMER_CONSTRUCT_GLOBAL_CONSUMER_OFFENSE_NAME ('WOMENS','MENS','KIDS')
# Sport: GLOBAL_SPORT_FOCUS ('SPORTSWEAR','RUNNING', etc.)
# SubBrand: SUB_BRAND_DESC
# Franchise: FRANCHISE
# Family: FAMILY (→ PDD Family)
# SubFamily: SUB_FAMILY (→ PDD SubFamily)
# MerchClass: MERCH_CLASSIFICATION
# Gate/Milestone: MILESTONE_CD
# DP Qty: DEMAND_PLAN_QTY_3_0
# WHS$: GLOBAL_WHOLESALE_PRICE_USD * DEMAND_PLAN_QTY_3_0

# UIF / gate series (align with GATE_ORDER in blob + static/index.html G)
UIF_GATES = tuple(GATE_ORDER) + ("LY",)

# Map UIF "div" (DIVISION_DESC) to OP/DP Product Engine codes (d)
_DIV_DESC_TO_PE = {
    "APPAREL DIVISION": "AP",
    "FOOTWEAR DIVISION": "FW",
    "EQUIPMENT DIVISION": "ACC",
}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _normalize_season_sf(raw: str) -> str:
    """Convert Snowflake season '2026FA' → 'FA2026'."""
    s = str(raw).strip()
    if len(s) >= 6 and s[:4].isdigit() and s[4:].isalpha():
        return s[4:].upper() + s[:4]
    return s


def _normalize_geo(raw: str) -> str:
    return GEO_NORMALIZE.get((raw or "").strip().upper(), (raw or "").strip())


def _normalize_brand_short(raw: str) -> str | None:
    u = (raw or "").strip().upper()
    if "JORDAN" in u: return "Jordan"
    if "NIKE" in u: return "Nike"
    return None


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


def _map_sport_nike(gsf: str, consumer: str, sub_brand: str) -> str | None:
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


def _map_sport_jordan(gsf: str, consumer: str) -> str | None:
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


def _map_sport(gsf: str, consumer: str, sub_brand: str, brand: str) -> str | None:
    if "JORDAN" in (brand or "").upper():
        return _map_sport_jordan(gsf, consumer)
    return _map_sport_nike(gsf, consumer, sub_brand)


def _season_sort_key(s: str):
    p = {"FA": 0, "HO": 1, "SP": 2, "SU": 3}
    if s and len(s) >= 6 and s[:2].upper() in p:
        return (s[2:], p[s[:2].upper()])
    return (s, 9)


def _safe_float(v) -> float:
    try:
        if v is None or pd.isna(v):
            return 0.0
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _business_season_year(normalized: str) -> int:
    """Parse calendar year from normalized FA2026 / raw 2025SP-style code."""
    s = (normalized or "").strip()
    if not s:
        return 0
    if s[:4].isdigit() and len(s) >= 5:
        try:
            return int(s[:4])
        except ValueError:
            return 0
    if len(s) >= 6 and s[2:6].isdigit():
        try:
            return int(s[2:6])
        except ValueError:
            return 0
    return 0


def _ap_scope_where() -> str:
    """Common season / geo / brand filter for GLBL_DEMAND_PLAN_V (``MIN_BUSINESS_SEASON_CALENDAR_YEAR``+)."""
    y = int(MIN_BUSINESS_SEASON_CALENDAR_YEAR)
    return (
        f"TRY_TO_NUMBER(LEFT(BUSINESS_SEASON_CD, 4)) >= {y}\n"
        f"  AND DP_GLBLGEODUPFLTRIND = 'Y'\n"
        f"  AND SUPER_GEOGRAPHY_CD IN ('NA', 'EMEA', 'GC', 'APLA')\n"
        f"  AND BRAND_NAME IN ('Nike', 'Jordan')"
    )


def _try_default_season_f1_max_submit() -> tuple[str | None, str | None]:
    """Default season: ``BUSINESS_SEASON_CD`` of the F1 row with the latest submit-like timestamp.

    Tries env ``SNOWFLAKE_F1_SUBMIT_DTTM_COL`` first, then ``F1_SUBMIT_DTTM_CANDIDATES``.
    Returns (normalized_season, column_used) or (None, None) if no column works.
    """
    from snowflake_client import query_dataframe
    from snowflake.connector.errors import ProgrammingError

    prefer = (os.environ.get("SNOWFLAKE_F1_SUBMIT_DTTM_COL") or "").strip()
    cols = [prefer] if prefer else []
    cols += [c for c in F1_SUBMIT_DTTM_CANDIDATES if c and c not in cols]

    ap = _ap_scope_where()
    for col in cols:
        sql = f"""
SELECT
  BUSINESS_SEASON_CD
FROM {SF_TABLE}
WHERE MILESTONE_CD = 'F1'
  AND {ap}
  AND {col} IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY {col} DESC) = 1
"""
        try:
            df = query_dataframe(sql)
        except ProgrammingError as e:
            msg = (getattr(e, "msg", None) or str(e) or "").upper()
            if "INVALID" in msg or "DOES NOT EXIST" in msg or "SQL COMPILATION" in msg or "UNRECOGNIZED" in msg:
                logger.info("F1 default: skip column %r (%s)", col, e)
                continue
            raise
        except Exception as e:  # noqa: BLE001
            logger.info("F1 default: skip column %r (%s)", col, e)
            continue
        if df is None or len(df) == 0 or "BUSINESS_SEASON_CD" not in df.columns:
            continue
        raw = str(df["BUSINESS_SEASON_CD"].iloc[0] or "").strip()
        if not raw:
            continue
        norm = _normalize_season_sf(raw) if (len(raw) >= 6 and raw[:4].isdigit()) else raw
        logger.info("Default season (F1 max %s): %s (raw=%s)", col, norm, raw)
        return (norm, col)
    logger.warning(
        "F1 default: no row with a known submit timestamp. Set SNOWFLAKE_F1_SUBMIT_DTTM_COL to a "
        "column on %s, or we fall back to newest APO/OP season in %s+.",
        SF_TABLE,
        MIN_BUSINESS_SEASON_CALENDAR_YEAR,
    )
    return (None, None)


# ══════════════════════════════════════════════════════════════════════════════
# Snowflake data loading
# ══════════════════════════════════════════════════════════════════════════════

def _build_snowflake_sql() -> str:
    """Single aggregation query that pulls all data needed for UIF, DP, and FR.

    Set env ``SNOWFLAKE_BNB_COL`` to a view column (Branded/Non-branded) to
    populate BnB on UIF/FR rows. If unset, BnB remains blank in the app.
    """
    w = _ap_scope_where()
    bnb_col = (os.environ.get("SNOWFLAKE_BNB_COL") or "").strip()
    bnb_select = (
        f"TRIM(TRY_TO_VARCHAR({bnb_col})) AS BNB_SOURCE,\n    "
        if bnb_col else
        ""
    )
    bnb_group = f",\n    TRIM(TRY_TO_VARCHAR({bnb_col}))" if bnb_col else ""
    return f"""
SELECT
    BUSINESS_SEASON_CD,
    MILESTONE_CD,
    BRAND_NAME,
    SUPER_GEOGRAPHY_CD,
    DIVISION_DESC,
    PRODUCT_PLANNING_DIVISION_NAME,
    CONSUMER_CONSTRUCT_GLOBAL_CONSUMER_OFFENSE_NAME AS CONSUMER,
    GLOBAL_SPORT_FOCUS,
    SUB_BRAND_DESC,
    FRANCHISE,
    FAMILY,
    SUB_FAMILY,
    MERCH_CLASSIFICATION,
    {bnb_select}SUM(DEMAND_PLAN_QTY_3_0)                            AS DP_QTY,
    SUM(GLOBAL_WHOLESALE_PRICE_USD * DEMAND_PLAN_QTY_3_0) AS WHS_DOLLARS
FROM {SF_TABLE}
WHERE {w}
GROUP BY
    BUSINESS_SEASON_CD,
    MILESTONE_CD,
    BRAND_NAME,
    SUPER_GEOGRAPHY_CD,
    DIVISION_DESC,
    PRODUCT_PLANNING_DIVISION_NAME,
    CONSUMER_CONSTRUCT_GLOBAL_CONSUMER_OFFENSE_NAME,
    GLOBAL_SPORT_FOCUS,
    SUB_BRAND_DESC,
    FRANCHISE,
    FAMILY,
    SUB_FAMILY,
    MERCH_CLASSIFICATION{bnb_group}
"""


def load_snowflake_data(df: pd.DataFrame):
    """Process Snowflake aggregated DataFrame → (uif_rows, dp_rows, fr_rows)."""
    logger.info("Processing %d aggregated rows from Snowflake", len(df))

    uif_agg = defaultdict(lambda: [0.0, 0.0])
    dp_agg  = defaultdict(lambda: [0.0, 0.0])
    fr_agg  = defaultdict(lambda: [0.0, 0.0])

    for _, row in df.iterrows():
        season_raw  = str(row.get("BUSINESS_SEASON_CD", "")).strip()
        season      = _normalize_season_sf(season_raw) if season_raw else ""
        if not season or season.upper() in ("TOTAL", "GRAND", "NAN"):
            continue

        brand_raw   = str(row.get("BRAND_NAME", "")).strip()
        brand_s     = _normalize_brand_short(brand_raw)
        brand_l     = _normalize_brand_long(brand_raw)
        if not brand_s or not brand_l:
            continue

        milestone   = str(row.get("MILESTONE_CD", "")).strip()
        geo         = str(row.get("SUPER_GEOGRAPHY_CD", "")).strip()
        div_full    = str(row.get("DIVISION_DESC", "")).strip()
        div_short   = DIV_SHORT_MAP.get(
            str(row.get("PRODUCT_PLANNING_DIVISION_NAME", "")).strip(), ""
        )
        consumer    = str(row.get("CONSUMER", "")).strip()
        gsf         = str(row.get("GLOBAL_SPORT_FOCUS", "")).strip()
        sub_brand   = str(row.get("SUB_BRAND_DESC", "")).strip()
        franchise   = str(row.get("FRANCHISE", "")).strip()
        pf          = str(row.get("FAMILY", "")).strip()
        psf         = str(row.get("SUB_FAMILY", "")).strip()
        mc          = str(row.get("MERCH_CLASSIFICATION", "")).strip()

        dp_qty      = _safe_float(row.get("DP_QTY"))
        whs         = _safe_float(row.get("WHS_DOLLARS"))

        bnb = str(row.get("BNB_SOURCE", "") or "").strip()

        # ── UIF aggregation (Code Gate milestones only) ──
        gate_upper = milestone.upper().replace(" ", "").replace("/", "")
        if gate_upper in GATE_ORDER or gate_upper == "LY":
            uif_key = (season, gate_upper, brand_s, geo, div_full, consumer,
                       gsf, sub_brand, franchise, pf, psf, bnb, mc)
            a = uif_agg[uif_key]
            a[0] += dp_qty
            a[1] += whs

        # ── DP / APO aggregation (wider gate set) ──
        if milestone in DP_VALID_GATES:
            msp = _map_sport(gsf, consumer, sub_brand, brand_l)
            if msp:
                dp_key = (season, milestone, brand_l, geo, div_short,
                          sub_brand, gsf, msp, pf, psf,
                          _normalize_consumer(consumer))
                a = dp_agg[dp_key]
                a[0] += dp_qty
                a[1] += whs

        # ── Franchise aggregation (all milestones with data) ──
        if milestone:
            msp_fr = _map_sport(gsf, consumer, sub_brand, brand_l)
            if msp_fr is not None:
                fr_up = franchise.upper()
                franchise_norm = (
                    "_Other" if (not franchise or fr_up in
                                 ("NO FRANCHISE", "NOT_SUPPLD", "NONE", "NAN", "*UNK*", "UNK"))
                    else franchise
                )
                sb_n = sub_brand.strip() if sub_brand else ""
                sb_n = (
                    "Not Supplied" if (not sb_n or sb_n.upper() in
                                      ("NOT_SUPPLD", "NONE", "NAN", "", "UNK", "*UNK*"))
                    else sb_n
                )
                fr_key = (season, milestone, geo, div_short, brand_l,
                          _normalize_consumer(consumer), msp_fr,
                          franchise_norm, pf, psf, sb_n, bnb)
                a = fr_agg[fr_key]
                a[0] += dp_qty
                a[1] += whs

    uif = [
        {
            "s": s, "g": g, "b": b, "geo": geo, "div": div,
            "d": _DIV_DESC_TO_PE.get((div or "").strip(), ""),
            "con": con, "gsf": gsf, "sb": sb, "fr": fr, "pf": pf, "psf": psf, "bnb": bnb, "mc": mc,
            "q": round(vals[0], 2), "w": round(vals[1], 2),
        }
        for (s, g, b, geo, div, con, gsf, sb, fr, pf, psf, bnb, mc), vals
        in uif_agg.items()
    ]

    dp = [
        {"s": s, "gt": gt, "b": b, "g": g, "d": d, "sb": sb, "sp": sp,
         "msp": msp, "pf": pf, "psf": psf, "con": con,
         "adp": round(vals[0], 2), "afpd": round(vals[1], 2)}
        for (s, gt, b, g, d, sb, sp, msp, pf, psf, con), vals
        in dp_agg.items()
    ]

    fr = [
        {"s": s, "gt": gt, "geo": geo, "dv": dv, "b": b, "con": con,
         "sp": sp, "fr": fr_, "pf": pf, "psf": psf, "sb": sb, "bnb": bnb,
         "u": round(vals[0], 0), "fpd": round(vals[1], 2)}
        for (s, gt, geo, dv, b, con, sp, fr_, pf, psf, sb, bnb), vals
        in fr_agg.items()
    ]

    logger.info("  Snowflake → UIF: %d, DP: %d, FR: %d", len(uif), len(dp), len(fr))
    if not uif and not df.empty and "MILESTONE_CD" in df.columns:
        raw = df["MILESTONE_CD"].dropna().astype(str).str.strip()
        norm = (
            raw.str.upper()
            .str.replace(" ", "", regex=False)
            .str.replace("/", "", regex=False)
        )
        top = norm.value_counts().head(20)
        logger.warning(
            "UIF is 0 rows: UIF only counts %s. Top MILESTONE_CD (normalized) from data: %s",
            str(list(GATE_ORDER) + ["LY"]),
            top.to_dict(),
        )
    return uif, dp, fr


# ══════════════════════════════════════════════════════════════════════════════
# Filter option builders (same logic as push_demand_plan.py)
# ══════════════════════════════════════════════════════════════════════════════

def _nike_sport_for_filter(gsf, con, sb):
    """UIF filter sport mapping (mirrors push_demand_plan.py nike_sport)."""
    con_u = (con or "").strip().upper()
    if con_u in ("KIDS", "KID"):
        return "Kids"
    if con_u not in ("MENS", "WOMENS", "MEN", "WOMEN",
                     "ADULT MALE", "ADULT FEMALE", "ADULT UNISEX"):
        return None
    sb_u = (sb or "").strip().upper()
    if "ACG" in sb_u:
        return "ACG"
    if "SKIMS" in sb_u or sb_u == "NIKESKIMS":
        return "NikeSkims"
    gsf_u = (gsf or "").strip().upper()
    if gsf_u == "STREETWEAR":
        return None
    return NIKE_SPORT_MAP.get(gsf_u) or ((gsf or "").strip() or None)


def get_opdp_filter_options(
    op_recs,
    dp_recs,
    fr_recs: list | None = None,
    uif_recs: list | None = None,
) -> dict:
    """Filter options for OPDP + Exec: OP Submit plus APO from ``dp``, ``fr``, and UIF (``u``)."""
    seasons = set()
    geos = set()
    divs = set()
    sub_brands = set()
    sports = set()
    consumers = set()
    pdd_families = set()
    pdd_sub_families = set()

    for r in op_recs or []:
        if r.get("s"):
            seasons.add(r["s"])
        if r.get("g"):
            geos.add(r["g"])
        if r.get("d"):
            divs.add(r["d"])
        if r.get("sb"):
            sub_brands.add(r["sb"])
        if r.get("msp"):
            sports.add(r["msp"])
        if r.get("con"):
            consumers.add(r["con"])
    for r in dp_recs or []:
        if r.get("s"):
            seasons.add(r["s"])
        if r.get("g"):
            geos.add(r["g"])
        if r.get("d"):
            divs.add(r["d"])
        if r.get("sb"):
            sub_brands.add(r["sb"])
        if r.get("msp"):
            sports.add(r["msp"])
        if r.get("pf"):
            pdd_families.add(r["pf"])
        if r.get("psf"):
            pdd_sub_families.add(r["psf"])
        if r.get("con"):
            consumers.add(r["con"])
    for r in fr_recs or ():
        if r.get("s"):
            seasons.add(r["s"])
        if r.get("geo"):
            geos.add(r["geo"])
        if r.get("dv"):
            divs.add(r["dv"])
        if r.get("sb"):
            sub_brands.add(r["sb"])
        if r.get("sp"):
            sports.add(r["sp"])
        if r.get("con"):
            consumers.add(r["con"])
        if r.get("pf"):
            pdd_families.add(r["pf"])
        if r.get("psf"):
            pdd_sub_families.add(r["psf"])
    for r in uif_recs or ():
        if r.get("s"):
            seasons.add(r["s"])
        if r.get("geo"):
            geos.add(r["geo"])
        dfull = (r.get("div") or "").strip()
        if dfull in _DIV_DESC_TO_PE:
            divs.add(_DIV_DESC_TO_PE[dfull])
        b = (r.get("b") or "").strip()
        if b == "Nike":
            sp = _nike_sport_for_filter(
                (r.get("gsf") or ""), (r.get("con") or ""), (r.get("sb") or "")
            )
            if sp:
                sports.add(sp)
        elif b == "Jordan":
            jsp = _map_sport_jordan(r.get("gsf") or "", r.get("con") or "")
            if jsp:
                sports.add(jsp)
        if r.get("sb"):
            sub_brands.add(r["sb"])
        nc = _normalize_consumer(r.get("con") or "")
        if nc:
            consumers.add(nc)
        if r.get("pf"):
            pdd_families.add(r["pf"])
        if r.get("psf"):
            pdd_sub_families.add(r["psf"])

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


def get_dpfr_filter_options(recs):
    seasons = set(); geos = set(); divs = set(); brands = set()
    consumers = set(); sports = set(); sub_brands = set(); bnbs = set()
    pdd_families = set(); pdd_sub_families = set(); franchises = set()
    for r in recs:
        if r.get("s"):   seasons.add(r["s"])
        if r.get("geo"): geos.add(r["geo"])
        if r.get("dv"):  divs.add(r["dv"])
        if r.get("b"):   brands.add(r["b"])
        if r.get("con"): consumers.add(r["con"])
        if r.get("sp"):  sports.add(r["sp"])
        if r.get("sb"):  sub_brands.add(r["sb"])
        if r.get("pf"):  pdd_families.add(r["pf"])
        if r.get("psf"): pdd_sub_families.add(r["psf"])
        if r.get("bnb"): bnbs.add(r["bnb"])
        if r.get("fr"):  franchises.add(r["fr"])
    fbat_set = {"American Football", "Global Football", "Baseball", "Training"}
    if sports & fbat_set:
        sports.add("FBAT")
    return {
        "seasons":          sorted(seasons, key=_season_sort_key),
        "geos":             sorted(geos),
        "divs":             pe_labels_in_data(divs),
        "brands":           sorted(brands),
        "sub_brands":       sorted(sub_brands),
        "consumers":        sorted(consumers),
        "sports":           sorted(sports),
        "pdd_families":     sorted(pdd_families),
        "pdd_sub_families": sorted(pdd_sub_families),
        "bnbs":             sorted(bnbs),
        "franchises":       sorted(franchises),
    }


def get_dpfr_filter_options_merged(fr_recs, dp_recs) -> dict:
    """Franchise tab: filter choices from APO ``d.fr`` plus DP grain so season/geo/PE match data."""
    fo = get_dpfr_filter_options(fr_recs or [])
    if not dp_recs:
        return fo
    seasons     = {s for s in fo["seasons"]}
    geos        = {g for g in fo["geos"]}
    divs        = {d for d in fo["divs"]}
    brands      = {b for b in fo["brands"]}
    sub_brands  = {x for x in fo["sub_brands"]}
    consumers   = {c for c in fo["consumers"]}
    sports      = {x for x in fo["sports"]}
    pdd_fa      = {x for x in fo["pdd_families"]}
    pdd_sfa     = {x for x in fo["pdd_sub_families"]}
    for r in dp_recs:
        if r.get("s"):
            seasons.add(r["s"])
        if r.get("g"):
            geos.add(r["g"])
        if r.get("d"):
            divs.add(r["d"])
        if r.get("b"):
            brands.add(r["b"])
        if r.get("sb"):
            sub_brands.add(r["sb"])
        if r.get("con"):
            consumers.add(r["con"])
        if r.get("msp"):
            sports.add(r["msp"])
        if r.get("pf"):
            pdd_fa.add(r["pf"])
        if r.get("psf"):
            pdd_sfa.add(r["psf"])
    fbat_set = {"American Football", "Global Football", "Baseball", "Training"}
    if sports & fbat_set:
        sports.add("FBAT")
    return {
        "seasons":          sorted(seasons, key=_season_sort_key),
        "geos":             sorted(geos),
        "divs":             pe_labels_in_data(divs),
        "brands":           sorted(brands),
        "sub_brands":       sorted(sub_brands),
        "consumers":        sorted(consumers),
        "sports":           sorted(sports),
        "pdd_families":     sorted(pdd_fa),
        "pdd_sub_families": sorted(pdd_sfa),
        "bnbs":             list(fo["bnbs"]),
        "franchises":       list(fo["franchises"]),
    }


def merge_sro_with_oo(so_sro: dict, oo: dict) -> dict:
    """SRO filter lists: OP (SRO rows) ∪ APO dimensions in ``oo`` so SRO + APO share one vocabulary."""
    sp_order = {s: i for i, s in enumerate(MAPPED_SPORT_ORDER)}
    out = dict(so_sro)
    for k in ("geos", "sub_brands"):
        a = set(out.get(k, []) or []) | set(oo.get(k, []) or [])
        out[k] = sorted(a)
    out["seasons"] = sorted(
        set(out.get("seasons", []) or []) | set(oo.get("seasons", []) or []),
        key=_season_sort_key,
        reverse=True,
    )
    out["divs"] = pe_labels_in_data(
        set(out.get("divs", []) or []) | set(oo.get("divs", []) or []),
    )
    out["sports"] = sorted(
        set(out.get("sports", []) or []) | set(oo.get("sports", []) or []),
        key=lambda x: sp_order.get(x, 99),
    )
    return out


# ══════════════════════════════════════════════════════════════════════════════
# OP Submit Excel loading (reuse from push_demand_plan.py)
# ══════════════════════════════════════════════════════════════════════════════

def load_op_data(path: Path):
    """Read OP Submit Excel → (op_records, sro_records)."""
    sys.path.insert(0, str(Path(__file__).parent))
    from push_demand_plan import load_op_data as _load_op
    return _load_op(path)


def default_season_from_op_dp(
    op_records: list,
    dp_rows: list,
    *,
    min_calendar_year: int = 0,
) -> str | None:
    """Newest APO/DP (``dp``) season; else OP-only. Optionally restrict to ``min_calendar_year``+.

    If ``min_calendar_year`` is set, prefer seasons at or above that year; if
    that leaves no codes, fall back to the full sets (avoids an empty preselect).
    """
    op_codes = {r.get("s") for r in op_records if r.get("s")}
    dp_codes = {r.get("s") for r in dp_rows if r.get("s")}
    if not op_codes and not dp_codes:
        return None
    sord = {"FA": 0, "HO": 1, "SP": 2, "SU": 3}

    def _key(s: str):
        s = (s or "").strip()
        if len(s) < 3:
            return (0, 99)
        pre = s[:2].upper()
        try:
            y = int(s[2:])
        except ValueError:
            y = 0
        return (y, sord.get(pre, 99))

    def _yflt(st: set) -> set:
        if min_calendar_year <= 0:
            return st
        return {x for x in st if _business_season_year(x) >= min_calendar_year}

    dp_f, op_f = _yflt(dp_codes), _yflt(op_codes)
    if dp_f:
        return max(dp_f, key=_key)
    if op_f:
        return max(op_f, key=_key)
    if dp_codes:
        return max(dp_codes, key=_key)
    return max(op_codes, key=_key) if op_codes else None


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Refresh dashboard from Snowflake + OP Submit Excel")
    p.add_argument("--op-submit-xlsx", type=Path,
                   default=_DEFAULT_DIR / "OP Submit.xlsx")
    p.add_argument("--dbfs-path", default=DBFS_PATH)
    p.add_argument("--dry-run", action="store_true",
                   help="Build blob but skip DBFS upload")
    return p.parse_args()


def main():
    import time
    args = parse_args()
    t0 = time.time()

    op_submit_xlsx = args.op_submit_xlsx.resolve()
    dbfs_path      = args.dbfs_path

    if not op_submit_xlsx.exists():
        logger.error("OP Submit file not found: %s", op_submit_xlsx)
        sys.exit(1)

    logger.info("=== refresh_from_snowflake.py ===")
    logger.info("  OP Submit : %s", op_submit_xlsx)
    logger.info("  DBFS path : %s", dbfs_path)

    # ── Step 1: Query Snowflake ──
    logger.info("Querying Snowflake APO data...")
    from snowflake_client import query_dataframe
    logger.info("  Season scope: year >= %s (BUSINESS_SEASON_CD)", MIN_BUSINESS_SEASON_CALENDAR_YEAR)
    sql = _build_snowflake_sql()
    sf_df = query_dataframe(sql)
    logger.info("  Snowflake returned %d aggregated rows", len(sf_df))

    # ── Step 2: Process Snowflake data ──
    uif_rows, dp_rows, fr_rows = load_snowflake_data(sf_df)

    # ── Step 3: Default season from latest F1 submit timestamp (second Snowflake query) ──
    f1_season, f1_col = _try_default_season_f1_max_submit()

    # ── Step 4: Load OP Submit Excel ──
    op_records, sro_records = load_op_data(op_submit_xlsx)

    # ── Step 5: Build merged blob ──
    min_y = int(MIN_BUSINESS_SEASON_CALENDAR_YEAR)
    ds = f1_season or default_season_from_op_dp(
        op_records, dp_rows, min_calendar_year=min_y
    )
    if f1_season:
        dsrc = f"f1_max_timestamp:{f1_col}"
    else:
        dsrc = "op_dp_max_fallback"
    oo = get_opdp_filter_options(op_records, dp_rows, fr_rows, uif_rows)
    so = merge_sro_with_oo(get_sro_filter_options(sro_records), oo)
    blob = {
        "m": {
            "ts":                 datetime.now(timezone.utc).isoformat(),
            "gates":              GATE_ORDER,
            "src":                "snowflake+excel",
            "op_submit_path":     str(op_submit_xlsx),
            "sf_table":           SF_TABLE,
            "min_season_year":    min_y,
            "default_season":     ds,
            "default_season_rule": dsrc,
        },
        "u":   uif_rows,
        "uo":  get_uif_filter_options(uif_rows),
        "op":  op_records,
        "dp":  dp_rows,
        "oo":  oo,
        "sro": sro_records,
        "so":  so,
        "fr":  fr_rows,
        "fo":  get_dpfr_filter_options_merged(fr_rows, dp_rows),
    }

    blob_json = json.dumps(blob, separators=(",", ":"))
    size_mb   = len(blob_json) / (1024 * 1024)
    logger.info("Blob size : %.2f MB", size_mb)

    if args.dry_run:
        logger.info("Dry run — skipping DBFS upload")
        # Write to local file for inspection
        local_path = Path(__file__).parent / "temp_blob_sf.json"
        local_path.write_text(blob_json, encoding="utf-8")
        logger.info("  Written to %s", local_path)
    else:
        # ── Step 5: Upload to DBFS ──
        logger.info("Uploading to DBFS …")
        dbfs_dir = dbfs_path.rsplit("/", 1)[0]
        subprocess.run(
            ["databricks", "fs", "mkdirs", dbfs_dir, "--profile", DATABRICKS_PROFILE],
            capture_output=True, timeout=30,
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(blob_json)
            tmp_path = tmp.name
        try:
            r = subprocess.run(
                ["databricks", "fs", "cp", tmp_path, dbfs_path,
                 "--overwrite", "--profile", DATABRICKS_PROFILE],
                capture_output=True, text=True, timeout=300,
            )
            if r.returncode != 0:
                logger.error("Upload failed:\n%s", r.stderr)
                sys.exit(1)
        finally:
            os.unlink(tmp_path)

    elapsed = time.time() - t0
    logger.info(
        "\n Done in %.0fs\n"
        "  UIF rows  : %d\n"
        "  OP rows   : %d\n"
        "  DP rows   : %d\n"
        "  SRO rows  : %d\n"
        "  FR rows   : %d\n"
        "  Blob size : %.2f MB",
        elapsed, len(uif_rows), len(op_records), len(dp_rows),
        len(sro_records), len(fr_rows), size_mb,
    )


if __name__ == "__main__":
    main()
