from __future__ import annotations

import argparse
import math
import re
from functools import lru_cache
from pathlib import Path

import pandas as pd


INPUT_COLUMNS = [
    "ID",
    "LICENSE ID",
    "ACCOUNT NUMBER",
    "SITE NUMBER",
    "DOING BUSINESS AS NAME",
    "LEGAL NAME",
    "ADDRESS",
    "CITY",
    "STATE",
    "ZIP CODE",
    "COMMUNITY AREA NAME",
    "NEIGHBORHOOD",
    "LICENSE DESCRIPTION",
    "BUSINESS ACTIVITY",
    "APPLICATION TYPE",
    "LICENSE TERM START DATE",
    "LICENSE TERM EXPIRATION DATE",
    "DATE ISSUED",
    "LICENSE STATUS",
    "LICENSE STATUS CHANGE DATE",
    "LATITUDE",
    "LONGITUDE",
]

AGGREGATION_COLUMN = "COMMUNITY AREA NAME"
#AGGREGATION_COLUMN = "ZIP CODE"


CATEGORY_RULES = [
    {
        "category": "fresh_food_and_daily_needs",
        "weight": 3,
        "keywords": [
            "retail sales of perishable foods",
            "sale of food prepared onsite",
            "retail sales of general merchandise and non-perishable food",
            "retail sales of general merchandise",
            "bakery",
            "grocery",
            "food mart",
            "produce",
            "deli",
            "pharmacy",
            "drug",
            "laund",
            "dry cleaning",
        ],
        "license_descriptions": [
            "Retail Food Establishment",
            "Package Goods",
            "Tobacco",
        ],
    },
    {
        "category": "restaurants_and_social_life",
        "weight": 3,
        "keywords": [
            "preparation of food and dining on premises with seating",
            "sale of food prepared onsite with dining area",
            "consumption of liquor on premises",
            "tavern",
            "sale of liquor outdoors on private property",
            "late hour",
            "catering of liquor",
            "special event beer",
            "special event liquor",
            "coffee",
            "cafe",
            "restaurant",
            "bar",
            "pub",
        ],
        "license_descriptions": [
            "Consumption on Premises - Incidental Activity",
            "Tavern",
            "Outdoor Patio",
            "Late Hour",
            "Caterer's Liquor License",
            "Special Event Liquor",
        ],
    },
    {
        "category": "community_and_entertainment",
        "weight": 2,
        "keywords": [
            "provides onsite amusement",
            "provides onsite entertainment",
            "charges a fee for entertainment",
            "museum",
            "gallery",
            "theater",
            "music",
            "dance",
            "raffles",
            "not-for-profit",
            "club",
            "shared kitchen user",
        ],
        "license_descriptions": [
            "Public Place of Amusement",
            "Raffles",
            "Not-For-Profit Club",
            "Shared Kitchen User (Long Term)",
            "Music and Dance",
            "Hotel",
        ],
    },
    {
        "category": "personal_and_health_services",
        "weight": 2,
        "keywords": [
            "hair services",
            "nail services",
            "massage",
            "health club",
            "fitness",
            "yoga",
            "pilates",
            "day care",
            "children",
            "home health care services",
            "animal care",
            "medical",
            "dental",
            "optical",
        ],
        "license_descriptions": [
            "Children's Services Facility License",
            "Massage Establishment",
            "Massage Therapist",
            "Animal Care License",
            "Day Care Center 2 - 6 Years",
        ],
    },
    {
        "category": "local_retail_and_commercial_services",
        "weight": 1,
        "keywords": [
            "retail sales of clothing",
            "retail sales of jewelry",
            "retail sales of cell phones",
            "retail sales of tobacco products",
            "miscellaneous commercial services",
            "tax preparation",
            "financial services commercial office",
            "administrative commercial office",
            "debt collecting",
            "barber",
            "beauty",
        ],
        "license_descriptions": [
            "Broker",
            "Pop-Up Retail User",
            "Secondhand Dealer",
        ],
    },
    {
        "category": "home_based_or_non_storefront",
        "weight": 0,
        "keywords": [
            "home based business",
            "home occupation",
            "online sales",
            "administrative commercial office",
            "pharmaceutical representative",
        ],
        "license_descriptions": [
            "Home Occupation",
            "Pharmaceutical Representative",
            "Residential Real Estate Developer",
        ],
    },
    {
        "category": "auto_oriented",
        "weight": -3,
        "keywords": [
            "operation of a fuel filling station",
            "motor vehicle repair",
            "hand car wash",
            "parking spaces for a fee",
            "valet parking",
            "tire",
            "tow",
            "commercial garage",
        ],
        "license_descriptions": [
            "Filling Station",
            "Motor Vehicle Services License",
            "Motor Vehicle Repair : Engine Only (Class II)",
            "Motor Vehicle Repair: Engine/Body(Class III)",
            "Commercial Garage",
            "Valet Parking Operator",
            "Tire Facilty Class I (100 - 1,000 Tires)",
        ],
    },
    {
        "category": "industrial_or_logistics",
        "weight": -2,
        "keywords": [
            "manufacturing",
            "hazardous materials",
            "wholesale food",
            "wholesale sales",
            "scavenger",
            "storage",
            "distribution",
            "warehouse",
        ],
        "license_descriptions": [
            "Manufacturing Establishments",
            "Hazardous Materials",
            "Wholesale Food Establishment",
            "Scavenger, Private",
        ],
    },
]


SECONDARY_CATEGORY_RULES = [
    {
        "category": "fresh_food_and_daily_needs",
        "weight": 3,
        "keywords": [
            "bakery",
            "deli",
            "farmacia",
            "food",
            "foods",
            "grocery",
            "grocer",
            "laundry",
            "market",
            "mart",
            "meat",
            "mini mart",
            "pantry",
            "pharmacy",
            "produce",
            "supermarket",
            "supermercado",
            "walgreen",
            "walgreens",
        ],
    },
    {
        "category": "restaurants_and_social_life",
        "weight": 3,
        "keywords": [
            "cafe",
            "coffee",
            "diner",
            "eatery",
            "grill",
            "lounge",
            "pizza",
            "pub",
            "restaurant",
            "taqueria",
            "tavern",
            "liquor",
        ],
    },
    {
        "category": "community_and_entertainment",
        "weight": 2,
        "keywords": [
            "academy",
            "arts",
            "center",
            "centre",
            "club",
            "dance",
            "discoteca",
            "gallery",
            "hotel",
            "museum",
            "music",
            "motel",
            "perform",
            "recordings",
            "studio",
            "theater",
            "theatre",
        ],
    },
    {
        "category": "personal_and_health_services",
        "weight": 2,
        "keywords": [
            "animal",
            "barber",
            "braiding",
            "clinic",
            "cosmetics",
            "dental",
            "fitness",
            "hair",
            "health",
            "home care",
            "massage",
            "medical",
            "multimedia",
            "nail",
            "optical",
            "pest control",
            "pet",
            "salon",
            "spa",
            "vision",
            "yoga",
        ],
    },
    {
        "category": "local_retail_and_commercial_services",
        "weight": 1,
        "keywords": [
            "bike shop",
            "boutique",
            "bridal",
            "cleaners",
            "clothing",
            "closet",
            "discount",
            "dollar",
            "fashion",
            "furniture",
            "gift",
            "gifts",
            "hardware",
            "jewel",
            "jeweler",
            "jewelers",
            "jewelry",
            "locksmith",
            "printing",
            "remodel",
            "shoe",
            "supply",
            "stationers",
            "tours",
            "travel",
            "travel bureau",
            "upholster",
            "watch",
        ],
    },
    {
        "category": "home_based_or_non_storefront",
        "weight": 0,
        "keywords": [
            "asset management",
            "capital",
            "consult",
            "consulting",
            "credit",
            "employment",
            "group",
            "holding",
            "management",
            "marketing",
            "partners",
            "recordings",
            "staffing",
            "strategies",
            "trading",
        ],
    },
    {
        "category": "auto_oriented",
        "weight": -3,
        "keywords": [
            "auto",
            "automotive",
            "car wash",
            "mechanical",
            "motor",
            "oil",
            "parking",
            "petroleum",
            "tire",
            "tow",
        ],
    },
    {
        "category": "industrial_or_logistics",
        "weight": -2,
        "keywords": [
            "casting",
            "disposal",
            "distribution",
            "gases",
            "industrial",
            "laborator",
            "logistics",
            "manufactur",
            "nutrition",
            "supply chain",
            "trailer",
            "warehouse",
            "wholesale",
        ],
    },
]


INACTIVE_STATUSES = {"AAC", "REV", "REA", "INQ"}
CHICAGO_ZIP_PATTERN = re.compile(r"^(606\d{2}|60666|60707|60827)$")
YEAR_MIN = 2002
YEAR_MAX = 2025
DEFAULT_INPUT_CSV = Path(r"C:\Users\johnh\Desktop\Work\Datasets\Raw\Business_Licenses.csv")
COMMUNITY_AREA_INFO_CSV = Path("BusinessLicenses/community areas info.csv")
# 2025 Census Gazetteer land area converted to square kilometers for Chicago ZCTAs where available.
# Non-geographic/special-use ZIPs that are not assigned a land polygon are kept as None.
# 60666 (O'Hare) uses a converted fallback area because Census omits a ZCTA for it.
ZIP_LAND_AREA_SQKM: dict[str, float | None] = {
    "60601": 1.007,
    "60602": 0.212,
    "60603": 0.308,
    "60604": 0.199,
    "60605": 3.704,
    "60606": 0.601,
    "60607": 5.99,
    "60608": 16.18,
    "60609": 20.072,
    "60610": 2.88,
    "60611": 2.054,
    "60612": 9.632,
    "60613": 6.03,
    "60614": 8.345,
    "60615": 5.669,
    "60616": 9.842,
    "60617": 35.413,
    "60618": 12.934,
    "60619": 15.859,
    "60620": 18.122,
    "60621": 9.722,
    "60622": 6.475,
    "60623": 13.926,
    "60624": 9.186,
    "60625": 9.746,
    "60626": 4.444,
    "60628": 28.433,
    "60629": 18.255,
    "60630": 12.279,
    "60631": 9.702,
    "60632": 19.293,
    "60633": 25.247,
    "60634": 18.411,
    "60635": None,
    "60636": 10.133,
    "60637": 11.727,
    "60638": 28.65,
    "60639": 12.851,
    "60640": 6.115,
    "60641": 10.43,
    "60642": 4.32,
    "60643": 19.052,
    "60644": 9.093,
    "60645": 5.765,
    "60646": 11.494,
    "60647": 10.264,
    "60649": 9.376,
    "60650": None,
    "60651": 8.969,
    "60652": 12.53,
    "60653": 6.112,
    "60654": 1.404,
    "60655": 11.416,
    "60656": 8.384,
    "60657": 5.633,
    "60659": 6.112,
    "60660": 3.299,
    "60661": 0.793,
    "60666": 18.13,
    "60670": None,
    "60707": 9.267,
    "60827": 18.2,
}


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_zip(value: object) -> str | None:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 5:
        zip_code = digits[:5]
        if CHICAGO_ZIP_PATTERN.match(zip_code):
            return zip_code
    return None


def parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def log_progress(message: str) -> None:
    print(f"[progress] {message}", flush=True)


def get_aggregation_slug() -> str:
    return re.sub(r"[^a-z0-9]+", "_", AGGREGATION_COLUMN.strip().lower()).strip("_")


def get_zip_land_area_sqkm(zip_code: object) -> float | None:
    if pd.isna(zip_code):
        return None
    return ZIP_LAND_AREA_SQKM.get(str(zip_code))


@lru_cache(maxsize=1)
def get_community_area_land_area_sqkm_lookup() -> dict[str, float]:
    community_area_info = pd.read_csv(COMMUNITY_AREA_INFO_CSV)
    area_lookup = (
        community_area_info.assign(
            community_area_key=community_area_info["community area name"].astype(str).str.strip().str.upper(),
            land_area_sqkm=pd.to_numeric(community_area_info["Area-km2"], errors="coerce"),
        )
        .dropna(subset=["community_area_key", "land_area_sqkm"])
        .drop_duplicates(subset=["community_area_key"])
    )
    return dict(zip(area_lookup["community_area_key"], area_lookup["land_area_sqkm"]))


def get_community_area_land_area_sqkm(community_area: object) -> float | None:
    if pd.isna(community_area):
        return None
    return get_community_area_land_area_sqkm_lookup().get(str(community_area).strip().upper())


def get_land_area_sqkm(aggregation_value: object) -> float | None:
    if AGGREGATION_COLUMN == "ZIP CODE":
        return get_zip_land_area_sqkm(aggregation_value)
    if AGGREGATION_COLUMN == "COMMUNITY AREA NAME":
        return get_community_area_land_area_sqkm(aggregation_value)
    return None


def calculate_density(value: float | int, land_area_sqkm: float | None) -> float | None:
    if land_area_sqkm is None or land_area_sqkm <= 0:
        return None
    return round(float(value) / land_area_sqkm, 3)


def get_biennium_start(year: int) -> int:
    return YEAR_MIN + (2 * ((int(year) - YEAR_MIN) // 2))


def classify_license(
    license_description: object,
    business_activity: object,
    dba_name: object,
    legal_name: object,
) -> tuple[str, int, str]:
    license_text = normalize_text(license_description)
    activity_text = normalize_text(business_activity)
    dba_text = normalize_text(dba_name)
    legal_text = normalize_text(legal_name)
    primary_combined = " | ".join(part for part in [activity_text, license_text, dba_text] if part)

    for rule in CATEGORY_RULES:
        if any(keyword in primary_combined for keyword in rule["keywords"]):
            return rule["category"], int(rule["weight"]), "primary"
        if any(desc.lower() == license_text for desc in rule["license_descriptions"]):
            return rule["category"], int(rule["weight"]), "primary"

    secondary_combined = " | ".join(part for part in [dba_text, legal_text] if part)
    for rule in SECONDARY_CATEGORY_RULES:
        if any(keyword in secondary_combined for keyword in rule["keywords"]):
            return rule["category"], int(rule["weight"]), "secondary_name_keywords"

    return "uncategorized", 0, "unclassified"


def build_keyword_mask(text: pd.Series, keywords: list[str]) -> pd.Series:
    mask = pd.Series(False, index=text.index)
    for keyword in keywords:
        mask = mask | text.str.contains(re.escape(keyword), na=False, regex=True)
    return mask


def classify_licenses_vectorized(frame: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Starting license classification for {len(frame):,} rows.")
    license_text = frame["LICENSE DESCRIPTION"].fillna("").astype(str).str.lower().str.strip()
    activity_text = frame["BUSINESS ACTIVITY"].fillna("").astype(str).str.lower().str.strip()
    dba_text = frame["DOING BUSINESS AS NAME"].fillna("").astype(str).str.lower().str.strip()
    legal_text = frame["LEGAL NAME"].fillna("").astype(str).str.lower().str.strip()
    primary_combined = activity_text + " | " + license_text + " | " + dba_text
    secondary_combined = dba_text + " | " + legal_text

    categories = pd.Series("uncategorized", index=frame.index, dtype="string")
    weights = pd.Series(0, index=frame.index, dtype="int64")
    workflows = pd.Series("unclassified", index=frame.index, dtype="string")

    for index, rule in enumerate(CATEGORY_RULES, start=1):
        log_progress(f"Primary classification rule {index}/{len(CATEGORY_RULES)}: {rule['category']}")
        keyword_mask = build_keyword_mask(primary_combined, rule["keywords"])
        desc_mask = pd.Series(False, index=frame.index)
        if rule["license_descriptions"]:
            desc_mask = license_text.isin([item.lower() for item in rule["license_descriptions"]])

        mask = (categories == "uncategorized") & (keyword_mask | desc_mask)
        categories.loc[mask] = rule["category"]
        weights.loc[mask] = int(rule["weight"])
        workflows.loc[mask] = "primary"

    log_progress("Starting secondary name-based classification for remaining uncategorized rows.")
    for index, rule in enumerate(SECONDARY_CATEGORY_RULES, start=1):
        log_progress(f"Secondary classification rule {index}/{len(SECONDARY_CATEGORY_RULES)}: {rule['category']}")
        keyword_mask = build_keyword_mask(secondary_combined, rule["keywords"])
        mask = (categories == "uncategorized") & keyword_mask
        categories.loc[mask] = rule["category"]
        weights.loc[mask] = int(rule["weight"])
        workflows.loc[mask] = "secondary_name_keywords"

    log_progress("Finished license classification.")
    return pd.DataFrame(
        {
            "walkability_category": categories,
            "walkability_weight": weights,
            "walkability_classification_workflow": workflows,
        }
    )


def build_scored_license_terms(df: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Preparing raw license records: {len(df):,} rows loaded.")
    frame = df.copy()
    if AGGREGATION_COLUMN not in frame.columns:
        raise KeyError(f"AGGREGATION_COLUMN '{AGGREGATION_COLUMN}' was not found in the input data.")
    frame["CITY"] = frame["CITY"].astype("string")
    frame["STATE"] = frame["STATE"].astype("string")
    frame["ZIP CODE"] = frame["ZIP CODE"].map(normalize_zip)
    frame["DATE ISSUED"] = parse_date(frame["DATE ISSUED"])
    frame["LICENSE TERM START DATE"] = parse_date(frame["LICENSE TERM START DATE"])
    frame["LICENSE TERM EXPIRATION DATE"] = parse_date(frame["LICENSE TERM EXPIRATION DATE"])
    frame["LICENSE STATUS CHANGE DATE"] = parse_date(frame["LICENSE STATUS CHANGE DATE"])
    frame["LICENSE STATUS"] = frame["LICENSE STATUS"].astype("string")

    frame = frame[
        (frame["CITY"].str.upper() == "CHICAGO")
        & (frame["STATE"].str.upper() == "IL")
        & frame["ZIP CODE"].notna()
    ].copy()
    log_progress(f"Filtered to Chicago licenses with valid Chicago geography fields: {len(frame):,} rows remain.")
    frame = frame[frame[AGGREGATION_COLUMN].notna()].copy()
    log_progress(f"Filtered to rows with non-null {AGGREGATION_COLUMN}: {len(frame):,} rows remain.")

    classified = classify_licenses_vectorized(frame)
    frame["walkability_category"] = classified["walkability_category"]
    frame["walkability_weight"] = classified["walkability_weight"]
    frame["walkability_classification_workflow"] = classified["walkability_classification_workflow"]

    frame["term_start"] = frame["LICENSE TERM START DATE"].fillna(frame["DATE ISSUED"])
    frame["term_end"] = frame["LICENSE TERM EXPIRATION DATE"]
    frame.loc[frame["term_end"] > pd.Timestamp("2100-12-31"), "term_end"] = pd.NaT
    frame.loc[frame["LICENSE STATUS CHANGE DATE"] > pd.Timestamp("2100-12-31"), "LICENSE STATUS CHANGE DATE"] = pd.NaT

    early_end_mask = (
        frame["LICENSE STATUS"].isin(INACTIVE_STATUSES)
        & frame["LICENSE STATUS CHANGE DATE"].notna()
        & (
            frame["term_end"].isna()
            | (frame["LICENSE STATUS CHANGE DATE"] < frame["term_end"])
        )
    )
    frame.loc[early_end_mask, "term_end"] = frame.loc[early_end_mask, "LICENSE STATUS CHANGE DATE"]

    frame = frame[frame["term_start"].notna()].copy()
    frame = frame[(frame["term_end"].isna()) | (frame["term_end"] >= frame["term_start"])].copy()
    log_progress(f"Built valid license terms: {len(frame):,} rows ready for yearly expansion.")

    frame["year_issued"] = frame["term_start"].dt.year.astype("Int64")
    frame["year_ended"] = frame["term_end"].dt.year.astype("Int64")
    return frame


def expand_active_license_years(scored_licenses: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Expanding active years for {len(scored_licenses):,} license terms.")
    max_start_year = int(scored_licenses["term_start"].dt.year.max())
    max_end_year = int(scored_licenses["term_end"].dropna().dt.year.max()) if scored_licenses["term_end"].notna().any() else max_start_year
    max_year = max(max_start_year, max_end_year)

    active_license_years = scored_licenses.copy()
    active_license_years["start_year"] = active_license_years["term_start"].dt.year.astype(int)
    effective_end = active_license_years["term_end"].fillna(pd.Timestamp(year=max_year + 1, month=1, day=1))
    active_license_years["last_active_year"] = (effective_end - pd.Timedelta(days=1)).dt.year.astype(int)
    active_license_years = active_license_years[
        active_license_years["last_active_year"] >= active_license_years["start_year"]
    ].copy()
    active_license_years["active_years"] = active_license_years.apply(
        lambda row: list(range(int(row["start_year"]), int(row["last_active_year"]) + 1)),
        axis=1,
    )
    active_license_years = active_license_years.explode("active_years").rename(columns={"active_years": "year"})
    active_license_years["year"] = active_license_years["year"].astype(int)
    log_progress(f"Expanded to {len(active_license_years):,} active license-year rows.")
    return active_license_years


def summarize_aggregation_periods(scored_licenses: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Summarizing {AGGREGATION_COLUMN} walkability scores in 2-year periods.")
    active_license_years = expand_active_license_years(scored_licenses)
    active_license_years = active_license_years[
        (active_license_years["year"] >= YEAR_MIN) & (active_license_years["year"] <= YEAR_MAX)
    ].copy()
    active_license_years["period_start_year"] = active_license_years["year"].map(get_biennium_start)
    rows: list[dict[str, object]] = []
    grouped = active_license_years.groupby([AGGREGATION_COLUMN, "period_start_year"], dropna=False)
    log_progress(f"Computing score metrics for {grouped.ngroups:,} {AGGREGATION_COLUMN}/2-year groups.")

    for (aggregation_value, period_start_year), group in grouped:
            aggregation_value = str(aggregation_value)
            active_license_count = round(group.groupby("year")["LICENSE ID"].nunique().mean(), 3)
            positive = group[group["walkability_weight"] > 0]
            negative = group[group["walkability_weight"] < 0]

            positive_weight = round(positive.groupby("year")["walkability_weight"].sum().mean(), 3)
            if positive.empty:
                positive_weight = 0.0
            negative_weight = round(((-negative["walkability_weight"]).groupby(negative["year"]).sum()).mean(), 3)
            if negative.empty:
                negative_weight = 0.0
            net_weight = round(positive_weight - negative_weight, 3)

            positive_storefronts = round(positive.groupby("year")["LICENSE ID"].nunique().mean(), 3)
            if positive.empty:
                positive_storefronts = 0.0
            land_area_sqkm = get_land_area_sqkm(aggregation_value)
            active_license_density = calculate_density(active_license_count, land_area_sqkm)
            positive_storefront_density = calculate_density(positive_storefronts, land_area_sqkm)
            positive_weight_density = calculate_density(positive_weight, land_area_sqkm)
            negative_weight_density = calculate_density(negative_weight, land_area_sqkm)
            net_weight_density = calculate_density(net_weight, land_area_sqkm)

            total_directional_weight = positive_weight + negative_weight
            mix_balance = positive_weight / total_directional_weight if total_directional_weight else 0.5
            destination_intensity = (
                min(math.log1p(positive_storefront_density) / math.log(26), 1.0)
                if positive_storefront_density is not None
                else 0.0
            )

            walkability_score = round(
                (mix_balance * 60) + (destination_intensity * 40),
                1,
            )

            rows.append(
                {
                    "aggregation_unit": aggregation_value,
                    "year": int(period_start_year),
                    "period_start_year": int(period_start_year),
                    "land_area_sqkm": land_area_sqkm,
                    "active_license_count": active_license_count,
                    "active_license_density_per_sqkm": active_license_density,
                    "positive_storefront_count": positive_storefronts,
                    "positive_storefront_density_per_sqkm": positive_storefront_density,
                    "positive_weight": positive_weight,
                    "positive_weight_density_per_sqkm": positive_weight_density,
                    "negative_weight": negative_weight,
                    "negative_weight_density_per_sqkm": negative_weight_density,
                    "net_weight": net_weight,
                    "net_weight_density_per_sqkm": net_weight_density,
                    "mix_balance": round(mix_balance, 3),
                    "destination_intensity": round(destination_intensity, 3),
                    "walkability_score": walkability_score,
                }
            )

    log_progress(f"Finished {AGGREGATION_COLUMN} 2-year walkability score summary.")
    return pd.DataFrame(rows).sort_values(["aggregation_unit", "period_start_year"]).reset_index(drop=True)


def build_aggregation_summary_wide(scored_licenses: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Pivoting {AGGREGATION_COLUMN}/2-year scores into wide format.")
    period_scores = summarize_aggregation_periods(scored_licenses)
    period_scores = period_scores.copy()
    period_scores["aggregation_unit"] = period_scores["aggregation_unit"].astype(str)
    metric_columns = [
        "land_area_sqkm",
        "active_license_count",
        "active_license_density_per_sqkm",
        "positive_storefront_count",
        "positive_storefront_density_per_sqkm",
        "positive_weight",
        "positive_weight_density_per_sqkm",
        "negative_weight",
        "negative_weight_density_per_sqkm",
        "net_weight",
        "net_weight_density_per_sqkm",
        "mix_balance",
        "destination_intensity",
        "walkability_score",
    ]

    aggregation_units = pd.DataFrame(
        {"aggregation_unit": sorted(period_scores["aggregation_unit"].dropna().astype(str).unique())}
    )

    aggregation_summary = aggregation_units.copy()
    aggregation_summary["land_area_sqkm"] = aggregation_summary["aggregation_unit"].map(get_land_area_sqkm)
    aggregation_summary["land_area_sqkm"] = pd.to_numeric(aggregation_summary["land_area_sqkm"], errors="coerce")
    period_starts = list(range(YEAR_MIN, YEAR_MAX + 1, 2))
    for metric in metric_columns:
        if metric == "land_area_sqkm":
            continue
        pivoted = (
            period_scores.pivot(index="aggregation_unit", columns="period_start_year", values=metric)
            .reindex(columns=period_starts)
            .reset_index()
        )
        pivoted.columns = ["aggregation_unit"] + [f"{metric}_{year}" for year in period_starts]
        aggregation_summary = aggregation_summary.merge(pivoted, on="aggregation_unit", how="left")

    density_like = [col for col in aggregation_summary.columns if "density_per_sqkm_" in col]
    known_area_mask = aggregation_summary["land_area_sqkm"].notna()
    if density_like:
        aggregation_summary.loc[known_area_mask, density_like] = (
            aggregation_summary.loc[known_area_mask, density_like].fillna(0.0)
        )
        aggregation_summary[density_like] = aggregation_summary[density_like].apply(pd.to_numeric, errors="coerce")

    float_like = [
        col
        for col in aggregation_summary.columns
        if col not in {"aggregation_unit", "land_area_sqkm", *density_like}
    ]
    aggregation_summary[float_like] = aggregation_summary[float_like].fillna(0.0).round(3)
    aggregation_summary["land_area_sqkm"] = aggregation_summary["land_area_sqkm"].round(3)
    if density_like:
        aggregation_summary[density_like] = aggregation_summary[density_like].round(3)
    log_progress(f"Finished wide {AGGREGATION_COLUMN} score pivot.")
    return aggregation_summary.sort_values("aggregation_unit").reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate walkability scores from Chicago business license term history."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=str(DEFAULT_INPUT_CSV),
        help=f"Path to Business_Licenses.csv. Defaults to {DEFAULT_INPUT_CSV}",
    )
    parser.add_argument(
        "--output-dir",
        default="BusinessLicenses/Output",
        help="Directory where output CSVs will be written",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    aggregation_slug = get_aggregation_slug()


    log_progress("Reading raw business license CSV.")
    raw_licenses = pd.read_csv(args.input_csv)
    scored_licenses = build_scored_license_terms(raw_licenses)
    aggregation_summary = build_aggregation_summary_wide(scored_licenses)

    print(scored_licenses.head())
    print(aggregation_summary.head())

    log_progress(f"Writing outputs to {output_dir}.")
    output_dir.mkdir(parents=True, exist_ok=True)
    scored_licenses.to_csv(output_dir / "biz_license_terms_scored.csv", index=False)
    aggregation_summary.to_csv(output_dir / f"biz_license_walkability_scores_by_{aggregation_slug}_wide.csv", index=False)
    log_progress("Pipeline complete.")

    print()
    print(f"Processed {len(raw_licenses):,} raw rows into {len(scored_licenses):,} scored Chicago license terms.")
    print(f"Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
