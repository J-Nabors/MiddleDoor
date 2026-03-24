from __future__ import annotations

import argparse
import math
import re
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
YEAR_MIN = 2000
YEAR_MAX = 2025
DEFAULT_INPUT_CSV = Path(r"C:\Users\johnh\Desktop\Work\Datasets\Raw\Business_Licenses.csv")


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


def build_license_terms(df: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Preparing raw license records: {len(df):,} rows loaded.")
    frame = df.copy()
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
    log_progress(f"Filtered to Chicago licenses with valid ZIP codes: {len(frame):,} rows remain.")

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


def expand_terms_by_active_year(frame: pd.DataFrame) -> pd.DataFrame:
    log_progress(f"Expanding active years for {len(frame):,} license terms.")
    max_start_year = int(frame["term_start"].dt.year.max())
    max_end_year = int(frame["term_end"].dropna().dt.year.max()) if frame["term_end"].notna().any() else max_start_year
    max_year = max(max_start_year, max_end_year)

    expanded = frame.copy()
    expanded["start_year"] = expanded["term_start"].dt.year.astype(int)
    effective_end = expanded["term_end"].fillna(pd.Timestamp(year=max_year + 1, month=1, day=1))
    expanded["last_active_year"] = (effective_end - pd.Timedelta(days=1)).dt.year.astype(int)
    expanded = expanded[expanded["last_active_year"] >= expanded["start_year"]].copy()
    expanded["active_years"] = expanded.apply(
        lambda row: list(range(int(row["start_year"]), int(row["last_active_year"]) + 1)),
        axis=1,
    )
    expanded = expanded.explode("active_years").rename(columns={"active_years": "year"})
    expanded["year"] = expanded["year"].astype(int)
    log_progress(f"Expanded to {len(expanded):,} active license-year rows.")
    return expanded


def summarize_zip_year_scores(frame: pd.DataFrame) -> pd.DataFrame:
    log_progress("Summarizing ZIP-by-year walkability scores.")
    positive_categories = [rule["category"] for rule in CATEGORY_RULES if int(rule["weight"]) > 0]
    expanded = expand_terms_by_active_year(frame)
    expanded = expanded[(expanded["year"] >= YEAR_MIN) & (expanded["year"] <= YEAR_MAX)].copy()
    rows: list[dict[str, object]] = []
    grouped = expanded.groupby(["ZIP CODE", "year"], dropna=False)
    log_progress(f"Computing score metrics for {grouped.ngroups:,} ZIP/year groups.")

    for (zip_code, year), group in grouped:
            active_license_count = int(group["LICENSE ID"].nunique())
            positive = group[group["walkability_weight"] > 0]
            negative = group[group["walkability_weight"] < 0]

            positive_weight = int(positive["walkability_weight"].sum())
            negative_weight = int((-negative["walkability_weight"]).sum())
            net_weight = positive_weight - negative_weight

            positive_storefronts = int(positive["LICENSE ID"].nunique())
            positive_category_count = int(positive["walkability_category"].nunique())
            positive_category_coverage = (
                positive_category_count / len(positive_categories) if positive_categories else 0.0
            )

            total_directional_weight = positive_weight + negative_weight
            mix_balance = positive_weight / total_directional_weight if total_directional_weight else 0.5
            destination_intensity = min(math.log1p(positive_storefronts) / math.log(26), 1.0)

            walkability_score = round(
                (mix_balance * 50) + (positive_category_coverage * 25) + (destination_intensity * 25),
                1,
            )

            rows.append(
                {
                    "zip_code": zip_code,
                    "year": year,
                    "active_license_count": active_license_count,
                    "positive_storefront_count": positive_storefronts,
                    "positive_weight": positive_weight,
                    "negative_weight": negative_weight,
                    "net_weight": net_weight,
                    "positive_category_count": positive_category_count,
                    "positive_category_coverage": round(positive_category_coverage, 3),
                    "mix_balance": round(mix_balance, 3),
                    "destination_intensity": round(destination_intensity, 3),
                    "walkability_score": walkability_score,
                }
            )

    log_progress("Finished ZIP-by-year walkability score summary.")
    return pd.DataFrame(rows).sort_values(["zip_code", "year"]).reset_index(drop=True)


def summarize_zip_category_mix(frame: pd.DataFrame) -> pd.DataFrame:
    log_progress("Summarizing ZIP-by-year category mix.")
    expanded = expand_terms_by_active_year(frame)
    expanded = expanded[(expanded["year"] >= YEAR_MIN) & (expanded["year"] <= YEAR_MAX)].copy()
    out = (
        expanded.groupby(["ZIP CODE", "year", "walkability_category"], dropna=False)
        .agg(
            active_license_count=("LICENSE ID", "nunique"),
            category_weight=("walkability_weight", "sum"),
        )
        .reset_index()
    )
    log_progress("Finished ZIP-by-year category mix summary.")
    return out.rename(columns={"ZIP CODE": "zip_code"}).sort_values(["zip_code", "year", "walkability_category"])


def pivot_zip_scores_wide(zip_scores: pd.DataFrame) -> pd.DataFrame:
    log_progress("Pivoting ZIP/year scores into wide format.")
    metric_columns = [
        "active_license_count",
        "positive_storefront_count",
        "positive_weight",
        "negative_weight",
        "net_weight",
        "positive_category_count",
        "positive_category_coverage",
        "mix_balance",
        "destination_intensity",
        "walkability_score",
    ]

    zip_codes = pd.DataFrame(
        {"zip_code": sorted(zip_scores["zip_code"].dropna().astype(str).unique())}
    )

    wide = zip_codes.copy()
    for metric in metric_columns:
        pivoted = (
            zip_scores.pivot(index="zip_code", columns="year", values=metric)
            .reindex(columns=list(range(YEAR_MIN, YEAR_MAX + 1)))
            .reset_index()
        )
        pivoted.columns = ["zip_code"] + [f"{metric}_{year}" for year in range(YEAR_MIN, YEAR_MAX + 1)]
        wide = wide.merge(pivoted, on="zip_code", how="left")

    count_like = [
        col
        for col in wide.columns
        if col.startswith("active_license_count_")
        or col.startswith("positive_storefront_count_")
        or col.startswith("positive_weight_")
        or col.startswith("negative_weight_")
        or col.startswith("net_weight_")
        or col.startswith("positive_category_count_")
    ]
    wide[count_like] = wide[count_like].fillna(0).astype(int)

    float_like = [col for col in wide.columns if col not in {"zip_code", *count_like}]
    wide[float_like] = wide[float_like].fillna(0.0).round(3)
    log_progress("Finished wide ZIP score pivot.")
    return wide.sort_values("zip_code").reset_index(drop=True)


def print_preview(name: str, df: pd.DataFrame, rows: int = 5) -> None:
    print()
    print(f"{name} preview (first {min(rows, len(df))} rows):")
    if df.empty:
        print("[empty dataframe]")
        return
    print(df.head(rows).to_string(index=False))




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate ZIP-by-year walkability scores from Chicago business license term history."
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


    log_progress("Reading raw business license CSV.")
    raw = pd.read_csv(r"C:\Users\johnh\Desktop\Work\Datasets\Raw\Business_Licenses.csv")
    terms = build_license_terms(raw)
    zip_scores = summarize_zip_year_scores(terms)
    zip_scores_wide = pivot_zip_scores_wide(zip_scores)
    zip_mix = summarize_zip_category_mix(terms)

    print(terms.head())
    print(zip_scores.head())
    print(zip_scores_wide.head())
    print(zip_mix.head())
    """
    print_preview("Chicago license terms", terms)
    print_preview("ZIP yearly walkability scores", zip_scores)
    print_preview("ZIP wide walkability scores", zip_scores_wide)
    print_preview("ZIP yearly category mix", zip_mix)"""

    log_progress(f"Writing outputs to {output_dir}.")
    output_dir.mkdir(parents=True, exist_ok=True)
    terms.to_csv(output_dir / "biz_license_terms_scored.csv", index=False)
    zip_scores.to_csv(output_dir / "biz_license_walkability_scores_by_zip_year_long.csv", index=False)
    zip_scores_wide.to_csv(output_dir / "biz_license_walkability_scores_by_zip.csv", index=False)
    zip_mix.to_csv(output_dir / "biz_license_walkability_category_mix_by_zip_year.csv", index=False)
    log_progress("Pipeline complete.")

    print()
    print(f"Processed {len(raw):,} raw rows into {len(terms):,} scored Chicago license terms.")
    print(f"Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
