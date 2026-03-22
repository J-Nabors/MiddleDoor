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

def classify_license(license_description: object, business_activity: object, dba_name: object) -> tuple[str, int]:
    license_text = normalize_text(license_description)
    activity_text = normalize_text(business_activity)
    dba_text = normalize_text(dba_name)
    combined = " | ".join(part for part in [activity_text, license_text, dba_text] if part)

    for rule in CATEGORY_RULES:
        if any(keyword in combined for keyword in rule["keywords"]):
            return rule["category"], int(rule["weight"])
        if any(desc.lower() == license_text for desc in rule["license_descriptions"]):
            return rule["category"], int(rule["weight"])

    return "uncategorized", 0


def classify_licenses_vectorized(frame: pd.DataFrame) -> pd.DataFrame:
    license_text = frame["LICENSE DESCRIPTION"].fillna("").astype(str).str.lower().str.strip()
    activity_text = frame["BUSINESS ACTIVITY"].fillna("").astype(str).str.lower().str.strip()
    dba_text = frame["DOING BUSINESS AS NAME"].fillna("").astype(str).str.lower().str.strip()
    combined = activity_text + " | " + license_text + " | " + dba_text

    categories = pd.Series("uncategorized", index=frame.index, dtype="string")
    weights = pd.Series(0, index=frame.index, dtype="int64")

    for rule in CATEGORY_RULES:
        keyword_mask = pd.Series(False, index=frame.index)
        for keyword in rule["keywords"]:
            keyword_mask = keyword_mask | combined.str.contains(re.escape(keyword), na=False, regex=True)

        desc_mask = pd.Series(False, index=frame.index)
        if rule["license_descriptions"]:
            desc_mask = license_text.isin([item.lower() for item in rule["license_descriptions"]])

        mask = (categories == "uncategorized") & (keyword_mask | desc_mask)
        categories.loc[mask] = rule["category"]
        weights.loc[mask] = int(rule["weight"])

    return pd.DataFrame(
        {
            "walkability_category": categories,
            "walkability_weight": weights,
        }
    )


def build_license_terms(df: pd.DataFrame) -> pd.DataFrame:
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

    classified = classify_licenses_vectorized(frame)
    frame["walkability_category"] = classified["walkability_category"]
    frame["walkability_weight"] = classified["walkability_weight"]

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

    frame["year_issued"] = frame["term_start"].dt.year.astype("Int64")
    frame["year_ended"] = frame["term_end"].dt.year.astype("Int64")
    return frame


def expand_terms_by_active_year(frame: pd.DataFrame) -> pd.DataFrame:
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
    return expanded


def summarize_zip_year_scores(frame: pd.DataFrame) -> pd.DataFrame:
    positive_categories = [rule["category"] for rule in CATEGORY_RULES if int(rule["weight"]) > 0]
    expanded = expand_terms_by_active_year(frame)
    expanded = expanded[(expanded["year"] >= YEAR_MIN) & (expanded["year"] <= YEAR_MAX)].copy()
    rows: list[dict[str, object]] = []

    for (zip_code, year), group in expanded.groupby(["ZIP CODE", "year"], dropna=False):
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

    return pd.DataFrame(rows).sort_values(["zip_code", "year"]).reset_index(drop=True)


def summarize_zip_category_mix(frame: pd.DataFrame) -> pd.DataFrame:
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
    return out.rename(columns={"ZIP CODE": "zip_code"}).sort_values(["zip_code", "year", "walkability_category"])


def pivot_zip_scores_wide(zip_scores: pd.DataFrame) -> pd.DataFrame:
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
    return wide.sort_values("zip_code").reset_index(drop=True)


def print_preview(name: str, df: pd.DataFrame, rows: int = 5) -> None:
    print()
    print(f"{name} preview (first {min(rows, len(df))} rows):")
    if df.empty:
        print("[empty dataframe]")
        return
    print(df.head(rows).to_string(index=False))


def write_outputs(
    terms: pd.DataFrame, zip_scores: pd.DataFrame, zip_scores_wide: pd.DataFrame, zip_mix: pd.DataFrame, output_dir: Path
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    terms.to_csv(output_dir / "biz_license_terms_scored.csv", index=False)
    zip_scores.to_csv(output_dir / "biz_license_walkability_scores_by_zip_year_long.csv", index=False)
    zip_scores_wide.to_csv(output_dir / "biz_license_walkability_scores_by_zip.csv", index=False)
    zip_mix.to_csv(output_dir / "biz_license_walkability_category_mix_by_zip_year.csv", index=False)


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
    input_path = Path(args.input_csv)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    raw = pd.read_csv(r"C:\Users\johnh\Desktop\Work\Datasets\Raw\Business_Licenses.csv")
    terms = build_license_terms(raw)
    zip_scores = summarize_zip_year_scores(terms)
    zip_scores_wide = pivot_zip_scores_wide(zip_scores)
    zip_mix = summarize_zip_category_mix(terms)

    print_preview("Chicago license terms", terms)
    print_preview("ZIP yearly walkability scores", zip_scores)
    print_preview("ZIP wide walkability scores", zip_scores_wide)
    print_preview("ZIP yearly category mix", zip_mix)

    write_outputs(terms, zip_scores, zip_scores_wide, zip_mix, output_dir)

    print()
    print(f"Processed {len(raw):,} raw rows into {len(terms):,} scored Chicago license terms.")
    print(f"Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
