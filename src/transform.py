import logging

import pandas as pd

logger = logging.getLogger(__name__)


def enrich_family_tree_with_parent(
    family_tree_members: list[dict],
    data_blocks: dict | None,
) -> pd.DataFrame:
    """
    For each member in the family tree, build a flat row with:
      - the member's own info (duns, name, address, industry)
      - the parent linkage from corporateLinkage
      - global revenue and employees from data_blocks (the same for all members)

    Returns a DataFrame with one row per member.
    """
    # data_blocks can be None if its file failed validation -- pipeline still continues, globals just become null
    db = data_blocks or {}
    global_duns = db.get("duns")
    global_revenue = _get_first_usd_revenue(db.get("financials", []))
    global_employees = _get_first_employee_count(db.get("numberOfEmployees", []))

    # list of dicts -> DataFrame is the most readable approach for this size
    rows = [
        _build_row(member, global_duns, global_revenue, global_employees)
        for member in family_tree_members
    ]

    df = pd.DataFrame(rows)
    logger.info("Enrichment done: %d records", len(df))
    return df


def _build_row(
    member: dict,
    global_duns: str | None,
    global_revenue: float | None,
    global_employees: int | None,
) -> dict:
    """Flatten a single family tree member into a row."""
    # `or {}` everywhere because the source can return either missing key or explicit null
    # -- chaining .get() on None would crash, so coerce to empty dict first
    linkage = member.get("corporateLinkage") or {}
    parent = linkage.get("parent") or {}
    roles = linkage.get("familytreeRolesPlayed") or []

    address = member.get("primaryAddress") or {}
    country = (address.get("addressCountry") or {}).get("isoAlpha2Code")
    city = (address.get("addressLocality") or {}).get("name")

    industry = member.get("primaryIndustryCode") or {}

    # snake_case here -- SQL/Parquet convention, even though the source JSON is camelCase
    return {
        "duns": member.get("duns"),
        "primary_name": member.get("primaryName"),
        "start_date": member.get("startDate"),
        "country_iso": country,
        "city": city,
        "parent_duns": parent.get("duns"),
        "global_ultimate_duns": global_duns,
        "hierarchy_level": linkage.get("hierarchyLevel"),
        # joining roles into a single string -- could be array, but string keeps the schema simple
        "roles": ", ".join(r.get("description", "") for r in roles),
        "sic_code": industry.get("usSicV4"),
        "sic_description": industry.get("usSicV4Description"),
        "global_revenue_usd": global_revenue,
        "global_employee_count": global_employees,
    }


def _get_first_usd_revenue(financials: list) -> float | None:
    """Return the first yearly revenue in USD, or None."""
    if not financials:
        return None
    revenues = financials[0].get("yearlyRevenue", [])
    # iterating instead of just taking [0] -- the first entry might be in EUR/GBP/etc.
    for entry in revenues:
        if entry.get("currency") == "USD":
            return entry.get("value")
    return None


def _get_first_employee_count(employees: list) -> int | None:
    """Return the first employee count value, or None."""
    if not employees:
        return None
    # first entry is normally the consolidated count -- good enough for this pipeline
    return employees[0].get("value")


def run_data_checks(df: pd.DataFrame) -> None:
    """
    Run basic data quality checks before writing to disk.
    Raises ValueError if any check fails.
    """
    # using ValueError instead of assert -- assert can be disabled with python -O, these checks must always run
    if df.empty:
        raise ValueError("Output is empty")

    if df["duns"].isna().any():
        raise ValueError("Some records are missing DUNS")

    if not df["duns"].is_unique:
        raise ValueError("Duplicate DUNS in output")

    invalid_levels = df["hierarchy_level"].dropna() < 1
    if invalid_levels.any():
        raise ValueError("Invalid hierarchy level (must be >= 1)")

    # fill rates are observability, not blocking -- low fill rate isn't always an error
    for col in ["parent_duns", "global_revenue_usd", "global_employee_count"]:
        fill_rate = df[col].notna().mean() * 100
        logger.info("%s fill rate: %.0f%%", col, fill_rate)

    logger.info("Data checks passed")
