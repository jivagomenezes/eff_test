import logging
import pathlib

import pandas as pd

from src.ingestion import load_data_blocks, load_family_tree
from src.transform import enrich_family_tree_with_parent, run_data_checks

# logging configured here at the entry point -- other modules just get a logger by name and inherit this config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = pathlib.Path(__file__).parent.parent
DATA_DIR = ROOT / "TechTaskDEI_II (2026)"
OUTPUT_DIR = ROOT / "output"
OUTPUT_FILE = OUTPUT_DIR / "enriched_companies.parquet"

# hardcoded for clarity -- in a real system this would be discovered dynamically (e.g. os.listdir(DATA_DIR))
COMPANIES = ["companyA", "companyB", "companyC"]


def process_company(company: str) -> pd.DataFrame | None:
    """Process a single company folder and return its enriched DataFrame."""
    logger.info("--- Processing %s ---", company)
    company_dir = DATA_DIR / company

    members = load_family_tree(company_dir / "family_tree.json")
    if not members:
        # nothing to enrich -- skip without aborting the whole pipeline
        logger.warning("No valid members for %s, skipping", company)
        return None

    # data_blocks may come back as None if the file is malformed -- transform handles that gracefully
    data_blocks = load_data_blocks(company_dir / "data_blocks.json")
    df = enrich_family_tree_with_parent(members, data_blocks)
    run_data_checks(df)

    logger.info("%s done: %d records", company, len(df))
    return df


def run():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # filter out None results (companies that had no valid members)
    frames = [df for df in (process_company(c) for c in COMPANIES) if df is not None]

    if not frames:
        logger.error("Nothing to write")
        return

    # ignore_index=True resets the row indices -- without it each company would restart from 0
    combined = pd.concat(frames, ignore_index=True)
    combined.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    logger.info("Done. %d total records saved to %s", len(combined), OUTPUT_FILE)


if __name__ == "__main__":
    run()
