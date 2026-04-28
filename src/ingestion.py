import json
import logging
from pathlib import Path

from pydantic import ValidationError

from src.models import DataBlocks, FamilyTreeMember

logger = logging.getLogger(__name__)


def load_family_tree(filepath: Path) -> list[dict]:
    """
    Load family tree members from JSON.
    Validates that each member has duns and primaryName, skips invalid ones.
    Returns a list of plain dicts to keep downstream code simple.
    """
    logger.info("Loading family tree: %s", filepath)

    with open(filepath) as f:
        raw = json.load(f)  # full file in memory -- fine for now, see Scale section in README

    # using .get with default in case the JSON is missing the key (don't want a KeyError, just an empty result)
    raw_members = raw.get("familyTreeMembers", [])
    logger.info("Found %d raw members", len(raw_members))

    valid_members = []
    rejected = 0

    for i, raw_member in enumerate(raw_members):
        try:
            # only checking the critical fields here -- if it passes, keep the original dict
            FamilyTreeMember.model_validate(raw_member)
            valid_members.append(raw_member)
        except ValidationError as e:
            # log-and-continue: better to lose 1 bad record than abort 1000 good ones
            rejected += 1
            duns = raw_member.get("duns", f"index {i}")
            logger.error("Skipping member %s: %s", duns, e)

    logger.info("Loaded %d members, %d rejected", len(valid_members), rejected)
    return valid_members


def load_data_blocks(filepath: Path) -> dict | None:
    """
    Load and validate the data_blocks file.
    Returns a plain dict if valid, None otherwise.
    """
    logger.info("Loading data blocks: %s", filepath)

    with open(filepath) as f:
        raw = json.load(f)

    try:
        DataBlocks.model_validate(raw)
        logger.info("Loaded data blocks for %s (%s)", raw["duns"], raw["primaryName"])
        return raw
    except ValidationError as e:
        # returning None instead of raising -- pipeline can still process members without enrichment
        logger.error("Invalid data blocks at %s: %s", filepath, e)
        return None
