import pandas as pd

from src.transform import enrich_family_tree_with_parent


def test_enrich_attaches_parent_duns_correctly():
    """
    Checks the core join logic of the enrichment:
      - the root (Global Ultimate) has no parent
      - the subsidiary points to the root via parent_duns
      - no records are dropped
      - hierarchy_level is preserved
    """
    # synthetic data on purpose -- minimum needed to exercise the parent-child relationship
    members = [
        {
            "duns": "111111111",
            "primaryName": "Root Corporation",
            "corporateLinkage": {
                "hierarchyLevel": 1,
                "parent": None,  # root has no parent
                "familytreeRolesPlayed": [
                    {"description": "Global Ultimate", "dnbCode": 12775}
                ],
            },
        },
        {
            "duns": "222222222",
            "primaryName": "Sub LLC",
            "corporateLinkage": {
                "hierarchyLevel": 2,
                "parent": {"duns": "111111111"},  # points back to the root
                "familytreeRolesPlayed": [
                    {"description": "Subsidiary", "dnbCode": 9159}
                ],
            },
        },
    ]

    data_blocks = {"duns": "111111111", "primaryName": "Root Corporation"}

    result = enrich_family_tree_with_parent(members, data_blocks)

    root = result[result["duns"] == "111111111"].iloc[0]
    sub = result[result["duns"] == "222222222"].iloc[0]

    # pd.isna instead of `is None` because pandas uses NaN for nulls in string columns
    assert pd.isna(root["parent_duns"]), "Root should have null parent_duns"
    assert sub["parent_duns"] == "111111111", "Sub should point to root"
    # this assert catches the classic bug of silently dropping records during the join
    assert len(result) == 2, "No records should be dropped"
    assert root["hierarchy_level"] == 1
    assert sub["hierarchy_level"] == 2
