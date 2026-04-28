from pydantic import BaseModel, ConfigDict


# Only validating the fields that actually break the pipeline if missing.
# Everything else is treated as optional and accessed defensively in transform.py
# (the D&B source has too many optional nested fields to model them all).

class FamilyTreeMember(BaseModel):
    model_config = ConfigDict(extra="allow")  # could be "ignore", but "allow" keeps the rest of the dict accessible if needed later

    duns: str          # business key -- can't do anything without it
    primaryName: str   # required for any output to be useful


class DataBlocks(BaseModel):
    model_config = ConfigDict(extra="allow")

    duns: str
    primaryName: str
