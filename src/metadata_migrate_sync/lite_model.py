"""the lite pydantic model to validat esgf document"""

from pydantic import BaseModel, field_validator
from datetime import datetime


class enforced_field(BaseModel):
    """fields will be enforced on their types."""

    latest: bool
    replica: bool
    retracted: bool
    deprecated: bool | None = None
    version: int
    dataset_id: str | None = None

    @field_validator("version")
    def validate_version(cls, v: int) -> int:
        if len(str(v)) != 8:
            raise ValueError("version must be in YYYYMMDD format (8 digits)")
        try:
            datetime.strptime(str(v), "%Y%m%d")
        except ValueError:
            raise ValueError("version must represent a valid date in YYYYMMDD format")
        return v
