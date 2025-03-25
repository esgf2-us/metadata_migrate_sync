"""syncing the indexes from the staged ones to the public (one way)."""
from typing import Literal

from pydantic import validate_call

from metadata_migrate_sync.project import ProjectReadWrite


@validate_call
def metadata_sync(
    *,
    source_epname: Literal["public"],
    target_epname: Literal["stage"],
    metatype: Literal["files", "datasets"],
    project: ProjectReadWrite
) -> None:
    """Sync the metadata between two Globus Indexes."""
    pass


