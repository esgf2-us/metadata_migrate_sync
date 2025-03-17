"""
syncing the indexes from the staged ones to the public (one way)
"""


from pydantic import validate_call

@validate_call
def metadata_sync(
    *,
    source_epname: Literal["public"],
    target_epname: Literal["stage"],
    metatype: Literal["files", "datasets"],
    project: ProjectReadWrite
) -> None:

    pass


