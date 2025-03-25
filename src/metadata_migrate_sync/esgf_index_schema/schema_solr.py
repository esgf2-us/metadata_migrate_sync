from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag

from .schema_cmipcv import CMIP5CV, CMIP6CV


class DatasetParams(BaseModel):
    """params in the response header for type=Dataset"""

    df: str
    q_alt: str = Field(..., alias="q.alt")
    indent: str
    echoParams: str
    fl: str
    start: str
    fq: str | list[str]
    rows: str
    q: str
    shards: str | None = None  # it is indexable
    tie: str
    facet_limit: str = Field(..., alias="facet.limit")
    qf: str
    facet_method: str = Field(..., alias="facet.method")
    facet_mincount: str = Field(..., alias="facet.mincount")
    wt: Literal["json", "xml"]
    facet_sort: str = Field(..., alias="facet.sort")


class FileParams(BaseModel):
    """params in the response header for type=File"""

    sort: str
    facet: str | None = None


class BaseHeader(BaseModel):
    status: int
    QTime: int


class DatasetHeader(BaseHeader):
    params: DatasetParams


class FileHeader(BaseHeader):
    params: FileParams


class BaseIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    title: str
    type: Literal["Dataset", "File", "Aggregation"]

    data_node: str
    index_node: str

    replica: bool
    latest: bool
    retracted: bool

    size: int | None = None
    url: list[str] | None

    version: str | None = None
    dataset_id: str | None = None
    master_id: str | None = None
    instance_id: str | None = None
    timestamp: str | None = None
    x_timestamp: str | datetime | None = Field(alias="_timestamp", default=None)

    description: list[str] | None = None
    height_bottom: float | None = None
    height_top: float | None = None
    height_units: str | None = None
    metadata_format: str | None = None
    metadata_url: str | None = None
    metadata_file_name: str | None = None

    text: list[str] | None = None
    text_rev: list[str] | None = None

    # geo: ? random?
    bbox: str | None = None
    x_version: str | None = Field(alias="_version", default=None)
    x_root_: str | None = Field(alias="_root_", default=None)
    x_version_: int | None = Field(alias="_version_", default=None)


class BaseDocs(BaseIndex):
    dataset_id_template_: list[str]
    directory_format_template_: list[str]
    model_cohort: list[str]
    citation_url: list[str]
    score: float


# single

#
#  <dynamicField name="*date" type="date" indexed="true" stored="true" multiValued="false"/>
#  <dynamicField name="date*" type="date" indexed="true" stored="true" multiValued="false"/>
#


class DatasetIndex(BaseModel):
    number_of_aggregations: int
    number_of_files: int
    datetime_start: datetime
    datetime_stop: datetime


class FileIndex(BaseModel):
    north_degrees: float
    south_degrees: float
    east_degrees: float
    west_degrees: float


class DatasetDocs5(BaseDocs, DatasetIndex, CMIP5CV):
    model_cohort: list[str] | None = None
    citation_url: list[str] | None = None
    directory_format_template_: list[str] | None = None

    north_degrees: float | None = None
    south_degrees: float | None = None
    east_degrees: float | None = None
    west_degrees: float | None = None

    datetime_start: datetime | None = None
    datetime_stop: datetime | None = None


class DatasetDocs6(BaseDocs, DatasetIndex, CMIP6CV):
    access: list[str]
    xlink: list[str]


class FileDocs5(BaseDocs, FileIndex, CMIP5CV):
    short_description: list[str] | None = None
    creation_date: datetime | None = None
    checksum: list[str] | None = None
    checksum_type: list[str] | None = None
    publish_path: list[str | Path] | None = None
    north_degrees: float | None = None
    south_degrees: float | None = None
    east_degrees: float | None = None
    west_degrees: float | None = None

    directory_format_template_: list[str] | None = None
    model_cohort: list[str] | None = None
    citation_url: list[str] | None = None

    datetime_start: datetime | None = None
    datetime_stop: datetime | None = None


class FileDocs6(BaseDocs, FileIndex, CMIP6CV):
    short_description: list[str]
    creation_date: datetime
    checksum: list[str]
    checksum_type: list[str]
    publish_path: list[str | Path]


class BaseResponse(BaseModel):
    numFound: int
    start: int
    maxScore: float


def mip_discriminator(v: dict[str, Any]):
    if "mip_era" in v:
        return "CMIP6"
    else:
        return "CMIP5"


DatasetDocs = Annotated[
    (Annotated[DatasetDocs5, Tag("CMIP5")] | Annotated[DatasetDocs6, Tag("CMIP6")]),
    Discriminator(mip_discriminator),
]


class DatasetResponse(BaseResponse):
    docs: list[DatasetDocs]


FileDocs = Annotated[
    (Annotated[FileDocs5, Tag("CMIP5")] | Annotated[FileDocs6, Tag("CMIP6")]),
    Discriminator(mip_discriminator),
]


class FileResponse(BaseResponse):
    model_config = ConfigDict(extra="forbid")
    docs: list[FileDocs]


class DatasetSolr(BaseModel):
    model_config = ConfigDict(extra="forbid")
    responseHeader: DatasetHeader
    response: DatasetResponse


class FacetModel(BaseModel):
    facet_queries: dict[str, Any]
    facet_fields: dict[str, Any]
    facet_ranges: dict[str, Any]
    facet_intervals: dict[str, Any]
    facet_heatmaps: dict[str, Any]


class FileSolr(BaseModel):
    model_config = ConfigDict(extra="forbid")
    responseHeader: FileHeader
    response: FileResponse
    facet_counts: FacetModel | None = None
