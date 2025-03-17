"""
The Controlled Vocabularies from CMIP5 and CMIP6
They are all optional and indexed
"""

from pydantic import BaseModel, Field, ConfigDict


class CMIP5CV(BaseModel):
    """
    CVs and keywords used by CMIP5
    """

    model_config = ConfigDict(extra="forbid")
    forcing: list[str] | None = None
    drs_id: list[str] | None = None
    ensemble: list[str] | None = None
    experiment: list[str] | None = None
    model: list[str] | None = None
    institute: list[str] | None = None
    master_gateway: list[str] | None = None
    access: list[str] | None = None
    variable_unit: list[str] | None = None
    time_frequency: list[str] | None = None
    cmor_table: list[str] | None = None
    cf_standard_name: list[str] | None = None
    experiment_family: list[str] | None = None
    format: list[str] | None = None
    product: list[str] | None = None
    project: list[str] | None = None
    realm: list[str] | None = None
    variable: list[str] | None = None
    variable_long_name: list[str] | None = None
    variable_units: list[str] | None = None
    geo: list[str] | None = None  # 'ENVELOPE(-180.0, -1.406...88.927734, -88.927734)'
    geo_units: list[str] | None = None
    tracking_id: list[str] | None = None

class CMIP6CV(BaseModel):
    """
    Cvs and keywords from CMIP6
    all fields are indexed(searchable and sortable)
    """

    model_config = ConfigDict(extra="forbid")
    mip_era: list[str] | None = None
    activity_id: list[str] | None = None
    experiment_id: list[str] | None = None
    institution_id: list[str] | None = None
    member_id: list[str] | None = None
    source_id: list[str] | None = None
    sub_experiment_id: list[str] | None = None
    realm: list[str] | None = None
    table_id: list[str] | None = None
    pid: list[str] | None = None
    source_type: list[str] | None = None
    experiment_title: list[str] | None = None
    activity_drs: list[str] | None = None
    cf_standard_name: list[str] | None = None
    data_specs_version: list[str] | None = None
    frequency: list[str] | None = None
    further_info_url: list[str] | None = None
    grid: list[str] | None = None
    grid_label: list[str] | None = None
    nominal_resolution: list[str] | None = None
    product: list[str] | None = None
    project: list[str] | None = None
    variable: list[str] | None = None
    variable_id: list[str] | None = None
    variable_long_name: list[str] | None = None
    variable_units: list[str] | None = None
    variant_label: list[str] | None = None
    branch_method: list[str] | None = None
    tracking_id: list[str] | None = None




