"""
Request models for the two-step dataset submission flow (POST /datasets then
POST /datasets/{dataset_id}/distributions), grouped the same way as the
6G-DALI Metadata Application Profile document (Identity / Object
Characteristics / Testbed Context A-B-C / Provenance).

Field placement follows the MAP: everything in TestbedContext (including the
CMT Content-C fields — observation points, measurement family/tools)
describes the dataset as a whole and is submitted at dataset-creation time.
DistributionMetrics (variable_measured, measurement_technique) describes one
distribution's file specifically (§5.3.E/§5.6) and is submitted per
distribution instead.
"""

from typing import Optional

from pydantic import BaseModel, Field


class DatasetIdentity(BaseModel):
    title: str
    description: str
    sns_project_name: str = "6G-DALI"
    publisher_name: Optional[str] = None
    contact_email: Optional[str] = None
    contributors: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    related_publications: list[str] = Field(default_factory=list)
    language: Optional[str] = None
    spatial: Optional[str] = None
    temporal_start: Optional[str] = None
    temporal_end: Optional[str] = None
    version: Optional[str] = None


class DatasetObject(BaseModel):
    license: str
    access_rights: str = "PUBLIC"  # PUBLIC | RESTRICTED | NON_PUBLIC
    gdpr_compliant: bool = True
    fair_compliant: bool = True
    contains_pii: bool = False
    produced_by: Optional[str] = None  # GAIA-X participant URI


class TestbedContext(BaseModel):
    underlay_platform: Optional[str] = None
    environment: Optional[str] = None
    network_domain: Optional[str] = None
    ran_3gpp_release: Optional[str] = None
    ran_new_radio_type: Optional[str] = None
    ran_split: Optional[str] = None
    ran_focused_technology: Optional[str] = None
    ran_coverage_type: Optional[str] = None
    ran_frequency_band: Optional[str] = None
    ran_bandwidth_mhz: Optional[float] = None
    ran_max_end_devices: Optional[int] = None
    ran_mobility_model: Optional[str] = None
    core_release: Optional[str] = None
    core_solution: Optional[str] = None
    transport_type: Optional[str] = None
    compute_orchestrator_type: Optional[str] = None
    compute_gpu_use: Optional[bool] = None
    compute_virtualization_type: Optional[str] = None
    compute_infrastructure_type: Optional[str] = None
    traffic_origin: Optional[str] = None
    traffic_pattern: Optional[str] = None
    slice_type: Optional[str] = None
    reference_plane: Optional[str] = None
    related_vertical: Optional[str] = None
    # CMT Content-C (dataset-level part) — where this distribution's own
    # measurements were observed within the testbed. Per-file column list and
    # technique live on the distribution instead (see DistributionMetrics).
    observation_point_horizontal: Optional[str] = None
    observation_point_vertical: Optional[str] = None
    measurement_family: list[str] = Field(default_factory=list)
    measurement_tool: list[str] = Field(default_factory=list)


class DistributionMetrics(BaseModel):
    """Describes one distribution's file — submitted with that distribution,
    not with the dataset (see MAP §5.3.E note / §5.6)."""
    variable_measured: list[str] = Field(default_factory=list)
    measurement_technique: Optional[str] = None


class DatasetCreateRequest(BaseModel):
    # dataset_id and catalogue_id are NOT client-supplied: dataset_id is a
    # server-generated UUID, and catalogue_id is the fixed contributed-
    # datasets catalogue (see config.CONTRIBUTED_DATASETS_CATALOGUE).
    identity: DatasetIdentity
    object: DatasetObject
    testbed_context: TestbedContext = Field(default_factory=TestbedContext)
