"""
Request models for POST /datasets/submit, grouped the same way as the
6G-DALI Metadata Application Profile document (Identity / Object
Characteristics / Testbed Context A-B-C / Provenance).
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


class DatasetMetrics(BaseModel):
    observation_point_horizontal: Optional[str] = None
    observation_point_vertical: Optional[str] = None
    measurement_family: list[str] = Field(default_factory=list)
    measurement_tool: list[str] = Field(default_factory=list)
    variable_measured: list[str] = Field(default_factory=list)
    measurement_technique: Optional[str] = None


class DatasetSubmission(BaseModel):
    # dataset_id and catalogue_id are NOT client-supplied: dataset_id is a
    # server-generated UUID, and catalogue_id is the fixed contributed-
    # datasets catalogue (see config.CONTRIBUTED_DATASETS_CATALOGUE).
    identity: DatasetIdentity
    object: DatasetObject
    testbed_context: TestbedContext = Field(default_factory=TestbedContext)
    metrics: DatasetMetrics = Field(default_factory=DatasetMetrics)