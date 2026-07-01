"""
Task registry — describes each reusable dali @task as a dcat:DataService record.
The orchestrator reads this to register services in piveau and to present
available tasks when building new DAGs dynamically.
"""

SERVICES = {
    "dali-download-dataset": {
        "title":        "Data Lake Dataset Download",
        "description":  "Downloads a tabular dataset from a MinIO/S3 Data Lake bucket and returns its raw CSV content for downstream processing.",
        "service_type": "Transformation",
        "input_format": None,
        "output_format": "text/csv",
        "framework":    "Apache Airflow",
        "module":       "dali.datalake",
        "function":     "download_dataset",
    },
    "dali-upload-results": {
        "title":        "Data Lake Results Upload",
        "description":  "Serialises a processing report as JSON and uploads it to a MinIO/S3 Data Lake bucket, returning the object key of the stored file.",
        "service_type": "Transformation",
        "input_format": "application/json",
        "output_format": "application/json",
        "framework":    "Apache Airflow",
        "module":       "dali.datalake",
        "function":     "upload_results",
    },
    "dali-run-expectations": {
        "title":        "Great Expectations Dataset Validation",
        "description":  "Validates a CSV dataset against a configurable Great Expectations suite. Auto-generates expectations from piveau column metadata when none are provided. Returns a per-expectation pass/fail report with statistics.",
        "service_type": "QualityCheck",
        "input_format": "text/csv",
        "output_format": "application/json",
        "framework":    "Great Expectations",
        "module":       "dali.validation",
        "function":     "run_expectations",
    },
    "dali-report-outcome": {
        "title":        "Validation Outcome Reporter",
        "description":  "Logs a human-readable summary of a validation run — pass/fail status, expectation counts, and details of any failed checks.",
        "service_type": "QualityCheck",
        "input_format": "application/json",
        "output_format": None,
        "framework":    "Apache Airflow",
        "module":       "dali.validation",
        "function":     "report_outcome",
    },
    "dali-publish-quality-to-piveau": {
        "title":        "DQV Quality Annotations Publisher",
        "description":  "Reads a validation report and writes per-expectation dqv:QualityMeasurement nodes into the dataset's metadata record in the piveau catalogue, preserving existing metadata fields.",
        "service_type": "QualityCheck",
        "input_format": "application/json",
        "output_format": None,
        "framework":    "piveau-hub",
        "module":       "dali.dataspace",
        "function":     "publish_quality_to_piveau",
    },
}
