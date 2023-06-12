from dataclasses import dataclass


# Data Ingestion Artifacts:
@dataclass
class DataIngestionArtifacts:
    train_data_file_path: str
    test_data_file_path: str


# Data Validation Artifacts:
@dataclass
class DataValidationArtifacts:
    data_drift_file_path: str
    validation_status: bool


# Data Transformation Artifacts:
@dataclass
class DataTransformationArtifacts:
    transformed_object_file_path: str
    transfomed_train_file_path: str
    transfomed_test_file_path: str




