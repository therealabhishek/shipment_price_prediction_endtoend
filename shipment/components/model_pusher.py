import sys
from shipment.configuration.s3_operations import S3Operation
from shipment.entity.artifact_entity import (
    DataTransformationArtifacts,
    ModelPusherArtifacts,
    ModelTrainerArtifacts,
)
from shipment.entity.config_entity import ModelPusherConfig
from shipment.exception import ShippingException
from shipment.logger import logging



class ModelPusher:
    def __init__(
        self,
        model_pusher_config: ModelPusherConfig,
        model_trainer_artifacts: ModelTrainerArtifacts,
        data_transformation_artifacts: DataTransformationArtifacts,
        s3: S3Operation,
    ):

        self.model_pusher_config = model_pusher_config
        self.model_trainer_artifacts = model_trainer_artifacts
        self.data_transformation_artifacts = data_transformation_artifacts
        self.s3 = s3

    # this is method is used to initiate model pusher
    def initiate_model_pusher(self) -> ModelPusherArtifacts:

        """
        Method Name :   initiate_model_pusher

        Description :   This method initiates model pusher. 
        
        Output      :    Model pusher artifact 
        """
        logging.info("Entered initiate_model_pusher method of ModelTrainer class")
        try:
            # Uploading the best model to s3 bucket
            self.s3.upload_file(
                self.model_trainer_artifacts.trained_model_file_path,
                self.model_pusher_config.S3_MODEL_KEY_PATH,
                self.model_pusher_config.BUCKET_NAME,
                remove=False,
            )
            logging.info("Uploaded best model to s3 bucket")
            logging.info("Exited initiate_model_pusher method of ModelTrainer class")

            # Saving the model pusher artifacts
            model_pusher_artifact = ModelPusherArtifacts(
                bucket_name=self.model_pusher_config.BUCKET_NAME,
                s3_model_path=self.model_pusher_config.S3_MODEL_KEY_PATH,
            )

            return model_pusher_artifact

        except Exception as e:
            raise ShippingException(e, sys) from e