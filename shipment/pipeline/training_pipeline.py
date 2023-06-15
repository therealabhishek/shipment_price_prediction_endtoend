import sys
from shipment.exception import ShippingException
from shipment.logger import logging
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    ModelEvaluationConfig
)

from shipment.entity.artifact_entity import (
    DataIngestionArtifacts,
    DataValidationArtifacts,
    DataTransformationArtifacts,
    ModelTrainerArtifacts,
    ModelEvaluationArtifacts
)

from shipment.components.data_ingestion import DataIngestion
from shipment.components.data_validation import DataValidation
from shipment.components.data_transformation import DataTransformation
from shipment.components.model_trainer import ModelTrainer
from shipment.components.model_evaluation import ModelEvaluation
from shipment.configuration.s3_operations import S3Operation



class TrainPipeline:
    def __init__(self) -> None:
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()
        self.model_evaluation_config = ModelEvaluationConfig()
        self.s3_operations = S3Operation()
        self.mongo_op = MongoDBOperation()


    # method is used to start data ingestion:
    def start_data_ingestion(self) -> DataIngestionArtifacts:
        logging.info("Entered the start_data_ingestion method of TrainPipeline class")
        try:
            logging.info("Getting the data from mongodb")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config, mongo_op=self.mongo_op)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info("Got Data from MongoDB.")
            logging.info("Got the train_set and test_set from mongodb")
            logging.info("Exited the start_data_ingestion method of TrainPipeline class")
            return data_ingestion_artifact

        except Exception as e:
            raise ShippingException(e, sys) from e
        

         # This method is used to start the data validation
    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifacts) -> DataValidationArtifacts:
        logging.info("Entered the start_data_validation method of TrainPipeline class")
        try:
            data_validation = DataValidation(data_ingestion_artifacts=data_ingestion_artifact,
                                             data_validation_config=self.data_validation_config,
                                             )
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info("Performed the data validation operation")
            logging.info("Exited the start_data_validation method of TrainPipeline class")
            return data_validation_artifact
        except Exception as e:
            raise ShippingException(e,sys)
        

    # This method is used to start the data transformation
    def start_data_transformation(
        self, data_ingestion_artifact: DataIngestionArtifacts
    ) -> DataTransformationArtifacts:
        logging.info(
            "Entered the start_data_transformation method of TrainPipeline class"
        )
        try:
            data_transformation = DataTransformation(
                data_ingestion_artifacts=data_ingestion_artifact,
                data_transformation_config=self.data_transformation_config,
            )
            data_transformation_artifact = (
                data_transformation.initiate_data_transformation()
            )
            logging.info(
                "Exited the start_data_transformation method of TrainPipeline class"
            )
            return data_transformation_artifact

        except Exception as e:
            raise ShippingException(e, sys) from e
        

    # This method is used to start the model trainer
    def start_model_trainer(self, data_transformation_artifact: DataTransformationArtifacts) -> ModelTrainerArtifacts:
        try:
            model_trainer = ModelTrainer(
                data_transformation_artifact=data_transformation_artifact,
                model_trainer_config=self.model_trainer_config,
            )
            model_trainer_artifact = model_trainer.initiate_model_trainer()
            return model_trainer_artifact

        except Exception as e:
            raise ShippingException(e,sys)
        

    # This method is used to start the model evaluation
    def start_model_evaluation(
        self,
        data_ingestion_artifact: DataIngestionArtifacts,
        model_trainer_artifact: ModelTrainerArtifacts,
    ) -> ModelEvaluationArtifacts:
        try:
            model_evaluation = ModelEvaluation(
                model_evaluation_config=self.model_evaluation_config,
                data_ingestion_artifact=data_ingestion_artifact,
                model_trainer_artifact=model_trainer_artifact,
            )
            model_evaluation_artifact = model_evaluation.initiate_model_evaluation()
            return model_evaluation_artifact

        except Exception as e:
            raise ShippingException(e, sys) from e


    def run_pipeline(self) -> None:
        logging.info("Entered the run_pipeline method of TrainPipeline class")
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(data_ingestion_artifact=data_ingestion_artifact)
            model_trainer_artifact = self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            
            model_evaluation_artifact = self.start_model_evaluation(
                data_ingestion_artifact=data_ingestion_artifact,
                model_trainer_artifact=model_trainer_artifact
            )

            if not model_evaluation_artifact.is_model_accepted:
                print("Model not Accepted.")
            else:
                print("Model Accepted.")


            logging.info("Exited the run_pipeline method of TrainPipeline class.")

        except Exception as e:
            raise ShippingException(e,sys)