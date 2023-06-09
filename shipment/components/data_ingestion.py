import sys
import os
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from typing import Tuple
from shipment.exception import ShippingException
from shipment.logger import logging
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.config_entity import DataIngestionConfig
from shipment.entity.artifact_entity import DataIngestionArtifacts
from shipment.constant import TEST_SIZE


class DataIngestion:
    def __init__(
        self, data_ingestion_config: DataIngestionConfig, mongo_op: MongoDBOperation
    ):
        self.data_ingestion_config = data_ingestion_config
        self.mongo_op = mongo_op

    # This method will fetch data from mongoDB
    def get_data_from_mongodb(self) -> DataFrame:

        """
        Method Name :   get_data_from_mongodb

        Description :   This method fetches data from MongoDB database. 
        
        Output      :   DataFrame 
        """
        logging.info("Entered get_data_from_mongodb method of Data_Ingestion class")
        try:
            logging.info("Getting the dataframe from mongodb")

            # Getting collection from MongoDB database
            df = self.mongo_op.get_collection_as_dataframe(
                self.data_ingestion_config.DB_NAME,
                self.data_ingestion_config.COLLECTION_NAME,
            )
            logging.info("Got the dataframe from mongodb")
            logging.info("Exited the get_data_from_mongodb method of Data_Ingestion class")
            return df

        except Exception as e:
            raise ShippingException(e, sys) from e



    # This method will split data:
    def split_data_as_train_test(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:

        """
        Method Name: split_data_as_train_test

        Description: This method will split the dataset into train and test based on split ratio and save it to the
                     desired location

        Output: Train and Test Dataframe
        """

        logging.info("Entered the split_data_as_train_test method of DataIngestion class.")
        try:
            #creating data ingestion artifacts dir inside artifacts folder:
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR, exist_ok= True)
            logging.info("Created artifacts directory.")

            #splitting data into train test:
            train_set, test_set = train_test_split(df, test_size=TEST_SIZE)

            #creating train directory under the data ingestion artifacts dir:
            os.makedirs(self.data_ingestion_config.TRAIN_DATA_ARTIFACTS_FILE_DIR,exist_ok=True)
            logging.info("Created train data directory.")

            #creating test directory under the data ingestion artifacts dir:
            os.makedirs(self.data_ingestion_config.TEST_DATA_ARTIFACTS_FILE_DIR, exist_ok= True)
            logging.info("Created test data directory.")

            #saving the train file to train directory:
            train_set.to_csv(self.data_ingestion_config.TRAIN_DATA_FILE_PATH, index = False, header = True)
            logging.info("Saved train file to train directory under artifacts directory.")

            #saving the test file to test directory:
            test_set.to_csv(self.data_ingestion_config.TEST_DATA_FILE_PATH, index = False, header = True)
            logging.info("Saved test file to test directory under artifacts directory.")

            logging.info("Exited split_data_as_train_test method of Data Ingestion Class.")

            return train_set, test_set

        except Exception as e:
            raise ShippingException(e,sys)
    
        

    # This method initiates data ingestion:
    def initiate_data_ingestion(self) -> DataIngestionArtifacts:

        """
        Method Name :   initiate_data_ingestion

        Description :   This method initiates data ingestion.
        
        Output      :   Data ingestion artifact 
        """
        logging.info("Entered initiate_data_ingestion method of Data_Ingestion class")
        try:
            # getting data from MongoDB
            df = self.get_data_from_mongodb()

            # dropping the unnecessary columns from dataframe
            df1 = df.drop(self.data_ingestion_config.DROP_COLS, axis=1)
            df1 = df1.dropna()
            logging.info("Got the data from mongodb")

            # splitting the data as train and test
            self.split_data_as_train_test(df1)
            logging.info("Exited the initiate_data_ingestion method of Data_Ingestion class.")

            #saving the data ingestion artifacts:
            data_ingestion_artifacts = DataIngestionArtifacts(
                train_data_file_path=self.data_ingestion_config.TRAIN_DATA_FILE_PATH,
                test_data_file_path=self.data_ingestion_config.TEST_DATA_FILE_PATH
            )

            return data_ingestion_artifacts

        except Exception as e:
            raise ShippingException(e,sys)





