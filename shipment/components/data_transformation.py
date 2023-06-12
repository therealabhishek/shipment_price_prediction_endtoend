# importing the required libraries:
import os
from shipment.logger import logging
import sys
from pandas import DataFrame
import numpy as np
import pandas as pd
from category_encoders.binary import BinaryEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder,StandardScaler
from shipment.entity.config_entity import DataTransformationConfig
from shipment.entity.artifact_entity import (
    DataIngestionArtifacts,
    DataTransformationArtifacts
)
from shipment.exception import ShippingException



class DataTransformation:
    def __init__(self,
                 data_ingestion_artifacts: DataIngestionArtifacts,
                 data_transformation_config: DataTransformationConfig):
        
        self.data_ingestion_artifacts = data_ingestion_artifacts
        self.data_transformation_config = data_transformation_config

        #reading train and test csv files from data ingestion artifacts:
        self.train_set = pd.read_csv(self.data_ingestion_artifacts.train_data_file_path)
        self.test_set = pd.read_csv(self.data_ingestion_artifacts.test_data_file_path)


    def get_data_transformer_object(self) -> object:
        logging.info("Entered get_data_transformer_object method of Data_Ingestion class.")
        try:
            # getting the required columns from the schema.yaml file:
            # numerical columns
            numerical_columns = self.data_transformation_config.SCHEMA_CONFIG["numerical_columns"]
            
            # categorical columns to onehot encode
            onehot_columns = self.data_transformation_config.SCHEMA_CONFIG["onehot_columns"]

            # binary columns
            binary_columns = self.data_transformation_config.SCHEMA_CONFIG["binary_columns"]

            logging.info("Got numerical columns, categorical cols to onehot encode and binary columns")

            # creating transformer objects:
            numeric_transformer = StandardScaler()
            ohe_transformer = OneHotEncoder()
            binary_transformer = BinaryEncoder()
            logging.info("Initialized Standard Scaler, One Hot Encoder, BinaryEncoder")

            # Creating a pre processing pipeline using the above objects:
            preprocessor = ColumnTransformer(
                [
                    ("OneHotEncoder",ohe_transformer, onehot_columns),
                    ("BinaryEncoder",binary_transformer, binary_columns),
                    ("StandardScaler", numeric_transformer, numerical_columns)
                ]
            )

            logging.info("Created preprocessor object from Column Transformer.")
            logging.info("Exited get_data_transformer_object method of DataTransformation class.")
            return preprocessor
        except Exception as e:
            raise ShippingException(e,sys)
        


    # This is static method for capping the outliers
    @staticmethod
    def _outlier_capping(col, df: DataFrame) -> DataFrame:

        """
        Method Name :   _outlier_capping

        Description :   This method performs outlier capping in the dataframe. 
        
        Output      :   DataFrame. 
        """
        logging.info("Entered _outlier_capping method of Data_Transformation class")
        try:
            logging.info("Performing _outlier_capping for columns in the dataframe")
            percentile25 = df[col].quantile(0.25)  # calculating 25 percentile
            percentile75 = df[col].quantile(0.75)  # calculating 75 percentile

            # Calculating upper limit and lower limit
            iqr = percentile75 - percentile25
            upper_limit = percentile75 + 1.5 * iqr
            lower_limit = percentile25 - 1.5 * iqr

            # Capping the outliers
            df.loc[(df[col] > upper_limit), col] = upper_limit
            df.loc[(df[col] < lower_limit), col] = lower_limit
            logging.info("Performed _outlier_capping method of Data_Transformation class")
            logging.info("Exited _outlier_capping method of Data_Transformation class")
            return df

        except Exception as e:
            raise ShippingException(e, sys)
        


    # This method is used to initialize the data transformer:
    def initiate_data_transformation(self) -> DataTransformationArtifacts:
        logging.info("Entered initiate_data_transformation method of Data Transformation class.")
        try:
            # Creating directory for data transformation artifacts
            os.makedirs(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR,exist_ok=True,)
            logging.info("Created artifacts directory for Data Transformation")

            # Getting preprocessor object
            preprocessor = self.get_data_transformer_object()
            logging.info("Got the preprocessor object")

            # Getting target column name from schema file
            target_column_name = self.data_transformation_config.SCHEMA_CONFIG["target_column"]
            # Getting traget column name from schema file
            numerical_columns = self.data_transformation_config.SCHEMA_CONFIG["numerical_columns"]  
            logging.info("Got target column name and numerical columns from schema config")


            # considering only those features which have unique values more than 25
            continuous_columns = [feature for feature in numerical_columns if len(self.train_set[feature].unique()) >= 25]
            logging.info("Got a list of continuous_columns")

            # Outlier capping for train data
            [self._outlier_capping(col, self.train_set) for col in continuous_columns]
            logging.info("Outliers capped in train df")

            # Outlier capping for test data
            [self._outlier_capping(col, self.test_set) for col in continuous_columns]
            logging.info("Outliers capped in test df")

            # Getting the input features and target feature of Training dataset
            input_feature_train_df = self.train_set.drop(columns=[target_column_name], axis=1)
            target_feature_train_df = self.train_set[target_column_name]
            logging.info("Got train features and target feature")

            # Getting the input features and target feature of Testing dataset
            input_feature_test_df = self.test_set.drop(columns=[target_column_name], axis=1)
            target_feature_test_df = self.test_set[target_column_name]
            logging.info("Got test features and target feature")

            # Applying preprocessing object on training dataframe and testing dataframe
            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessor.transform(input_feature_test_df)
            logging.info("Used the preprocessor object to transform the test features")

            # Concatinating input feature array and target feature array of Train dataset
            train_arr = np.c_[input_feature_train_arr, np.array(target_feature_train_df)]
            logging.info("Created train array.")

            # Creating direcory for transformed train dataset array and saving the array
            os.makedirs(self.data_transformation_config.TRANSFORMED_TRAIN_DATA_DIR,exist_ok=True,)
            transformed_train_file = self.data_transformation_config.UTILS.save_numpy_array_data(
                self.data_transformation_config.TRANSFORMED_TRAIN_FILE_PATH, train_arr
            )
            logging.info("Saved train array to transformed train file directory.")


            # Concatinating input feature array and target feature array of Test dataset
            test_arr = np.c_[input_feature_test_arr, np.array(target_feature_test_df)]
            logging.info("Created test array.")

            # Creating direcory for transformed test dataset array and saving the array
            os.makedirs(self.data_transformation_config.TRANSFORMED_TEST_DATA_DIR, exist_ok=True)
            transformed_test_file = self.data_transformation_config.UTILS.save_numpy_array_data(
                self.data_transformation_config.TRANSFORMED_TEST_FILE_PATH, test_arr
            )
            logging.info("Saved test array to transformed test file directory.")


            # Saving the preprocessor object to data transformation artifacts directory
            preprocessor_obj_file = self.data_transformation_config.UTILS.save_object(
                self.data_transformation_config.PREPROCESSOR_FILE_PATH, preprocessor
            )
            logging.info("Saved the preprocessor object in DataTransformation artifacts directory.")
            logging.info("Exited initiate_data_transformation method of Data_Transformation class")

            # Saving data transformation artifacts
            data_transformation_artifacts = DataTransformationArtifacts(
                transformed_object_file_path=preprocessor_obj_file,
                transformed_train_file_path=transformed_train_file,
                transformed_test_file_path=transformed_test_file
            )

            return data_transformation_artifacts

        except Exception as e:
            raise ShippingException(e,sys)