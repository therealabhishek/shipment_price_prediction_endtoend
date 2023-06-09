# importing the required libraries:

import sys
from json import loads
from typing import Collection
from pandas import DataFrame
from pymongo.database import Database
import pandas as pd
from pymongo import MongoClient
from shipment.constant import DB_URL
from shipment.exception import ShippingException
from shipment.logger import logging


class MongoDBOperation:
    def __init__(self) -> None:
        self.DB_URL = DB_URL
        self.client = MongoClient(self.DB_URL)

    
    def get_database(self, db_name) -> Database:

        """
        Method Name : get_database

        Description : This method creates a mongodb database based on the database name given

        Output : Mongodb database
        
        """

        logging.info("Entered get_database method of MongoDB_Operations class.")

        try:
            db = self.client[db_name]

            logging.info(f"Created {db_name} database in MongoDB.")
            logging.info("Exited get_database method of MongoDB_Operations class.")
            return db
        except Exception as e:
            raise ShippingException(e,sys)
        

    
    @staticmethod
    def get_collection(database, collection_name) -> Collection:

        """
        Method Name :   get_collection
        
        Description :   This method gets collection from the particular database and collection name
        
        Output      :   Mongodb Collection
        """
        logging.info("Entered get_collection method of MongoDB_Operation class")

        try:
            # Getting the collection name
            collection = database[collection_name]

            logging.info(f"Created {collection_name} collection in mongodb")
            logging.info("Exited get_collection method of MongoDB_Operation class ")
            return collection

        except Exception as e:
            raise ShippingException(e, sys)
        

    
    def get_collection_as_dataframe(self, db_name, collection_name):
        """
        Method Name: get_collection_as_dataframe

        Description: This method is used to convert the selected collection to dataframe

        Output: A collection is returned from the selected db_name and collection_name
    
        """

        logging.info("Entered get_collection_as_dataframe method of MongoDB_Operation class")

        try:

            # getting the database:
            database = self.get_database(db_name)

            # getting the collection name:
            collection = database.get_collection(collection_name)

            #reading the dataframe and dropping unwanted column
            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            logging.info("Converted collection to Dataframe.")
            logging.info("Exited get_collection_as_dataframe method of MongoDB_Operation class.")
            return df
        
        except Exception as e:
            raise ShippingException(e,sys)

