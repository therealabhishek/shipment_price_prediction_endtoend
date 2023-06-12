import shutil
import sys
from typing import Dict, Tuple, List
import dill
import xgboost
import numpy as np
import pandas as pd
import yaml
from pandas import DataFrame
#from sklearn.metrics import r2_score
from sklearn.model_selection import GridSearchCV
#from sklearn.utils import all_estimators
#from yaml import safe_dump
from shipment.constant import *
from shipment.exception import ShippingException
from shipment.logger import logging


class MainUtils:
    def read_yaml_file(self, filename:str) -> dict:
        logging.info("Enetered read_yaml_file method of MainUtils class.")
        try:
            with open(filename, "rb") as yaml_file:
                return yaml.safe_load(yaml_file)
        except Exception as e:
            raise ShippingException(e,sys)
        

    def write_json_to_yaml_file(self, json_file: dict, yaml_file_path: str) -> yaml:
        logging.info("Entered the write_json_to_yaml_file method of MainUtils class")
        try:
            data = json_file
            stream = open(yaml_file_path, "w")
            yaml.dump(data, stream)

        except Exception as e:
            raise ShippingException(e, sys)
        

    def save_numpy_array_data(self, file_path: str, array: np.array):
        logging.info("Entered the save_numpy_array_data method of MainUtils class")
        try:
            with open(file_path, "wb") as file_obj:
                np.save(file_obj, array)
            logging.info("Exited the save_numpy_array_data method of MainUtils class")
            return file_path

        except Exception as e:
            raise ShippingException(e, sys)
        


    @staticmethod
    def save_object(file_path: str, obj: object) -> None:
        logging.info("Entered the save_object method of MainUtils class")
        try:
            with open(file_path, "wb") as file_obj:
                dill.dump(obj, file_obj)

            logging.info("Exited the save_object method of MainUtils class")

            return file_path

        except Exception as e:
            raise ShippingException(e, sys)