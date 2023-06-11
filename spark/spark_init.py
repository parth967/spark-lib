from abc import ABC
import os
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class SparkInitialization(ABC):
    def __init__(self) -> None:
        self.spark_conf_file = './spark/spark_conf.json'
        self.spark_conf = self._get_spark_conf(self.spark_conf_file) 

    def _get_spark_conf(self, json_path):
        with open(json_path, '+r') as conf:
            return json.load(conf)
        
    def _init_spark(self, extra_conf = None):

        #will replace this default config to set default while submiting spark 
        spark =  spark = SparkSession.builder \
                            .master("local[*]") \
                            .enableHiveSupport() \
                            .appName(self.spark_conf.get("applicationName")) \
                            .config("spark.sql.warehouse.dir","/Users/parthpatel/Applications/Databricks_app") \
                            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                            .getOrCreate()

        if extra_conf and isinstance(extra_conf, 'dict'):
             for key, value in extra_conf.items():
                    spark.conf.set(key, value)

        return spark
        


