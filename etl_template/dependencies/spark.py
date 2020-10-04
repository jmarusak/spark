from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

def start_spark(app_name='my_spark_app', master='local[*]', 
               jar_packages=[], files=[], spark_config={}):

    spark_builder = SparkSession.builder.appName(app_name)

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    spark_sess = spark_builder.getOrCreate()

    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(spark_files_dir) if filename.endswith('config.json')]

    if config_files:
        

    return spark_sess, config_dict