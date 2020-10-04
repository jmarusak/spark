#exec(open("./jobs/etl_job_template.py").read())

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws

import sys
sys.path.append('.')

from dependencies import logging

def extract_data(spark):
    """Load data from Parquet file
    :param spark: Spark session
    :return: Spark Data Frame
    """
    
    df = spark.read.parquet('../../../pyspark/data/etl_template/employees')
    return df

def transform_data(df):
    """Transform original dataset.
    :param df: Input DataFrame
    :return: Transformed DataFrame
    """
    df_transformed = df.select(col('id'),
                               concat_ws(' ', col('first_name'), col('last_name')).alias('name'),
                               (col('floor') * lit(10)).alias('steps_to_desk'),
                              )

    return df_transformed

def load_data(df):
    """Collect data locally and write to CSV
    :param df: DataFrame to export
    :return: None
    """
    df.coalesce(1).write.mode('overwrite').csv('../../../pyspark/data/etl_template/employees_transformed')

    return None

def create_test_data(spark, config):
    """Create test data.

    : return: None
    """

    # sample records
    local_records = [
        Row(id=1, first_name='Dan', last_name='Germain', floor=1),
        Row(id=2, first_name='Dan', last_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', last_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', last_name='Lai', floor=2),
        Row(id=5, first_name='Stu', last_name='White', floor=3),
        Row(id=6, first_name='Mark', last_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', last_name='Bird', floor=4),
        Row(id=8, first_name='Kim', last_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)
    df.coalesce(1).write.parquet('../../../pyspark/data/etl_template/employees', mode='overwrite')

    return None

    
def main():
    spark = SparkSession.builder.getOrCreate()
    spark_logger = logging.Log4j(spark)
        
    create_test_data(spark, None)

    spark_logger.warn('ETL_TEMPLATE - Extracting data ...')
    data = extract_data(spark)
    
    spark_logger.warn('ETL_TEMPLATE - Transforming data ...')
    data_transformed = transform_data(data)

    spark_logger.warn('ETL_TEMPLATE - Loading data ...')  
    load_data(data_transformed)
    
    spark.stop()
    return None

if __name__ == '__main__':
    main()
