"""
IN: s3://spark-job-cluster-bucket/input/customers.csv
OP: s3://spark-job-cluster-bucket/output/

spark-submit --deploy-mode cluster --master yarn script.py s3://spark-job-cluster-bucket/input/customers.csv s3://spark-job-cluster-bucket/output/
"""

from __future__ import print_function
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
        
    with SparkSession.builder.appName("My EMR Job").enableHiveSupport().getOrCreate() as spark:
    
        df = spark.read.csv(sys.argv[1], header=True)
    
        table_name = "trade_sample_table_orc_ext_emr"
    
        df.write.mode("OVERWRITE").option("path", sys.argv[2]).format("orc").saveAsTable(table_name)
    
        