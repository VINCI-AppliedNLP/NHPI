import os, sys
from pyspark.sql import SparkSession

def spark_start(appname):
							   

	spark = SparkSession.builder.appName(appname).master("local[32]")\
		.config("spark.driver.memory","1000G")\
		.config("spark.executor.memory","500G")\
		.config("spark.sql.execution.arrow.pyspark.enabled","true")\
		.config("spark.task.maxFailures",'10')\
		.config('spark.sql.parquet.compression.codec', 'gzip') \
		.config('spark.executor.heartbeatInterval','9500s')\
		.config('spark.network.timeout','9600s')\
		.config('spark.driver.maxResultSize','150g') \
		.getOrCreate()

	spark.conf.set("spark.sql.parser.escapedStringLiterals","true")
	
	return spark