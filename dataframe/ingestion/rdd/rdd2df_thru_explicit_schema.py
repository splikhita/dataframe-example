from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import unix_timestamp, approx_count_distinct, sum
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType
import os.path
import yaml
import sys


if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nConvert RDD to Dataframe using SparkSession.createDataframe(),")
    # Creating RDD of Row
    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record: record.find("txn_id")) \
        .map(lambda record: record.split("|")) \
        .map(lambda record: Row(int(record[0]), int(record[1]), float(record[2]), int(record[3]), int(record[4]), int(record[5]), record[6]))
        # RDD[Row[Long, Long, Double, Long, Int, Long, String]]

    # Creating the schema
    txn_fct_schema = StructType([
        StructField("txn_id", LongType(), False),
        StructField("created_time_str", LongType(), False),
        StructField("amount", DoubleType(), True),
        StructField("cust_id", LongType(), True),
        StructField("status", IntegerType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("created_time_ist", StringType(), True)
        ])

    txn_fct_df = spark.createDataFrame(txn_fct_rdd, txn_fct_schema)
    txn_fct_df.printSchema()
    txn_fct_df.show(5, False)

    # Applying transformation on dataframe using DSL (Domain Specific Language)
    txn_fct_df = txn_fct_df \
        .withColumn("created_time_ist", unix_timestamp(txn_fct_df["created_time_ist"], "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

    txn_fct_df.printSchema()
    txn_fct_df.show(5, False)

    print("# of records = " + str(txn_fct_df.count()))
    print("# of merchants = " + str(txn_fct_df.select(txn_fct_df["merchant_id"]).distinct().count()))

    txnAggDf = txn_fct_df \
        .repartition(10, txn_fct_df["merchant_id"]) \
        .groupBy("merchant_id") \
        .agg(sum("amount"), approx_count_distinct("status"))

    txnAggDf.show(5, False)

    txnAggDf = txnAggDf \
        .withColumnRenamed("sum(amount)", "total_amount") \
        .withColumnRenamed("approx_count_distinct(status)", "dist_status_count")

    txnAggDf.show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/rdd/rdd2df_thru_explicit_schema.py
