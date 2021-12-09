from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, date_format, expr
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("Read Files") \
        .master('local[*]') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    # write into NYC_OMO_YEAR_WISE
    dfFromParquet=sparkSession\
        .read\
        .format("parquet")\
        .load("s3a://" + doc["s3_conf"]["s3_bucket"] + "/NYC_OMO")\
        .withColumn("OrderYear", date_format("OMOCreateDate", "YYYY"))\
        .repartition(5)

    dfFromParquet.printSchema()
    dfFromParquet.show(5, False)

    dfFromParquet\
        .write\
        .partitionBy("OrderYear")\
        .mode("overwrite")\
        .parquet("s3a://" + doc["s3_conf"]["s3_bucket"] + "/NYC_OMO_YEAR_WISE")

    nycOmoDf = sparkSession.read\
        .parquet("s3a://" + doc["s3_conf"]["s3_bucket"] + "/NYC_OMO_YEAR_WISE")\
        .repartition(5)

    parquetExplianPlan = nycOmoDf \
        .select("OMOID", "OMONumber", "BuildingID") \
        .filter((col("OrderYear") == "2018") & (col("Lot") > "50"))

    print("spark.sql.parquet.filterPushdown:", sparkSession.conf.get("spark.sql.parquet.filterPushdown"))
    print("spark.sql.parquet.mergeSchema:", sparkSession.conf.get("spark.sql.parquet.mergeSchema"))

    parquetExplianPlan.explain()

    # turn on Parquet push-down, stats filtering, and dictionary filtering
    sparkSession.conf.set('spark.sql.parquet.filterPushdown', "true")
    print("spark.sql.parquet.filterPushdown", sparkSession.conf.get("spark.sql.parquet.filterPushdown"))
    sparkSession.conf.set('parquet.filter.statistics.enabled', "true")
    sparkSession.conf.set('parquet.filter.dictionary.enabled', "true")

    #use the non-Hive read path
    sparkSession.conf.set("spark.sql.hive.convertMetastoreParquet", "true")

    # turn off schema merging, which turns off push-down
    sparkSession.conf.set("spark.sql.parquet.mergeSchema", "false")
    sparkSession.conf.set("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")

    parquetExplianPlan1 =nycOmoDf \
        .select("OMOID", "OMONumber", "BuildingID") \
        .filter((col("OrderYear") == "2018") & (col("Lot") > "50"))

    parquetExplianPlan1.explain()
