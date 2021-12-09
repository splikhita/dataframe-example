from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, sum
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .master('local[*]') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-avro_2.11:2.4.5') \
        .getOrCreate()

    # Reading without com.databricks.spark.avro
    # Apache Avro as a Built-in Data Source in Apache Spark 2.4

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file_path = os.path.abspath(current_dir + "/../../../" + "application.yml")

    with open(app_config_file_path) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("\nCreating DF ingestion csv and write as avro 'SparkSession.read.format(csv)")
    fin_schema = StructType()\
        .add("id", IntegerType(), True)\
        .add("has_debt", BooleanType(), True)\
        .add("has_financial_dependents", BooleanType(), True)\
        .add("has_student_loans", BooleanType(), True)\
        .add("income", DoubleType(), True)

    print("\n Check Pushdown filter for CSV")
    fin_df = spark.read\
        .option("header", "false")\
        .option("delimiter", ",")\
        .format("csv")\
        .schema(fin_schema)\
        .load("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv")

    csv_explain_plan = fin_df.select("id", "income").filter("has_debt=true")
    print(csv_explain_plan.explain()) # explain(True)

    fin_df\
        .write\
        .format("avro")\
        .mode("overwrite")\
        .save("s3a://"+doc["s3_conf"]["s3_bucket"] + "/finAvro")

    print("\n Check Pushdown filter for Avro")
    finAvroDF = spark\
        .read\
        .format("avro")\
        .load("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finAvro")

    avroExplianPlan = finAvroDF.select("id","income").filter("has_debt=true")
    print(avroExplianPlan.explain()) # explain(True)
    finAvroDF.printSchema()

    finAvroDF\
        .withColumn("ranked",row_number().over(Window.partitionBy("has_debt").orderBy(col("income").desc())))\
        .withColumn("windowedSum",sum("income").over(Window.partitionBy("has_debt")))\
        .withColumn("windowedRunningTotal",sum("income").over(Window.partitionBy("has_debt").orderBy(col("income").desc())))\
        .show(5,False)
