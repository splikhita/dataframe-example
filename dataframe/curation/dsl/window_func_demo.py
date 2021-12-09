from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from model.Product import Product
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

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

    finFilePath = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    financeDf = spark.read.parquet(finFilePath)
    financeDf.printSchema()

    accNumPrev4WindowSpec = Window.partitionBy("AccountNumber")\
        .orderBy("Date")\
        .rowsBetween(-4, 0)  # takes the first first 5 rows to window aggregation

    financeDf\
        .withColumn("Date", to_date(from_unixtime(unix_timestamp("Date", "MM/dd/yyyy"))))\
        .withColumn("RollingAvg", avg("Amount").over(accNumPrev4WindowSpec))\
        .show(20, False)

    productList = [
        Product("Thin", "Cell phone", 6000),
        Product("Normal", "Tablet", 1500),
        Product("Mini", "Tablet", 5500),
        Product("Ultra Thin", "Cell phone", 5000),
        Product("Very Thin", "Cell phone", 6000),
        Product("Big", "Tablet", 2500),
        Product("Bendable", "Cell phone", 3000),
        Product("Foldable", "Cell phone", 3000),
        Product("Pro", "Tablet", 4500),
        Product("Pro2", "Tablet", 6500)
    ]

    products = spark.createDataFrame(productList)
    products.printSchema()

    catRevenueWindowSpec = Window.partitionBy("category")\
        .orderBy("revenue")

    products \
        .select("product",
                "category",
                "revenue",
                lag("revenue", 1).over(catRevenueWindowSpec).alias("prevRevenue"),
                lag("revenue", 2, 0).over(catRevenueWindowSpec).alias("prev2Revenue"),
                row_number().over(catRevenueWindowSpec).alias("row_number"),
                rank().over(catRevenueWindowSpec).alias("rev_rank"),
                dense_rank().over(catRevenueWindowSpec).alias("rev_dense_rank")) \
        .show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/window_func_demo.py
