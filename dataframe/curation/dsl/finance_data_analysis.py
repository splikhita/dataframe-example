from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

    fin_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    finance_df = spark.read.parquet(fin_file_path)

    finance_df.printSchema()
    finance_df.show(5, False)

    finance_df\
        .orderBy("Amount")\
        .show(5)

    # concat_ws function available sql.functions
    finance_df\
        .select(concat_ws(" - ", "AccountNumber", "Description").alias("AccountDetails"))\
        .show(5, False)

    finance_df\
        .withColumn("AccountDetails", concat_ws(" - ", "AccountNumber", "Description"))\
        .show(5, False)

    agg_finance_df = finance_df\
        .groupBy("AccountNumber")\
        .agg(avg("Amount").alias("AverageTransaction"),
             sum("Amount").alias("TotalTransaction"),
             count("Amount").alias("NumberOfTransaction"),
             max("Amount").alias("MaxTransaction"),
             min("Amount").alias("MinTransaction"),
             collect_set("Description").alias("UniqueTransactionDescriptions")
        )

    agg_finance_df.show(5, False)

    agg_finance_df\
        .select("AccountNumber",
                "UniqueTransactionDescriptions",
                size("UniqueTransactionDescriptions").alias("CountOfUniqueTransactionTypes"),
                sort_array("UniqueTransactionDescriptions", False).alias("OrderedUniqueTransactionDescriptions"),
                array_contains("UniqueTransactionDescriptions", "Movies").alias("WentToMovies"))\
        .show(5, False)

    companies_df = spark.read.json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")
    print("Count = ", companies_df.count())
    companies_df.show(5, False)
    companies_df.printSchema()

    employee_df_temp = companies_df \
        .select("company", explode("employees").alias("employee"))
    employee_df_temp.show()
    companies_df \
        .select("company", posexplode("employees").alias("employeePosition", "employee")) \
        .show()
    employeeDf = employee_df_temp.select("company", expr("employee.firstName as firstName"))
    employeeDf.select("*",
                      when(col("company") == "FamilyCo", "Premium")
                      .when(col("company") == "OldCo", "Legacy")
                      .otherwise("Standard").alias("Tier"))\
                .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/finance_data_analysis.py
