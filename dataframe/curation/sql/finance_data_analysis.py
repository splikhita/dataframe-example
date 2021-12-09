from pyspark.sql import SparkSession
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
    app_config_path = os.path.abspath(current_dir + "/../../../"+"application.yml")
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
    finance_df = spark.sql("select * from parquet.`{}`".format(fin_file_path))

    finance_df.printSchema()
    finance_df.show(5, False)
    finance_df.createOrReplaceTempView("finances")

    spark.sql("select * from finances order by amount").show(5, False)

    spark.sql("select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances").show(5, False)

    agg_finance_df = spark.sql("""
        select
            AccountNumber,
            sum(Amount) as TotalTransaction,
            count(Amount) as NumberOfTransaction,
            max(Amount) as MaxTransaction,
            min(Amount) as MinTransaction,
            collect_set(Description) as UniqueTransactionDescriptions
        from
            finances
        group by
            AccountNumber
        """)

    agg_finance_df.show(5, False)
    agg_finance_df.createOrReplaceTempView("agg_finances")

    spark.sql(app_conf["spark_sql_demo"]["agg_demo"]) \
        .show(5, False)

    companies_df = spark.read.json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")
    companies_df.createOrReplaceTempView("companies")
    companies_df.show(5, False)
    companies_df.printSchema()

    employee_df_temp = spark.sql("select company, explode(employees) as employee from companies")
    employee_df_temp.show()
    employee_df_temp.createOrReplaceTempView("employees")
    spark.sql("select company, posexplode(employees) as (employeePosition, employee) from companies") \
        .show()

    spark.sql(app_conf["spark_sql_demo"]["case_when_demo"]) \
        .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/finance_data_analysis.py
