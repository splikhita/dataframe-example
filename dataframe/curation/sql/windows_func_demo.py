from pyspark.sql import SparkSession, Row, Window
import os.path
import yaml
from model.Product import Product

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

    finFilePath = "s3a://" + app_conf["s3_conf"]["s3_bucket"]+"/finances-small"
    financeDf = spark.read.parquet(finFilePath)
    financeDf.createOrReplaceTempView("raw_finances")

    spark.sql("""
          select
            AccountNumber,
            Amount,
            to_date(cast(unix_timestamp(Date, 'MM/dd/yyyy') as timestamp)) as Date,
            Description
          from
            raw_finances
          """)\
        .createOrReplaceTempView("finances")

    financeDf.printSchema()

    spark.sql("""
          select
            AccountNumber,
            Amount,
            Date,
            Description,
            avg(Amount) over (partition by AccountNumber order by Date rows between 4 preceding and 0 following) as RollingAvg
          from
            finances
          """)\
        .show(5, False)

    # Row can be used as case class in pyspark
    productList = [
        Row(product="Thin",category= "Cell phone",revenue=6000),
        Row(product="Normal", category= "Tablet", revenue=1500),
        Row(product="Mini", category= "Tablet", revenue=5500),
        Row(product="Ultra Thin", category= "Cell phone",revenue= 5000),
        Row(product="Very Thin", category= "Cell phone", revenue=6000),
        Row(product="Big", category= "Tablet", revenue=2500),
        Row(product="Bendable", category= "Cell phone", revenue=3000),
        Row(product="Foldable", category= "Cell phone", revenue=3000),
        Row(product="Pro", category= "Tablet", revenue=4500),
        Row(product="Pro2", category= "Tablet",revenue= 6500)
                  ]

    productList1= [
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
    products = spark.createDataFrame(productList1)
    products.createOrReplaceTempView("products")
    products.printSchema()

    catRevenueWindowSpec = Window.partitionBy("category")\
        .orderBy("revenue")

    spark.sql("""
            select
              product,
              category,
              revenue,
              lag(revenue, 1) over (partition by category order by revenue) as prevRevenue,
              lag(revenue, 2, 0) over(partition by category order by revenue) as prev2Revenue,
              row_number() over (partition by category order by revenue) as row_number,
              rank() over(partition by category order by revenue) as rev_rank,
              dense_rank() over(partition by category order by revenue) as rev_dense_rank
             from
              products
          """)\
        .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/windows_func_demo.py
