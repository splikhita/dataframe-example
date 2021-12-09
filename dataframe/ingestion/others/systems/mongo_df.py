from pyspark.sql import SparkSession
import yaml
import os.path

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
         '--packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" pyspark-shell'
    )
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    students = spark\
        .read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database", app_conf["mongodb_config"]["database"])\
        .option("collection", app_conf["mongodb_config"]["collection"])\
        .load()

    students.show()

# spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" dataframe/ingestion/others/systems/mongo_df.py
