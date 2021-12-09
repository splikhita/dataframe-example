from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml
import sys

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    demographics_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")
    finances_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")
    coursesRDD = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/course.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))) \
        .filter(lambda rec: rec[1][2] == "Switzerland")

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4])))) \
        .filter(lambda r: r[1][0] and r[1][1])

    print('Participants belongs to \'Switzerland\', having debts and financial dependents,')
    join_pair_rdd = demographics_pair_rdd.join(finances_pair_rdd)

    join_pair_rdd.foreach(print)

    print('Participants belongs to \'Switzerland\', having debts, financial dependents and the course enrolled,')
    courses_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/course.csv")
    courses_pair_rdd = courses_rdd.map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), lst[1])) \

    join_pair_rdd = join_pair_rdd \
        .map(lambda rec: (rec[1][0][6], (rec[0], rec[1][0], rec[1][1]))) \
        .join(courses_pair_rdd) \
        .map(lambda rec: (rec[1][0][0], (rec[1][0][1], rec[1][0][2], rec[1][1])))

    join_pair_rdd.foreach(print)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/scholaship_recipient_filter_join.py
