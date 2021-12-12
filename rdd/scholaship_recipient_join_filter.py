from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ == '__main__':
    #Setting up the configuration. To read data from S3 bucket we have a library org.apache.hadoop:hadoop-aws:2.7.4
    #pyspark-shell will run the code locally
    #If we have all the configurations already setup on local computer then we can directly right click here and run the code. The below config setup is not needed in that case.
    #If we are running it on EMR, even in that case we don't need the below config. We can just move the code to EMR and run it there
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession object. SparkSession is used to create dataframes, SparkContext is used to create RDD's to read data from different data sources and create dataframes and RDD's
    spark = SparkSession \
        .builder \
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()
    #Setting the log level to ERROR which shows only Error logs if there are any
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    #We have 2 configuration files below
    #application.yml has application level configurations. Ex: s3 bucket config, database config etc.,
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    #.secrets has system level configurations. '.' files will not appear in git because this file has passwords. we dont want these files to be pushed to git
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    #Opening the config files
    conf = open(app_config_path)
    #Reading config files. FullLoader loads teh entire file
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    #Through hadoop we setup the access and secret keys
    #spark-object has the attribute sparkcontext
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    #Reading the csv files from S3
    demographics_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")
    finances_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")
    #convert to pair rdd
    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))) #converted rdd of string to rdd of respective data types and converted to key value pairs where student id is key and rest are values
    #converting the finances.rdd to pair rdd same as above
    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4])))) #student id is key at 0th index and the rest are the values

    print('Participants belongs to \'Switzerland\', having debts and financial dependents,')
    #joining RDD's, when we join the RDD's the data will be converted to nested tuples where Key (student id on both RDD's) will be at 0 index and values from demographics will be at 0 index in values and values from finances will be at 1st index of values
    #We use indexes to access the values from the key value pair that we want to use in filters
    join_pair_rdd = demographics_pair_rdd \
        .join(finances_pair_rdd) \
        .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][1][0] == 1) and (rec[1][1][1] == 1)) \

    join_pair_rdd.foreach(print)

#The below command is used to run the python file
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/scholaship_recipient_join_filter.py
