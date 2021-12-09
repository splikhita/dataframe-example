from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    def initcap(line: str):
        lst = line.split(" ")
        return ' '.join(list(map(str.capitalize, lst)))

    sampleDf = spark\
        .createDataFrame(
            [(1, "This is some sample data"),
             (2, "and even more.")]
        ).toDF("id", "text")

    # Register: Method1
    initcap_udf1 = spark.udf\
        .register("initcap", initcap, StringType())

    # Register: Method2
    initcap_udf2 = udf(initcap, StringType())

    # Register: Method3
    initcap_udf3 = spark.udf\
        .register("initcap", lambda rec: ' '.join(list(map(str.capitalize, rec.split(" ")))), StringType())

    # Register: Method4
    initcap_udf4 = spark.udf \
        .register("initcap", lambda line, delimiter: ' '.join(list(map(str.capitalize, line.split(delimiter)))), StringType())

    sampleDf.select("id",
                    initcap_udf1("text").alias("text1"),
                    initcap_udf2("text").alias("text2"),
                    initcap_udf3("text").alias("text3"),
                    initcap_udf4("text", lit(" ")).alias("text4")
                    )\
        .show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/udf_demo.py
