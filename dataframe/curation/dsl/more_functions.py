from pyspark.sql import SparkSession
from pyspark.sql.functions import first,trim,lower,ltrim,initcap,format_string,coalesce,lit,col
from model.Person import Person

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    people_df = spark.createDataFrame([
        Person("Sidhartha", "Ray", 32, None, "Programmer"),
        Person("Pratik", "Solanki", 22, 176.7, None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, 125.3, "Chemical Engineer"),
        Person("Pratik", "Solanki", 22, 222.2, "Teacher")
    ])

    people_df.show()
    people_df.groupBy("firstName").agg(first("weightInLbs")).show()
    people_df.groupBy(trim(lower(col('firstName')))).agg(first("weightInLbs")).show()
    people_df.groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    people_df.sort(col("weightInLbs").desc()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    people_df.sort(col("weightInLbs").asc_nulls_last()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()

    corrected_people_df = people_df\
        .withColumn("firstName", initcap("firstName"))\
        .withColumn("firstName", ltrim(initcap("firstName")))\
        .withColumn("firstName", trim(initcap("firstName")))\

    corrected_people_df.groupBy("firstName").agg(first("weightInLbs")).show()

    corrected_people_df = corrected_people_df\
        .withColumn("fullName", format_string("%s %s", "firstName", "lastName"))\

    corrected_people_df.show()

    corrected_people_df = corrected_people_df\
        .withColumn("weightInLbs", coalesce("weightInLbs", lit(0)))\

    corrected_people_df.show()

    corrected_people_df\
        .filter(lower(col("jobType")).contains("engineer"))\
        .show()

    # List
    corrected_people_df \
        .filter(lower(col("jobType")).isin(["chemical engineer", "abc", "teacher"])) \
        .show()

    # Without List
    corrected_people_df\
        .filter(lower(col("jobType")).isin("chemical engineer", "teacher"))\
        .show()

    # Exclusion
    corrected_people_df \
        .filter(~lower(col("jobType")).isin("chemical engineer", "teacher")) \
        .show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/more_functions.py
