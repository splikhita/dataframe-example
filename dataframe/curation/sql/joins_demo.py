from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from model.Role import Role
from model.Employee import Employee

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    emp_df = spark.createDataFrame([
        Employee(1, "Sidhartha", "Ray"),
        Employee(2, "Pratik", "Solanki"),
        Employee(3, "Ashok", "Pradhan"),
        Employee(4, "Rohit", "Bhangur"),
        Employee(5, "Kranti", "Meshram"),
        Employee(7, "Ravi", "Kiran")
    ])
    emp_df.createOrReplaceTempView("emp")

    role_df = spark.createDataFrame([
        Role(1, "Architect"),
        Role(2, "Programmer"),
        Role(3, "Analyst"),
        Role(4, "Programmer"),
        Role(5, "Architect"),
        Role(6, "CEO")
    ])
    role_df.createOrReplaceTempView("role")

    spark.sql('select a.*, b.* from emp a join role b on a.id = b.id').show(5, False)

    spark.sql("select /*BROADCAST(b)*/ a.*, b.* from emp a join role b on a.id = b.id").show()
    # Join Types: "left_outer"/"left", "full_outer"/"full"/"outer"
    spark.sql("select a.*, b.* from emp a inner join role b on a.id = b.id").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "right_outer").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "left_anti").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "full").show()

    # cross join
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "cross").show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/joins_demo.py
