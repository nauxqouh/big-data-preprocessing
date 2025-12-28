from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round

# Database connection properties
db_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

jdbc_url = "jdbc:postgresql://postgres-db:5432/analytics"

# Create Spark session
spark = SparkSession.builder \
    .appName("Lesson6Assignment") \
    .master("spark://spark-master:7077") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

print("\\n=== READING DATA FROM POSTGRESQL ===")

# Read customers table
customers_df = spark.read.jdbc(
    url=jdbc_url,
    table="customers",
    properties=db_properties
)