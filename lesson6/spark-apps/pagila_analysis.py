from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round, max as _max, min as _min, \
    stddev, percentile_approx, \
    date_format, datediff,  \
    countDistinct, concat_ws, \
    row_number
from pyspark.sql.window import Window

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

print("\\n=== READING PAGILA DATA FROM POSTGRESQL ===")

# Read all tables needed
df_film = spark.read.jdbc(url=jdbc_url, table="film", properties=db_properties)
df_film_category = spark.read.jdbc(url=jdbc_url, table="film_category", properties=db_properties)
df_category = spark.read.jdbc(url=jdbc_url, table="category", properties=db_properties)
df_inventory = spark.read.jdbc(url=jdbc_url, table="inventory", properties=db_properties)
df_rental = spark.read.jdbc(url=jdbc_url, table="rental", properties=db_properties)
df_payment = spark.read.jdbc(url=jdbc_url, table="payment", properties=db_properties)
df_store = spark.read.jdbc(url=jdbc_url, table="store", properties=db_properties)
df_customer = spark.read.jdbc(url=jdbc_url, table="customer", properties=db_properties)

print("\\n=== All tables loaded! ===")


print("\\n=== EXERCISE 1. Monthly Revenue by Film Category")

monthly_revenue_by_category = df_payment \
    .join(df_rental, "rental_id") \
    .join(df_inventory, "inventory_id") \
    .join(df_film, "film_id") \
    .join(df_film_category, "film_id") \
    .join(df_category, "category_id") \
    .withColumn("year_month", date_format(col("payment_date"), "yyyy-MM")) \
    .groupBy("year_month", col("name").alias("category_name")) \
    .agg(
        _sum("amount").alias("total_revenue")
    ) \
    .orderBy("year_month", col("total_revenue").desc())

# Show result
print("\\nMonthly Revenue by Category:")
monthly_revenue_by_category.show(truncate=False)

pivot_revenue = (
    monthly_revenue_by_category
    .groupBy("category_name")      
    .pivot("year_month")           
    .agg(_round(_sum("total_revenue"), 2))
    .orderBy("category_name")
)
    
print("\\n Pivot View (Month x Category):")
pivot_revenue.show(truncate=False)


print("\\n=== EXERCISE 2. Customer Lifetime Value")

clv_summary = df_payment \
    .join(df_customer, "customer_id") \
    .groupBy(
        "customer_id",
        concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name")
    ) \
    .agg(
        # Total Revenue (CLV)
        _sum("amount").alias("clv"),
        # Total Transactions
        count("payment_id").alias("total_transactions"),
        # Number of active months
        countDistinct(date_format(col("payment_date"), "yyyy-MM")).alias("total_active_months"),
        # Average revenue of each transaction value
        _round(avg("amount"), 2).alias("avg_transaction_value"),
        # Tenure days
        datediff(_max("payment_date"), _min("payment_date")).alias("tenure_days"),
        # _max("payment_date").alias("last_purchase_date"),
        # _min("payment_date").alias("first_purchase_date")
    ) \
    .withColumn(
        "avg_monthly_revenue",
        _round(col("clv")/col("total_active_months"), 2)
    ) \
    .orderBy(col("clv").desc())

# Show result
print("\\n Top 20 Customers by CLV:")
clv_summary.show(20, truncate=False)

# Statistic Summary
print("\\n CLV Statistic Summary.")
clv_summary.select(
    _round(avg("clv"), 2).alias("avg_clv"),
    _round(stddev("clv"), 2).alias("stddev_clv"),
    _round(_min("clv"), 2).alias("min_clv"),
    _round(_max("clv"), 2).alias("max_clv"),
    _round(percentile_approx("clv", 0.25), 2).alias("25th_percentile_clv"),
    _round(percentile_approx("clv", 0.5), 2).alias("median_clv"),
    _round(percentile_approx("clv", 0.75), 2).alias("75th_percentile_clv")
).show()


print("\\n=== EXERCISE 3. Top 1% of Customers Generating 80% of Revenue")

# Compute total revenue
total_revenue = clv_summary.agg(_sum("clv")).first()[0]
# Compute total customers
total_customers = clv_summary.count()

# Compute cumulative revenue
windowSpec = Window.orderBy(col("clv").desc())

# Pareto
## Ranking customer by clv
## Cumulative revenue from customer #1 to present
## % revenue at this present row
## % number of customers at this present row
customer_pareto = clv_summary \
    .withColumn("row_num", row_number().over(windowSpec)) \
    .withColumn("cumulative_revenue", _sum("clv").over(windowSpec.rowsBetween(Window.unboundedPreceding, 0))) \
    .withColumn("pct_revenue", col("cumulative_revenue") / total_revenue * 100) \
    .withColumn("pct_customer", col("row_num") / total_customers * 100)

customer_pareto.show(100, truncate=False)

# PART 1 — Top 1% customers generate how much revenue?
top1pct_cutoff = int(total_customers * 0.01)

top1pct_customer_revenue = customer_pareto \
    .filter(col("row_num") <= top1pct_cutoff) \
    .agg(_max("pct_revenue").alias("top1pct_customer_revenue")) \
    .collect()[0]["top1pct_customer_revenue"]

top1pct_customer = customer_pareto \
    .filter(col("row_num") <= top1pct_cutoff) \
    .select("customer_id", "customer_name", "clv", "total_transactions", "pct_revenue")

# PART 2 — How many customers are needed to reach 80% revenue?
pct_customer_gen_80pct_revenue = customer_pareto \
    .filter(col("pct_revenue") >= 80) \
    .agg(_max("pct_customer").alias("pct_customer_gen_80pct_revenue")) \
    .collect()[0]["pct_customer_gen_80pct_revenue"]


# Summary Result
print(f"Total Revenue: ${total_revenue}")
print(f"Total Customers: {total_customers} customers")
print(f"\n1. Top 1% customers generate approximately {top1pct_customer_revenue}% of total revenue.")
print("\\n== Top 1% Customers:")
top1pct_customer.show(20, truncate=False)
print(f"\n2. To generate 80% of revenue, approximately {pct_customer_gen_80pct_revenue}% of customers are required.")

