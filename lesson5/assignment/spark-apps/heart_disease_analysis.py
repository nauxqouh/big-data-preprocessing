from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, round as _round, when, rank
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from scipy.stats import chi2_contingency



# Spark Session
spark = SparkSession.builder \
    .appName("HeartDiseaseAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("sex", IntegerType(), True),
    StructField("cp", IntegerType(), True),
    StructField("trestbps", IntegerType(), True),
    StructField("chol", IntegerType(), True),
    StructField("fbs", IntegerType(), True),
    StructField("restecg", IntegerType(), True),
    StructField("thalach", IntegerType(), True),
    StructField("exang", IntegerType(), True),
    StructField("oldpeak", DoubleType(), True),
    StructField("slope", IntegerType(), True),
    StructField("ca", IntegerType(), True),
    StructField("thal", IntegerType(), True),
    StructField("target", IntegerType(), True)
])

# Read CSV data
print("Reading heart data...")
heart_df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/opt/spark-data/input/heart.csv")

# Preprocessing
## Drop duplicates
heart_df = heart_df.dropDuplicates()

## Rename df columns
print("Rename dataframe columns...")
rename_map = { 
    'cp': 'angina', 
    'trestbps': 'systolic', 
    'chol': 'cholesterol',
    'fbs': 'diabetes',
    'restecg': 'electrocardiogram',
    'thalach': 'max_heart_rate',
    'exang': 'exercise_induced_angina',
    'oldpeak': 'ST_depression',
    'slope': 'ST_slope',
    'ca': 'number_of_major_vessels',
    'thal': 'thalassemia',
    'target': 'heart_disease'
}

heart_df = heart_df.select([ 
    col(c).alias(rename_map.get(c, c)) 
    for c in heart_df.columns
])

## Filter error records
heart_df = heart_df.where("number_of_major_vessels < 4")
heart_df.show(10)

# Exercise
# 1. What is the most common chest pain type by age group?
print("\\n=== CHEST PAIN TYPE BY AGE GROUP ANALYSIS ===")
## Create age group and map angina column
age_group = heart_df \
    .withColumn(
        "age_group",
        when((col("age") > 28) & (col("age") <= 37), "29-37")
        .when((col("age") > 37) & (col("age") <= 45), "38-45")
        .when((col("age") > 45) & (col("age") <= 53), "46-53")
        .when((col("age") > 53) & (col("age") <= 61), "54-61")
        .when((col("age") > 61) & (col("age") <= 69), "62-69")
        .when((col("age") > 69) & (col("age") <= 77), "70-77")
    ) \
    .withColumn(
        "angina",
        when(col("angina") == 0, "Typical Angina")
        .when(col("angina") == 1, "Atypical Angina")
        .when(col("angina") == 2, "Non-anginal Pain")
        .when(col("angina") == 3, "Asymptomatic") 
    ) \
    .select("age_group", "angina")

## Group by age_group and angina
chest_pain_age_group = age_group.groupBy("age_group", "angina") \
    .agg(
        count("*").alias("counts")
    ) \
    .fillna(0)

print("\\n=== PIVOT TABLE WITH RANK ===")
## Create ranking columns through window partition
window = Window.partitionBy("age_group") \
    .orderBy(col("counts").desc())
chest_pain_age_group = chest_pain_age_group \
    .withColumn("rank", rank().over(window)) \
    .orderBy("age_group", "rank")
## Pivot table
pivot_table = chest_pain_age_group \
    .groupBy("age_group") \
    .pivot("angina") \
    .agg(_sum("rank")) \
    .orderBy("age_group")
pivot_table.show(truncate=False)

print("\\n=== THE MOST COMMON CHEST PAIN TYPE BY AGE GROUP ===")
most_common_cp = chest_pain_age_group.filter(col("rank") == 1) \
    .select("age_group", "angina") \
    .withColumnRenamed("angina", "most_common_angina")
most_common_cp.show(truncate=False)

# 2. Explore the effect of diabetes on heart disease.
print("\\n=== Explore the effect of diabetes on heart disease ===")
diabetes = heart_df.withColumn(
        "diabetes",
        when(col("diabetes") == 1, "Diabetes")
        .when(col("diabetes") == 0, "No Diabetes")
    ) \
    .select("diabetes", "heart_disease")
    
## Calculate disease rate by diabetes
disease_rate = diabetes.groupBy("diabetes") \
    .agg(
        _round(avg(col("heart_disease")) * 100).alias("pct_heart_disease (%)")
    )
disease_rate.show(truncate=False)

## Chi-square test
print("\\n=== Chi-square test for diabetes and heart disease ===")
diabetes = diabetes.withColumn(
        "heart_disease",
        when(col("heart_disease") == 1, "True")
        .when(col("heart_disease") == 0, "False")
    )

print("\\n=== CONTINGENCY TABLE ===")
contingency_table = diabetes.groupBy("diabetes", "heart_disease") \
    .agg(
        count("*").alias("observed")
    ) 
contingency_table.show(truncate=False)

contingency_table = contingency_table.toPandas() \
    .pivot(
        index='diabetes', 
        columns='heart_disease', 
        values='observed'
    ) \
    .fillna(0)

chi2, p_value, dof, expected = chi2_contingency(contingency_table.values)

print(f'Chi-square test of independence: {chi2}')
print(f'p-value: {p_value}')
print(f'Degrees of freedom: {dof}')
print(f'Expected frequencies: {expected}')
print(f'p-value < 0.05: {p_value < 0.05}')

if p_value < 0.05:
    print(">> Reject null hypothesis.\nConclusion: With p_value = 0.05, there is no association between diabetes and heart disease rate..")
else:
    print(">> Fail to reject null hypothesis.\nConclusion: With p_value = 0.05, the data is not reliable and the association between diabetes and heart disease rate is not significant.")

# 3. Explore the effect of hypertension on heart disease.
print("\\n=== Explore the effect of hypertension on heart disease ===")

hypertension = heart_df.withColumn(
        "hypertension",
        when(col("systolic") > 130, "Hypertension")
        .otherwise("No Hypertension")
    ) \
    .select("hypertension", "heart_disease")
    
## Calculate hypertension rate by diabetes
disease_rate = hypertension.groupBy("hypertension") \
    .agg(
        _round(avg(col("heart_disease")) * 100).alias("pct_heart_disease (%)")
    )
disease_rate.show(truncate=False)

## Chi-square test
print("\\n=== Chi-square test for hypertension and heart disease ===")
hypertension = hypertension.withColumn(
        "heart_disease",
        when(col("heart_disease") == 1, "True")
        .when(col("heart_disease") == 0, "False")
    )

print("\\n=== CONTINGENCY TABLE ===")
contingency_table = hypertension.groupBy("hypertension", "heart_disease") \
    .agg(
        count("*").alias("observed")
    ) 
contingency_table.show(truncate=False)

contingency_table = contingency_table.toPandas() \
    .pivot(
        index='hypertension', 
        columns='heart_disease', 
        values='observed'
    ) \
    .fillna(0)

chi2, p_value, dof, expected = chi2_contingency(contingency_table.values)

print(f'Chi-square test of independence: {chi2}')
print(f'p-value: {p_value}')
print(f'Degrees of freedom: {dof}')
print(f'Expected frequencies: {expected}')
print(f'p-value < 0.05: {p_value < 0.05}')

if p_value < 0.05:
    print(">> Reject null hypothesis.\nConclusion: With p_value = 0.05, there is no association between hypertension and heart disease rate.")
else:
    print(">> Fail to reject null hypothesis.\nConclusion: With p_value = 0.05, the data is not reliable and the association between hypertension and heart disease rate is not significant.")

# 4. Build a machine learning model for heart disease prediction.


    
