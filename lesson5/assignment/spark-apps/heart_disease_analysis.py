from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, round as _round, when, rank
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from scipy.stats import chi2_contingency
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold, KFold, GridSearchCV
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, \
    roc_auc_score, confusion_matrix
import pandas as pd
import time

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
    .withColumnRenamed("angina", "most_common_chest_pain")
most_common_cp.show(truncate=False)

# Save results
print("\\nSaving results to output directory...")
most_common_cp.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-data/output/most_common_chest_pain_by_age_group")
print("\\nAnalysis complete! Check spark-data/output/ directory for results.")

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
print("\\n=== Build a machine learning model for heart disease prediction ===")
heart_pd = heart_df.toPandas()

## Feature Engineering
X = heart_pd.drop('heart_disease', axis=1).copy()
y = heart_pd['heart_disease'].copy()
X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2, random_state=1)

num_cols = ["age", "systolic", "cholesterol", "max_heart_rate", "ST_depression"]
cat_cols = [c for c in X.columns if c not in num_cols]
preprocessor = ColumnTransformer([
    ('cat', OneHotEncoder(), cat_cols),
    ('num', StandardScaler(), num_cols)
])

## Train model
print("\\n=== Training ===")
models = {
    'Logistic Regression': LogisticRegression(),
    'Random Forest': RandomForestClassifier(),
    'KNN': KNeighborsClassifier()
}

results = []
for name, model in models.items():
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', model)
    ])
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    y_probs = pipeline.predict_proba(X_test)[:, 1]

    results.append({
        'Model': name,
        'Accuracy': accuracy_score(y_test, y_pred),
        'Precision': precision_score(y_test, y_pred),
        'Recall': recall_score(y_test, y_pred),
        'F1-score': f1_score(y_test, y_pred),
        'ROC_AUC': roc_auc_score(y_test, y_probs)
    })
    
results_df = pd.DataFrame(results)
print("\\n=== Comparison of Models ===")
print(results_df)

## Hyperparameter Tuning
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('logisticregression', LogisticRegression())
])

param_grid = {
    'logisticregression__solver': ['liblinear', 'saga'],
    'logisticregression__penalty': ['l1', 'l2'],
    'logisticregression__tol': [1e-6, 1e-5, 1e-4, 1e-3],
    'logisticregression__C': [1e-6, 1e-5, 1e-4, 1e-3, 1e-2],
    'logisticregression__class_weight': [None, 'balanced'],
    'logisticregression__max_iter': [12000, 15000, 18000]
}

cv = StratifiedKFold(shuffle=True, random_state=1)

print("\\n=== Hyperparameter Tuning ===")
grid_search = GridSearchCV(
    estimator=pipeline,
    param_grid=param_grid,
    scoring='roc_auc',
    cv=cv,
    n_jobs=-1,
    verbose=1
)

start_time = time.time()
grid_search.fit(X_train, y_train)
end_time = time.time()
execution_time = (end_time - start_time) / 60

print(f'The best estimator is: {grid_search.best_estimator_}\n'
      f'The best params are: {grid_search.best_params_}\n'
      f'The best ROC_AUC score is: {grid_search.best_score_}\n'
      f'Execution time: {execution_time:.3f} minutes')

y_pred = grid_search.predict(X_test)
y_probs = grid_search.predict_proba(X_test)[:, 1]

print("\\n=== Best estimator confusion matrix ===")
print(confusion_matrix(y_test, y_pred))

print("\\n=== Best estimator important feature ===")
coefficients = grid_search.best_estimator_.named_steps.logisticregression.coef_[0]
important_model = pd.Series(
    coefficients[:len(X.columns)],
    index=X.columns[:len(coefficients[:len(X.columns)])]
).sort_values()

important_df = important_model.reset_index()
important_df.columns = ['Feature', 'Coefficient']
print(important_df)
    
spark.stop()