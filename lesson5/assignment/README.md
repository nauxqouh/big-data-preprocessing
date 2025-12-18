# Distributed Exploratory Analysis and Heart Disease Prediction with Apache Spark

## Requirements

Perform analyses on the heart.csv data:

1. What is the most common chest pain type by age group?
2. Explore the effect of diabetes on heart disease.
3. Explore the effect of hypertension on heart disease.
4. Build a machine learning model for heart disease prediction.

Data describe:

- `age` - The age of the patient
- `sex` - The gender of the patient
- `angina` - The chest pain experienced (0: Typical Angina, 1: Atypical Angina, 2: Non-Anginal Pain, 3: Asymptomatic)
- `systolic` - The patient's systolic blood pressure (mm Hg on admission to the hospital)
- `cholesterol` - The patient's cholesterol measurement in mg/dl
- `diabetes` - If the patient has diabetes (0: False, 1: True)
- `electrocardiogram` - Resting electrocardiogram results (0: Normal, 1: ST-T Wave Abnormality, 2: Left Ventricular Hypertrophy)
- `max_heart_rate` - The patient's maximum heart rate achieved
- `exercise_induced_angina` - Exercise induced angina (0: No, 1: Yes)
- `ST_depression` - ST depression induced by exercise relative to rest ('ST' relates to positions on the ECG plot)
- `ST_slope` - The slope of the peak exercise ST segment (0: Upsloping, 1: Flatsloping, 2: Downsloping)
- `number_of_major_vessels` - The number of major vessels (0-3)
- `thalassemia` - A blood disorder called thalassemia (0: Normal, 1: Fixed Defect, 2: Reversible Defect)
- `heart_disease` - Heart disease (0: No, 1: Yes)

## Results

Submit a test application:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/heart_disease_analysis.py
```

The result:

### 1. What is the most common chest pain type by age group?

```text
\n=== CHEST PAIN TYPE BY AGE GROUP ANALYSIS ===
\n=== PIVOT TABLE WITH RANK ===
+---------+------------+---------------+----------------+--------------+
|age_group|Asymptomatic|Atypical Angina|Non-anginal Pain|Typical Angina|
+---------+------------+---------------+----------------+--------------+
|29-37    |2           |1              |4               |3             |
|38-45    |4           |3              |2               |1             |
|46-53    |4           |3              |1               |2             |
|54-61    |4           |3              |2               |1             |
|62-69    |3           |4              |2               |1             |
|70-77    |4           |3              |1               |2             |
+---------+------------+---------------+----------------+--------------+

\n=== THE MOST COMMON CHEST PAIN TYPE BY AGE GROUP ===
+---------+------------------+
|age_group|most_common_angina|
+---------+------------------+
|29-37    |Atypical Angina   |
|38-45    |Typical Angina    |
|46-53    |Non-anginal Pain  |
|54-61    |Typical Angina    |
|62-69    |Typical Angina    |
|70-77    |Non-anginal Pain  |
+---------+------------------+
```

### 2. Explore the effect of diabetes on heart disease.

```text
\n=== Explore the effect of diabetes on heart disease ===
+-----------+---------------------+
|diabetes   |pct_heart_disease (%)|
+-----------+---------------------+
|No Diabetes|56.0                 |
|Diabetes   |62.0                 |
+-----------+---------------------+
```

```text
\n=== Chi-square test for diabetes and heart disease ===
\n=== CONTINGENCY TABLE ===
+-----------+-------------+--------+
|diabetes   |heart_disease|observed|
+-----------+-------------+--------+
|Diabetes   |True         |87      |
|Diabetes   |False        |53      |
|No Diabetes|True         |410     |
|No Diabetes|False        |324     |
+-----------+-------------+--------+

Chi-square test of independence: 1.645609752610422
p-value: 0.19955752208047411
Degrees of freedom: 1
Expected frequencies: [[ 60.38901602  79.61098398]
 [316.61098398 417.38901602]]
p-value < 0.05: False
Fail to reject null hypothesis.
Conclusion: With p_value = 0.05, the data is not reliable and the association between diabetes and heart disease rate is not significant.
```

### 3. Explore the effect of hypertension on heart disease.

```text
\n=== Explore the effect of hypertension on heart disease ===
+---------------+---------------------+
|hypertension   |pct_heart_disease (%)|
+---------------+---------------------+
|Hypertension   |50.0                 |
|No Hypertension|64.0                 |
+---------------+---------------------+
```

```text
\n=== Chi-square test for hypertension and heart disease ===
\n=== CONTINGENCY TABLE ===
+---------------+-------------+--------+
|hypertension   |heart_disease|observed|
+---------------+-------------+--------+
|No Hypertension|False        |167     |
|Hypertension   |False        |210     |
|Hypertension   |True         |206     |
|No Hypertension|True         |291     |
+---------------+-------------+--------+

Chi-square test of independence: 16.89694124479942
p-value: 3.9465126770832034e-05
Degrees of freedom: 1
Expected frequencies: [[179.4416476 236.5583524]
 [197.5583524 260.4416476]]
p-value < 0.05: True
>> Reject null hypothesis.
Conclusion: With p_value = 0.05, there is no association between hypertension and heart disease rate.
```

### 4. Build a machine learning model for heart disease prediction.

