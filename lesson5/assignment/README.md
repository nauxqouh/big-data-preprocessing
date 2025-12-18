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

---

## Results

### 1. What is the most common chest pain type by age group?

Patients were grouped into 6 age ranges, and the most common **chest pain type** was computed for each age group.  

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

According to the table, we can conclude the following:

  - **Typical Angina** is the most common type of chest pain across most age groups, except for 29-37, 46-53, and 70-77, where **Atypical Angina** or **Non-anginal Pain** dominates. 
  - **Non-anginal** is generally the second most common, **Atypical Angina** the third, and **Asymptomati**c the least common, with a few age-specific exceptions.

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

From the table above, we can conclude that:

- Diabetes patients have a heart disease rate of 62%.
- Patients without diabetes have a 56% heart disease rate.

It appears that patients diagnosed with diabetes have a higher positive heart disease rate than those without diabetes.

However, I decide to perform a chi-square test of independence to verify the reliability of the data

- **Null hypothesis:** There is no association between diabetes and heart disease rate.
- **Alternative hypothesis:** Diabetes and heart disease rate are associated.
- **Threshold:** p-value = 0.05

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

Therefore, we can conclude that the data is not reliable and the association between diabetes and heart disease rate is not significant.

### 3. Explore the effect of hypertension on heart disease.

High blood pressure, also called hypertension, is blood pressure that is higher than normal. 

Some health care professionals diagnose patients with high blood pressure if their blood pressure is consistently a constant number of mm Hg or higher. I decide to use the [2017](https://www.jacc.org/doi/10.1016/j.jacc.2017.11.006?_ga=2.86879320.1182640551.1528306905-1524800955.1528306905) guideline to diagnose hypertension with the limit at 130 mm Hg.

```text
\n=== Explore the effect of hypertension on heart disease ===
+---------------+---------------------+
|hypertension   |pct_heart_disease (%)|
+---------------+---------------------+
|Hypertension   |50.0                 |
|No Hypertension|64.0                 |
+---------------+---------------------+
```

From the table above, we can conclude that:

- Hypertension patients have a heart disease rate of 64%.
- Patients without hypertension have a 50% heart disease rate.

It appears that patients diagnosed with hypertension have a higher positive heart disease rate than those without hypertension.

However, I decide to perform a chi-square test of independence to verify the reliability of the data

- **Null hypothesis:** There is no association between hypertension and heart disease rate.
- **Alternative hypothesis:** Hypertension and heart disease rate are associated.
- **Threshold:** p-value = 0.05

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

Therefore, the null hypothesis is rejected in favor of the alternative hypothesis, then we conclude that: Hypertension effects heart disease rate.

### 4. Build a machine learning model for heart disease prediction.

In this step, I build predictive models to identify patients at risk of heart disease using the features in the dataset. I train and compare three models: **Logistic Regression**, **Random Forest**, and **K-Nearest Neighbors**.

**Pipeline and preprocessing:**

- Numeric features are standardized using `StandardScaler`.
- Categorical features are one-hot encoded using `OneHotEncoder`.
- The preprocessing steps are combined with each classifier in a `Pipeline` to ensure consistent treatment of the data during training and evaluation.

**Model evaluation:** I compare models using key metrics such as Accuracy, Precision, Recall, F1-score, and ROC-AUC on the test set.

```text
\n=== Training ===
\n=== Comparison of Models ===
                 Model  Accuracy  Precision  Recall  F1-score   ROC_AUC
0  Logistic Regression  0.971429   0.979798    0.97  0.974874  0.984667
1        Random Forest  0.965714   0.979592    0.96  0.969697  0.996000
2                  KNN  0.954286   0.960000    0.96  0.960000  0.968800
```

Although Random Forest achieves slightly higher ROC-AUC on the test set, I select Logistic Regression for the final model because:

1. Logistic Regression provides coefficients for each feature, which allows us to understand how each variable affects heart disease risk.
2. Logistic Regression is faster to train and easier to deploy.
3. Logistic Regression achieves a high ROC-AUC (0.985), close to Random Forest, making it a practical choice for explanation and deployment in clinical contexts.

I perform GridSearchCV with Stratified K-Fold cross-validation on Logistic Regression to optimize parameters based on ROC-AUC score. This ensures the model is robust and avoids overfitting.

```text
\n=== Hyperparameter Tuning ===
Fitting 5 folds for each of 480 candidates, totalling 2400 fits

The best estimator is: Pipeline(steps=[('preprocessor',
                 ColumnTransformer(transformers=[('cat', OneHotEncoder(),
                                                  ['sex', 'angina', 'diabetes',
                                                   'electrocardiogram',
                                                   'exercise_induced_angina',
                                                   'ST_slope',
                                                   'number_of_major_vessels',
                                                   'thalassemia']),
                                                 ('num', StandardScaler(),
                                                  ['age', 'systolic',
                                                   'cholesterol',
                                                   'max_heart_rate',
                                                   'ST_depression'])])),
                ('logisticregression',
                 LogisticRegression(C=0.01, class_weight='balanced',
                                    max_iter=12000, solver='saga',
                                    tol=1e-06))])
The best params are: {'logisticregression__C': 0.01, 'logisticregression__class_weight': 'balanced', 'logisticregression__max_iter': 12000, 'logisticregression__penalty': 'l2', 'logisticregression__solver': 'saga', 'logisticregression__tol': 1e-06}
The best ROC_AUC score is: 0.9599237825966659
Execution time: 1.515 minutes
\n=== Best estimator confusion matrix ===
[[69  6]
 [ 7 93]]
```
The final algorithm, optimized through the use of GridSearchCV, was evaluated on the test data and shown to have an area under the receiver operating characteristic curve (ROC_AUC) of 95.99%.

```text
\n=== Best estimator important feature ===
                    Feature  Coefficient
0                    angina    -0.386301
1               thalassemia    -0.228056
2                       sex    -0.188587
3         electrocardiogram    -0.054691
4   exercise_induced_angina    -0.049703
5                  ST_slope     0.020109
6             ST_depression     0.029595
7            max_heart_rate     0.054692
8                  systolic     0.086123
9                  diabetes     0.108389
10                      age     0.188587
11              cholesterol     0.191791
12  number_of_major_vessels     0.228057
```

After tuning, the Logistic Regression model highlights the most influential factors for predicting heart disease, including **angina**, **thalassemia**, **number of major vessels**. This insight can support clinicians in identifying high-risk patients effectively.

---

## How to Run

Submit a test application:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/heart_disease_analysis.py
```