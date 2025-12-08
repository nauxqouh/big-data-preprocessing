# My Assignment

## Requirements:

### Part 1. Basic operations with PySpark

Using [customers-100.csv](../customers-100.csv), and [customers-1000.csv](../customers-1000.csv) practice importing CSV files into PySpark, exploring the schema, and performing basic transformations.

### Part 2. Redo lesson 3 exercise with PySpark

Using sakila database (download [sakila database](https://dev.mysql.com/doc/index-other.html)) or provided sql file in this lesson 3 folder

- [sakila-schema.sql](../lesson3/sakila-schema.sql)
- [sakila-data.sql](../lesson3/sakila-data.sql)

Using PySpark to solve:

- Import all data into HDFS.
- Import film table into another folder but only has film released in 2008 or later.
- Add some record to film table, update all table has changed into HDFS.

## Solutions

### Folder Structure

```bash
big-data-processing-experiment/
├── README.md                        # Assignment overview & instructions
├── PySpark_CSVExperiment.ipynb      # Jupyter Notebook for Part 1
├── PySpark_HDFS.ipynb               # Jupyter Notebook for Part 2
└── output/
    ├── customers_combined/          # Output of Part 1 (combined CSV)
    └── part2/                       # Output of Part 2 (joined/cleaned data, HDFS copy, etc.)
```

### Instructions

1. Make sure `Python 3.13+`, `Java 17` and `PySpark` are installed.

2. Run `PySpark_CSVExperiment.ipynb` to explore basic operations in PySpark.

3. Run `PySpark_HDFS.ipynb` for 

4. Outputs will be saved into the `output/` folder.
