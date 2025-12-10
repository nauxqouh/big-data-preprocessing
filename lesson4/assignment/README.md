# My Assignment

The exercises include working with CSV files, performing DataFrame operations, loading the Sakila relational database into Spark, modifying records, and exporting the final results.

## Requirements

### Part 1. Basic operations with PySpark

Using the provided CSV files:

- [customers-100.csv](../customers-100.csv)
- [customers-1000.csv](../customers-1000.csv)

You will:

1. Import CSV files into PySpark.
2. Explore and print schema.
3. Perform basic transformations.
4. Save the output as a combined dataset.

### Part 2. Redo lesson 3 exercise in memory with PySpark

Using sakila database (download [sakila database](https://dev.mysql.com/doc/index-other.html)) or provided sql file in this lesson 3 folder

- [sakila-schema.sql](../../lesson3/sakila-schema.sql)
- [sakila-data.sql](../../lesson3/sakila-data.sql)

Using PySpark to solve:

1. Load all tables from Maria Database into PySpark DataFrames.
2. Filter films released in 2008 or later.
3. Add new film records (in memory).
4. Save outputs to Parquet for further processing.

## Folder Structure

```bash
assignment/
├── README.md                        # Assignment documentation
├── PySpark_CSVExperiment.ipynb      # Jupyter Notebook for Part 1
├── PySpark_HDFS.ipynb               # Jupyter Notebook for Part 2
├── lib/                             
│   └── mysql-connector-j-8.3.xx.jar # JDBC driver
└── output/
    ├── part1/                       # Output from Part 1
    └── part2/                       # Output from Part 2
```

## Environment & Dependencies

To run the notebooks, ensure the following environment:
- `Python 3.13+`
- `Java 17`
- `Apache Spark 3.5.x`
- `PySpark 3.5.x`
- `MySQL 8.x`
- JDBC Driver: `mysql-connector-j-8.3.x.jar`.

## Results

All results are fully documented in each notebook.

Please open the notebooks (`.ipynb` files) to review the outputs.

