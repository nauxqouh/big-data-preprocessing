# My Assignment

## Requirements

### Part 1. Basic operations with PySpark

Using [customers-100.csv](../customers-100.csv), and [customers-1000.csv](../customers-1000.csv) practice importing CSV files into PySpark, exploring the schema, and performing basic transformations.

### Part 2. Redo lesson 3 exercise with PySpark

Using sakila database (download [sakila database](https://dev.mysql.com/doc/index-other.html)) or provided sql file in this lesson 3 folder

- [sakila-schema.sql](lesson3/sakila-schema.sql)
- [sakila-data.sql](lesson3/sakila-data.sql)

Using PySpark to solve:

- Import all data into HDFS.
- Import film table into another folder but only has film released in 2008 or later.
- Add some record to film table, update all table has changed into HDFS.

## Solutions

### Folder Structure

```bash
assignment/
├── README.md                        # Assignment overview & instructions
├── PySpark_CSVExperiment.ipynb      # Jupyter Notebook for Part 1
├── PySpark_HDFS.ipynb               # Jupyter Notebook for Part 2
└── output/
    ├── customers_combined/          # Output of Part 1 (combined CSV)
    └── part2/                       # Output of Part 2 (joined/cleaned data, HDFS copy, etc.)
```

### Instructions

1. Ensure `Python 3.13+`, `Java 17`, and `PySpark` are installed.
2. **For Part 1:**
- Open `PySpark_CSVExperiment.ipynb`.
- Import CSV files, explore the schema, and perform basic transformations.
- Combined outputs will be saved in `output/customers_combined/`.
3. **For Part 2:**
- Set up `sakila` database like lesson 3.
- Open `PySpark_HDFS.ipynb`.
- Load all tables from `sakila` into HDFS.
- Filter film table for films released in 2008 or later and save into a separate HDFS folder.
- Add example records to film table and perform incremental updates.
- Outputs and logs will be saved in `output/part2/`.
4. To check incremental load, verify the last value used and ensure new rows are appended or merged correctly.

### Notes

- Use Spark's DataFrame API for loading, filtering, and writing data.
- Each table should have its own output folder in HDFS (similar to `sqoop import-all-tables`).
- Remember to stop the Spark session after completing each notebook.
- For reproducibility, all outputs are stored in the `output/` folder.