# Advanced Import Techniques in Sqoop

## Prerequisite

- Setup Hadoop and Sqoop in your machine
- Basic Unix knowledge
- Basic SQL knowledge

## Controlling the import

🎯 In this lesson, I try to controll what data will import from SQL to Hadoop by Sqoop. Special the most five important techniques in Big Data Processing:

### 1. Filtering Rows

The `--where` argument: import a subset of rows based on a condition.

```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table orders \
--where "shippedDate > '2003-04-03'" \
--target-dir /user/root/newdata/ \
-m 1
```

👉 Actually, it works the same way as in SQL:
```bash
SELECT * FROM orders WHERE shippedDate > '2003-04-03';
```

Log:
```bash
Transferred 21.3711 KB in 61.38 seconds (356.5189 bytes/sec)
Retrieved 297 records.
HDFS: Bytes Written=21884
```

Enter HDFS:
```bash
hdfs dfs -ls /user/root/newdata/
```

Review 20 first records:
```bash
hdfs dfs -cat /user/root/newdata/part-m-00000 | head -20
```

### 2. Filtering Columns

The `--columns` argument: select specific columns.

```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table orders \
--where "shippedDate > '2003-04-03'" \
--columns "orderDate,shippedDate"
--target-dir /user/root/newdata/ \
-m 1
```

*Note: Ensure there are no whitespaces between column names in the `--columns` argument, otherwise Sqoop may throw errors.*

### 3. Handling NULL Values

Databases have a true `NULL` type, but text files in HDFS do not. Sqoop defaults to storing `NULL` as the string `"null"`.

`--null-string <value>`: Represents a NULL in a string column with `<value>`.

```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table orders \
--where "shippedDate > '2003-04-03'" \
--target-dir /user/root/newdata/ \
--null-string "Empty description" \
-m 1
```

`--null-non-string <value>`: Represents a NULL in a non-string (e.g., integer, date) column with `<value>`.

```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table employees \
--target-dir /user/root/newdata/ \
--null-non-string 999999 \
-m 1
```

*Note: Sqoop import always requires `target-dir` to be a new directory. Remove the old folder if you get an error `ERROR tool.ImportTool: Import failed: org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://hdfs-namenode:9000/user/root/newdata already exists`*

```bash
hdfs dfs -rm -r /user/root/newdata
```

### 4. Import all tables

Use sqoop `import-all-tables` command as a convenient way to import every table from a database schema.

```bash
sqoop import-all-tables \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
-m 1 # Use 1 mapreduce for avoiding split column
```

*Note: Sqoop requires a split column for each table when running with multiple mappers. If `-m 1` is not used, any table without a PRIMARY KEY or a valid numeric split column will cause the import to fail.*

### 5. Incremental Imports

Relying on a "full refresh" (re-importing an entire massive table) just to add new records is incredibly inefficient. This method wastes time, consumes significant system resources, and increases costs.

A much better practice is an "incremental load", which intelligently fetches only the new or updated records (the "delta"). This approach is faster, cheaper, and puts less strain on the database, allowing for much fresher, near-real-time data.

- **Append Mode**: Used when new rows are added to the source table, and rows are identified by an auto-incrementing key.
```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table employees \
--target-dir /user/root/newdata/ \
--incremental append \
--check-column employeeNumber \
--last-value 1702 \
-m 1
```

- **Last Modified Mode**: Used when existing rows can be updated, based on a timestamp column.
```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/classicmodels \
--username root \
--password rootpassword \
--table orders \
--target-dir /user/root/newdata/ \
--incremental lastmodified \
--check-column shippedDate \
--last-value "2005-05-27" \
--merge-key orderNumber \
-m 1
```

## Exercise

Using sakila database (download [sakila database](https://dev.mysql.com/doc/index-other.html)) or provided sql file in this repository

- [sakila-schema.sql](./sakila-schema.sql)
- [sakila-data.sql](./sakila-data.sql)

Using Sqoop to solve:

- Import all data into HDFS.
- Import film table into another folder but only has film released in 2008 or later.
- Add some record to film table, update all table has changed into HDFS

## Results

