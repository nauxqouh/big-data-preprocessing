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

This section summarizes the results after completing the exercises.

### Import *.sql file into database in Docker

```bash
docker exec -i lesson2-mariadb-1 mariadb -uroot -prootpassword sakila < ./sakila-schema.sql
docker exec -i lesson2-mariadb-1 mariadb -uroot -prootpassword sakila < ./sakila-data.sql
```

### Import all data into HDFS.

- All tables from Sakila database were successfully imported into HDFS through:

```bash
sqoop import-all-tables \
--connect jdbc:mysql://mariadb:3306/sakila \
--username root \
--password rootpassword \
--warehouse-dir /user/hadoop/sakila \
-m 1
```

- Directory structure:

```bash
/user/hadoop/sakila/
    ├── actor
    ├── actor_info
    ├── address
    ├── category
    ├── city
    ├── country
    ├── customer
    ├── customer_list
    ├── film
    ├── film_actor
    ├── film_category
    ├── film_list
    ├── film_text
    ├── inventory
    ├── language
    ├── nicer_but_slower_film_list
    ├── payment
    ├── rental
    ├── sales_by_film_category
    ├── sales_by_store
    ├── staff
    ├── staff_list
    └── store
```
    Each table was stored in its own folder under `sakila`.

- You can check it:
```bash
hdfs-namenode:/# hdfs dfs -ls /user/hadoop/sakila
```

Output:

```bash
Found 23 items
drwxr-xr-x   - root supergroup          0 2025-12-08 02:54 /user/hadoop/sakila/actor
drwxr-xr-x   - root supergroup          0 2025-12-08 02:56 /user/hadoop/sakila/actor_info
drwxr-xr-x   - root supergroup          0 2025-12-08 02:58 /user/hadoop/sakila/address
drwxr-xr-x   - root supergroup          0 2025-12-08 03:00 /user/hadoop/sakila/category
drwxr-xr-x   - root supergroup          0 2025-12-08 02:59 /user/hadoop/sakila/city
drwxr-xr-x   - root supergroup          0 2025-12-08 02:59 /user/hadoop/sakila/country
drwxr-xr-x   - root supergroup          0 2025-12-08 02:49 /user/hadoop/sakila/customer
drwxr-xr-x   - root supergroup          0 2025-12-08 02:50 /user/hadoop/sakila/customer_list
drwxr-xr-x   - root supergroup          0 2025-12-08 02:48 /user/hadoop/sakila/film
drwxr-xr-x   - root supergroup          0 2025-12-08 02:51 /user/hadoop/sakila/film_actor
drwxr-xr-x   - root supergroup          0 2025-12-08 03:01 /user/hadoop/sakila/film_category
drwxr-xr-x   - root supergroup          0 2025-12-08 03:01 /user/hadoop/sakila/film_list
drwxr-xr-x   - root supergroup          0 2025-12-08 02:52 /user/hadoop/sakila/film_text
drwxr-xr-x   - root supergroup          0 2025-12-08 02:53 /user/hadoop/sakila/inventory
drwxr-xr-x   - root supergroup          0 2025-12-08 02:56 /user/hadoop/sakila/language
drwxr-xr-x   - root supergroup          0 2025-12-08 02:54 /user/hadoop/sakila/nicer_but_slower_film_list
drwxr-xr-x   - root supergroup          0 2025-12-08 02:47 /user/hadoop/sakila/payment
drwxr-xr-x   - root supergroup          0 2025-12-08 02:58 /user/hadoop/sakila/rental
drwxr-xr-x   - root supergroup          0 2025-12-08 02:55 /user/hadoop/sakila/sales_by_film_category
drwxr-xr-x   - root supergroup          0 2025-12-08 02:52 /user/hadoop/sakila/sales_by_store
drwxr-xr-x   - root supergroup          0 2025-12-08 02:57 /user/hadoop/sakila/staff
drwxr-xr-x   - root supergroup          0 2025-12-08 02:49 /user/hadoop/sakila/staff_list
drwxr-xr-x   - root supergroup          0 2025-12-08 02:47 /user/hadoop/sakila/store
```

### Import film table into another folder but only has film released in 2008 or later.


```bash
sqoop eval \
--connect jdbc:mysql://mariadb:3306/sakila \
--username root \
--password rootpassword \
--query "DESCRIBE film"
```

A second import was executed for the `film` table with filtering:

```bash
sqoop import \
--connect jdbc:mysql://mariadb:3306/sakila \
--username root \
--password rootpassword \
--table film \
--where "release_year >= 2008" \
--target-dir /user/hadoop/sakila_film_2008_later/ \
-m 1
```

Resulting directory:

```bash
/user/hadoop/sakila_film_2008_later/
```

- Contains only films released 2006 and later.

### Add some record to film table, update all table has changed into HDFS

Create some example records:
```sql
INSERT INTO film (
    film_id, title, description, release_year,
    language_id, original_language_id,
    rental_duration, rental_rate, length,
    replacement_cost, rating, special_features,
    last_update
) VALUES
(1001, 'AI REVOLUTION', 'A future ruled by AI', 2024, 1, NULL, 6, 2.99, 120, 19.99, 'PG', 'Trailers', '2025-01-01 10:00:00'),
(1002, 'DATA WARRIORS', 'Hackers fight for truth', 2024, 1, NULL, 5, 1.99, 95, 14.99, 'PG-13', 'Trailers', '2025-01-02 11:00:00'),
(1003, 'CODE MATRIX', 'Programmers trapped in virtual world', 2025, 1, NULL, 7, 2.99, 110, 17.99, 'R', 'Commentaries', '2025-01-03 12:00:00'),
(1004, 'HADOOP RISING', 'Big data saves the world', 2025, 1, NULL, 4, 0.99, 85, 12.99, 'PG', 'Trailers', '2025-01-04 13:00:00'),
(1005, 'SQOOP LEGACY', 'Import data like a hero', 2024, 1, NULL, 3, 1.99, 75, 11.99, 'PG', 'Deleted Scenes', '2025-01-05 14:00:00'),
(1006, 'SPARK RUNNERS', 'Speed of light computing', 2025, 1, NULL, 5, 2.49, 105, 21.99, 'PG-13', 'Behind the Scenes', '2025-01-06 15:00:00'),
(1007, 'KAFKA CHAOS', 'Messaging gone wild', 2024, 1, NULL, 6, 3.49, 90, 13.99, 'R', 'Trailers', '2025-01-07 16:00:00'),
(1008, 'FLINK WAVE', 'Real-time processing adventure', 2025, 1, NULL, 4, 1.49, 88, 10.99, 'PG', 'Trailers', '2025-01-08 17:00:00'),
(1009, 'DEEP LEARNERS', 'Neural networks awaken', 2024, 1, NULL, 5, 2.99, 130, 24.99, 'PG-13', 'Commentaries', '2025-01-09 18:00:00'),
(1010, 'CYBER SENTINELS', 'Guardians of the cloud', 2025, 1, NULL, 7, 1.99, 98, 15.99, 'PG', 'Trailers', '2025-01-10 19:00:00');
```

We simulated adding new rows to the film table and performed incremental import using:

- **Append Mode**: using `film_id` column.
    ```bash
    sqoop import \
    --connect jdbc:mysql://mariadb:3306/sakila \
    --username root \
    --password rootpassword \
    --table film \
    --target-dir /user/hadoop/sakila/film \
    --incremental append \
    --check-column film_id \
    --last-value 1000 \
    -m 1
    ```

    After each incremental import, Sqoop prints the next `--last-value` in the log output. For example, you may see: `--last-value 1010`. **You must save this value and reuse it as the --last-value for the next incremental import**.

- **Last Modified Mode**: using `last_update` column.
    ```bash
    sqoop import \
    --connect jdbc:mysql://mariadb:3306/sakila \
    --username root \
    --password rootpassword \
    --table film \
    --target-dir /user/hadoop/sakila/film/ \
    --incremental lastmodified \
    --check-column last_update \
    --last-value "2006-02-15 05:03:42.0" \
    --merge-key film_id \
    -m 1
    ```

*Note: Use `hdfs dfs -cat <directory>/* | head` to verify imported records.*


