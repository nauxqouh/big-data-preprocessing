# ⚙️ Setup mini Hadoop and Sqoop System on Your Machine

## Getting Started:

```bash
git clone https://github.com/nauxqouh/big-data-preprocessing.git
cd lesson2
```

Follow the steps below to set up the entire system.

### 1. Build Images and Start All Containers

First of all, you need to create your own `.env` file, include:
```bash
MYSQL_ROOT_PASSWORD=<your_password>
MYSQL_DATABASE=<your_db_name>
MYSQL_USER=<your_username>
MYSQL_PASSWORD=<your_password>
```

Start building!
```bash
docker compose up -d
```

Check container status:
```bash
docker ps
```

If all containers are running, your environment is ready.

### 2. Verify Your Hadoop + Sqoop Environment (Testing Guide)

This section helps you confirm that each component (Hadoop, MariaDB, Sqoop, phpMyAdmin) is working correctly.

#### 2.1. Test 1: Hadoop HDFS

Enter the `namenode` container:
```bash
docker exec -it lesson2-namenode-1 bash
```

Check Hadoop storage status:
```bash
hdfs dfsadmin -report
```

- Expected output (sample):
    ```bash
    hdfs-namenode:/# hdfs dfsadmin -report
    Configured Capacity: 0 (0 B)
    Present Capacity: 0 (0 B)
    DFS Remaining: 0 (0 B)
    DFS Used: 0 (0 B)
    DFS Used%: 0.00%
    Under replicated blocks: 0
    Blocks with corrupt replicas: 0
    Missing blocks: 0
    Missing blocks (with replication factor 1): 0
    Pending deletion blocks: 0

    -------------------------------------------------
    ```

#### 2.2. Test 2: Import SQL sample into MariaDB

Import sample data:

```bash
docker exec -i lesson2-mariadb-1 mariadb -uroot -prootpassword mydb < ./mysqlsampledatabase.sql
```

Access MariaDB:
```bash
docker exec -it lesson2-mariadb-1 mariadb -uroot -prootpassword
```

Inside MariaDB:
```bash
SHOW DATABASES;
USE mydb;
SHOW TABLES;
SELECT * FROM customers LIMIT 5;
```

If tables and data appear, MariaDB is working.

**Note:**

You can use `mysqlsampledatabase.sql` file in this directory to testing your system. 

- Execute sql into database in Docker:
```bash
docker exec -i lesson2-mariadb-1 mariadb -uroot -prootpassword mydb < ./mysqlsampledatabase.sql
```

*Replace `lesson2-mariadb-1` with your container name and `./mysqlsampledatabase.sql` with your sql file path. Using your own password in `.env` for `-prootpassword` and `mydb`.*

#### 2.3. Test 3 — Access phpMyAdmin

Open browser 👉 http://localhost:8080

Log in with:

- Username: `root`
- Password: in your `.env` ~ `MYSQL_ROOT_PASSWORD=<your_password>`

If you can see the databases, phpMyAdmin is working.

#### 2.4. Test 4 — Test connection between Sqoop and MariaDB

Enter Sqoop container:
```bash
docker exec -it lesson2-sqoop-1 bash
```

Run:
```bash
sqoop list-databases \
  --connect jdbc:mysql://mariadb:3306 \
  --username root \
  --password rootpassword
```

Right results return a list of database. Sqoop is correctly connected to MariaDB.

### 3. Finished

If you reach this point, congratulations 🎉!

You have successfully set up a mini Big Data system including:

- Hadoop (NameNode, DataNode, Secondary NameNode)
- MariaDB
- Sqoop
- phpMyAdmin

Note: *Sqoop import/export workflows will be explored in the next lesson.*

## Stop All Containers

```bash
docker compose down
```

