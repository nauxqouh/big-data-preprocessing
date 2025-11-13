# Setup mini Hadoop and Sqoop system in your machine

Getting Started:

```bash
git clone https://github.com/nauxqouh/big-data-preprocessing.git
cd lesson2
```

Then, following all below to setup system in your machine.

### 1. Build image from Dockerfile

```bash
docker compose build
```

### 2. Run all container

```bash
docker compose up -d
```

You can check container status
```bash
docker ps
```

### 3. Access into container if you want to test inside

Example: You want to get into `namenode` to test Hadoop

```bash
docker exec -it <container_id> bash
```

### Additional: Stop docker

```bash
docker compose down
```

You can use `mysqlsampledatabase.sql` file in this directory to testing your system. 

- Execute sql into database in Docker:
```bash
docker exec -i lesson2-mariadb-1 mariadb -uroot -prootpassword mydb < ./mysqlsampledatabase.sql
```

Replace `lesson2-mariadb-1` with your container name and `./mysqlsampledatabase.sql` with your sql file path. Using your own password in `.env` for `-prootpassword` and `mydb`.