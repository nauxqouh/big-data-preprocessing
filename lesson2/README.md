# Build mini Hadoop and Sqoop system for testing big data


### 1. Build image from Dockerfile

```bash
docker compose build
```

### 2. Run all container

```bash
docker compose up
```

You can check container status
```bash
docker ps
```

### 3. Access into container if you want to test inside

Example: You want to get into `namenode` to test Hadoop

```bash
docker exec -it <container_id> /bin/bash
```

### 4. Stop system

```bash
docker compose down
```