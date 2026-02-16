# Settings

### Setting up volume directories for SPARK Docker containers
```bash
mkdir -p spark/data spark/logs spark/spark-events
```

### Build docker image
```bash
docker compose --env-file ../../.env up -d
```

### Rebuild after Dockerfile / compose changes
```bash
docker compose --env-file ../../.env up -d --build
```
