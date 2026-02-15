# Settings

### Setting up volume directories for Docker containers
```bash
mkdir -p data logs spark-events notebooks
```

### Build docker image
```bash
docker compose --env-file ../../.env up -d
```

### Rebuild after Dockerfile / compose changes
```bash
docker compose --env-file ../../.env up -d --build
```

### Structure

- config : 설정 값 변경 가능 -> 변경 후에는 "docker-compose up -d --build" 로 재빌드 필요
- jobs
- data
- spark-events
- logs
- notebook
