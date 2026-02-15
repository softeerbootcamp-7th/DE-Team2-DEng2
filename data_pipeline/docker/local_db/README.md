# Local Postgres (for local mode)

## Start
```bash
cd /Users/apple/DE-Team2-DEng2/worktrees/db/data_pipeline/docker/local_db
docker compose --env-file ../../../.env up -d
```

Default host access:
- host: `localhost`
- port: `5432`
- database: `deng2`
- user: `deng2`
- password: `deng2`

Container network host for Spark job:
- host: `postgres-local`
- port: `5432`

## Stop
```bash
docker compose  down
```
