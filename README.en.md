# Totally-not-odido-parser-v3
![Screenshot](screen.png)

Local viewer for large `dataset.txt` files.

Run one command, the web app comes online immediately, and data is ingested/indexed in the background.

## What matters most

> 🚨 **Vibe code alert:** this fork uses PostgreSQL backend for better performance and concurrent user support.
>
> - Database connection via PostgreSQL (host, port, user, password, dbname)
> - Docker Compose setup with automatic PostgreSQL service
> - Migrating from SQLite? See `MIGRATION_SQLITE_TO_POSTGRES.md`

- The web interface is available right away at `http://localhost:8080`.
- You can already search and view records during the **first ingest**.
- Results become more complete and faster as ingest/indexing progresses.
- If the process stops, it resumes automatically from the last committed batch.

## Quick start with Docker Compose

```bash
# Start both PostgreSQL and the app
docker-compose up
```

Then open:

- `http://localhost:8080/` (search UI)
- `http://localhost:8080/analytics` (analytics UI)

## Manual startup

If you have your own PostgreSQL database:

```bash
go run ./cmd/server \
  -dataset dataset.txt \
  -db-host localhost \
  -db-port 5432 \
  -db-user postgres \
  -db-password mypassword \
  -db-name odido_parser
```

## Docker

### Multi-arch build (AMD64 + ARM64):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t totally-not-odido-parser-v3:latest .
```

### Manual container startup:

```bash
# Start PostgreSQL first
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=odido_parser -p 5432:5432 postgres:16-alpine

# Then the app
docker run --rm -p 8080:8080 -v "$(pwd):/data" \
  -e DB_HOST=host.docker.internal \
  -e DB_PASSWORD=postgres \
  totally-not-odido-parser-v3:latest
```

## What the app does

- Reads lines from `dataset.txt` (JSONL format).
- Stores them in PostgreSQL database.
- Builds indexes for fast search/filtering.
- Shows indexing progress in the UI (top-right) and periodically in logs.

## New in this fork (short)

- PostgreSQL backend for better concurrency and performance.
- Web UI is available immediately, even during the first ingest.
- Records are searchable during ingest; no need to wait for 100%.
- Resume after restart: indexing continues from the last committed batch.
- Faster default ingest settings.
- Better keyword search across more fields.
- Improved UI with index status, communication/notes views, and analytics.
- Enhanced error handling and UTF-8 validation.

## First startup

If the database is still empty:

- the app creates the tables automatically;
- ingest/indexing starts automatically;
- the web UI remains usable during this process.

## Next startups

- If the database is valid, everything is ready immediately.
- If the dataset changed, rebuild starts automatically.

## Default behavior

- Fast-index is always enabled.
- Default commit batch is `50000`.
- JSON fields are indexed by default.
- Schema creation has 5-minute timeout.

## Useful options

```bash
go run ./cmd/server \
  -addr :8080 \
  -dataset dataset.txt \
  -db-host localhost \
  -db-port 5432 \
  -db-user postgres \
  -db-password password \
  -db-name odido_parser \
  -skip-index  # Skip indexing on startup (for development)
```

- `-addr`: listen address/port (default `:8080`)
- `-dataset`: dataset path (default `dataset.txt`)
- `-db-host`: PostgreSQL host (default `localhost`)
- `-db-port`: PostgreSQL port (default `5432`)
- `-db-user`: database user (default `postgres`)
- `-db-password`: database password (default empty)
- `-db-name`: database name (default `odido_parser`)
- `-skip-index`: skip indexing on startup (for development)

## Common API endpoints

- `GET /api/health`
- `GET /api/index/status`
- `GET /api/stats`
- `GET /api/facets`
- `GET /api/json/paths`
- `GET /api/records?...`
- `GET /api/records/comm`
- `GET /api/records/phones`
- `GET /api/records/flash`
- `GET /api/records/{id}`
- `GET /api/analytics/fields`
- `GET /api/analytics/distribution?field=email_domain&limit=25&not_empty=true`
- `GET /api/analytics/count`

## Troubleshooting

### Check port:

```bash
ss -ltnp | grep ':8080'
```

### Health check:

```bash
curl -s http://localhost:8080/api/health
```

### Index status:

```bash
curl -s http://localhost:8080/api/index/status
```

### Database connection issues:

```bash
# Test database connection
psql -h localhost -U postgres -d odido_parser -c "SELECT 1;"
```

### Slow startup:

- Schema creation can take 10-30 seconds on first run
- Index creation can take minutes depending on data size
- Check logs for detailed progress

## Credits

Based on the original project: https://github.com/stuncs69/totally-not-odido-parser

## SQLite Migration

If you're migrating from the old SQLite version, see `MIGRATION_SQLITE_TO_POSTGRES.md` for instructions.
