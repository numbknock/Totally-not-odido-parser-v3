# Totally-not-odido-parser-v3
![Screenshot](screen.png)

Lokale viewer voor grote `dataset.txt` bestanden.

Start één commando, de webapp komt direct online, en de data wordt op de achtergrond geïndexeerd.

## Belangrijk om te weten

> 🚨 **Vibe code alert:** deze fork gebruikt PostgreSQL backend voor betere performance en ondersteuning voor meerdere gebruikers.
>
> - Database connectie via PostgreSQL (host, port, user, password, dbname)
> - Docker Compose setup met automatische PostgreSQL service
> - Migratie van SQLite? Zie `MIGRATION_SQLITE_TO_POSTGRES.md`

- De webinterface is meteen beschikbaar op `http://localhost:8080`.
- Ook tijdens de **eerste ingest** kun je al zoeken en records bekijken.
- Hoe langer ingest/indexeren loopt, hoe completer en sneller de resultaten worden.
- Stop je het proces tussendoor, dan hervat het later automatisch vanaf de laatste batch.

## Snel starten met Docker Compose

```bash
# Start zowel PostgreSQL als de app
docker-compose up
```

Daarna open je:

- `http://localhost:8080/` (zoekscherm)
- `http://localhost:8080/analytics` (analyses)

## Handmatig opstarten

Als je een eigen PostgreSQL database hebt:

```bash
go run ./cmd/server \
  -dataset dataset.txt \
  -db-host localhost \
  -db-port 5432 \
  -db-user postgres \
  -db-password mijnwachtwoord \
  -db-name odido_parser
```

## Docker

### Multi-arch build (AMD64 + ARM64):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t totally-not-odido-parser-v3:latest .
```

### Handmatig container starten:

```bash
# Eerst PostgreSQL starten
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=odido_parser -p 5432:5432 postgres:16-alpine

# Dan de app
docker run --rm -p 8080:8080 -v "$(pwd):/data" \
  -e DB_HOST=host.docker.internal \
  -e DB_PASSWORD=postgres \
  totally-not-odido-parser-v3:latest
```

## Wat doet de app?

- Leest regels uit `dataset.txt` (JSONL formaat).
- Slaat ze op in PostgreSQL database.
- Maakt zoeken/filteren snel via indexen.
- Toont voortgang in de UI (rechtsboven) en periodiek in de logs.

## Nieuw in deze fork (kort)

- PostgreSQL backend voor betere concurrentie en performance.
- Webinterface is direct beschikbaar, ook tijdens de eerste ingest.
- Records zijn al doorzoekbaar tijdens ingest; geen wachten op 100%.
- Hervatten na herstart: indexeren gaat verder vanaf de laatste batch.
- Snellere standaard ingest-instellingen.
- Betere keyword-zoeking over meer velden.
- Verbeterde UI met indexstatus, communicatie/opmerkingen-weergave en analytics.
- Uitgebreide foutafhandeling en UTF-8 validatie.

## Eerste keer opstarten

Als de database nog leeg is:

- de app maakt automatisch de tabellen aan;
- ingest/indexeren start automatisch;
- de web UI blijft gewoon bruikbaar tijdens dit proces.

## Volgende starts

- Als de database actueel is, alles direct klaar.
- Als de dataset veranderd is, start automatisch een rebuild.

## Standaard gedrag

- Fast-index staat altijd aan.
- Default commit batch is `50000`.
- JSON-velden worden standaard geïndexeerd.
- Schema creatie heeft 5 minuten timeout.

## Handige opties

```bash
go run ./cmd/server \
  -addr :8080 \
  -dataset dataset.txt \
  -db-host localhost \
  -db-port 5432 \
  -db-user postgres \
  -db-password wachtwoord \
  -db-name odido_parser \
  -skip-index  # Sla indexeren over (voor development)
```

- `-addr`: poort/adres (default `:8080`)
- `-dataset`: pad naar dataset bestand (default `dataset.txt`)
- `-db-host`: PostgreSQL host (default `localhost`)
- `-db-port`: PostgreSQL poort (default `5432`)
- `-db-user`: database gebruiker (default `postgres`)
- `-db-password`: database wachtwoord (default leeg)
- `-db-name`: database naam (default `odido_parser`)
- `-skip-index`: sla indexeren over bij opstarten (voor development)

## Veelgebruikte API endpoints

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

### Poort controleren:

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

### Database connectie problemen:

```bash
# Test database connectie
psql -h localhost -U postgres -d odido_parser -c "SELECT 1;"
```

### Langzame opstart:

- Schema creatie kan 10-30 seconden duren bij eerste keer
- Index creatie kan minuten duren afhankelijk van data grootte
- Check logs voor gedetailleerde voortgang

## Credits

Gebaseerd op het originele project: https://github.com/stuncs69/totally-not-odido-parser

## Migratie van SQLite

Als je van de oude SQLite versie migreert, zie `MIGRATION_SQLITE_TO_POSTGRES.md` voor instructies.
