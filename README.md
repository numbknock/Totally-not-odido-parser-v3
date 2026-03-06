# Totally-not-odido-parser-v2

Lokale viewer voor grote `dataset.txt` bestanden.

Je start 1 commando, de webapp komt direct online, en de data wordt op de achtergrond ingelezen en geïndexeerd.

## Belangrijk om te weten

- De webinterface is meteen beschikbaar op `http://localhost:8080`.
- Ook tijdens de **eerste ingest** kun je al zoeken en records bekijken.
- Hoe langer ingest/indexeren loopt, hoe completer en sneller de resultaten worden.
- Stop je het proces tussendoor, dan hervat het later automatisch vanaf de laatste batch.

## Snel starten

```bash
go run ./cmd/server -dataset dataset.txt -db dataset.sqlite
```

Daarna open je:

- `http://localhost:8080/` (zoekscherm)
- `http://localhost:8080/analytics` (analyses)

## Wat doet de app?

- Leest regels uit `dataset.txt`.
- Slaat ze lokaal op in `dataset.sqlite`.
- Maakt zoeken/filteren snel via indexen.
- Toont voortgang in de UI (rechtsboven) en periodiek in de CLI.

## Nieuw in deze fork (kort)

- Webinterface is direct beschikbaar, ook terwijl de eerste ingest nog loopt.
- Records zijn al doorzoekbaar tijdens ingest; je hoeft niet te wachten op 100%.
- Hervatten na herstart: indexeren gaat verder vanaf de laatste batch.
- Snellere standaard ingest-instellingen (fast-index altijd aan, batch op `50000`).
- Betere keyword-zoeking over meer velden (met `fts-broad` standaard aan).
- Verbeterde UI met indexstatus, communicatie/opmerkingen-weergave en analytics.

## Eerste keer opstarten

Als er nog geen `dataset.sqlite` bestaat:

- maakt de app automatisch de database aan;
- start ingest/indexeren vanzelf;
- blijft de webinterface gewoon bruikbaar tijdens dit proces.

## Volgende starts

- Bestaat er al een geldige database, dan is alles direct klaar.
- Is de dataset veranderd, dan start automatisch een rebuild.

## Standaard gedrag

- Fast-index staat altijd aan (voor zo snel mogelijke build).
- Default commit batch is `50000`.
- JSON-velden worden standaard mee geïndexeerd.

## Handige opties

```bash
go run ./cmd/server -addr :8080 -dataset dataset.txt -db dataset.sqlite
```

- `-addr`: poort/adres (default `:8080`)
- `-dataset`: pad naar dataset (default `dataset.txt`)
- `-db`: pad naar sqlite bestand (default `dataset.sqlite`)
- `--fts-broad`: bredere keyword-index (default `true`)

## Veelgebruikte API endpoints

- `GET /api/health`
- `GET /api/index/status`
- `GET /api/stats`
- `GET /api/records?...`
- `GET /api/analytics/distribution?field=email_domain&limit=25&not_empty=true`

## Credits

Gebaseerd op het originele project: https://github.com/stuncs69/totally-not-odido-parser
