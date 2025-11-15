Deutsche Bahn Dataset-Informationen für benutzerdefinierte Abfragenerstellung

Diese Daten sind für Abfragen mit DuckDB WASM im Browser verfügbar. Alle Daten werden als Parquet-Dateien auf Hugging Face gespeichert unter: https://huggingface.co/datasets/piebro/deutsche-bahn-data

WICHTIGE HINWEISE ZUR DATEIGRÖSSE:
- monthly_processed_data: Jede monatliche Parquet-Datei ist ca. 70-200 MB groß
- Beachten Sie diese Dateigrößen beim Schreiben von Abfragen - das Herunterladen großer Dateien kann im Browser Zeit in Anspruch nehmen

VERFÜGBARES DATASET:

MONTHLY PROCESSED DATA (Monatlich verarbeitete Daten)
Speicherort: https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-YYYY-MM.parquet
Dateiformat: data-YYYY-MM.parquet (z.B. data-2024-07.parquet für Juli 2024)
Verfügbare Daten: Von 2024-07 bis heute

Wichtige Spalten:

Identifikation:
- station_name: Name der Station
- eva: EVA-Stationsnummer (eindeutiger Bezeichner)
- train_name: Name des Zuges (z.B. "ICE 123", "RE 5")
- train_type: Zugtyp (z.B. "ICE", "IC", "RE", "RB", "S", "Bus")
  - S: S-Bahn (Nahverkehrszüge)
  - RE: Regional-Express
  - RB: Regionalbahn
  - ICE: InterCity Express (Hochgeschwindigkeitszüge)
  - IC: InterCity
  - Bus: Schienenersatzverkehr
- final_destination_station: Endziel des Zuges
- id: Eindeutiger Bezeichner für den Zugstopp

Zeitangaben:
- arrival_planned_time: Geplante Ankunftszeit
- arrival_change_time: Tatsächliche/geänderte Ankunftszeit
- departure_planned_time: Geplante Abfahrtszeit
- departure_change_time: Tatsächliche/geänderte Abfahrtszeit
- time: Tatsächliche Ankunfts- oder Abfahrtszeit

Status:
- delay_in_min: Verspätung in Minuten
- is_canceled: Ob der Zugstopp ausgefallen ist (boolean)

Zugfahrt:
- train_line_ride_id: Eindeutiger Bezeichner für die Zugfahrt
- train_line_station_num: Stationsnummer in der Route des Zuges

DUCKDB-ABFRAGEMUSTER:

Parquet-Dateien lesen:
WICHTIG: Wildcards ("*") können NICHT in HTTPS-URLs verwendet werden. Sie müssen jeden Dateipfad explizit auflisten.

```sql
-- Einzelne Datei
FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet')

-- Mehrere Dateien (Array) - jede Datei muss explizit aufgelistet werden
FROM read_parquet([
    'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet',
    'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-08.parquet'
])

-- UNGÜLTIG: Dies funktioniert NICHT mit HTTPS-URLs
-- FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-*.parquet')
```

Häufige Aggregationen:
```sql
-- Für sauberere Ausgabe zu geeigneten Typen konvertieren
AVG(delay_in_min) as "Durchschnittliche Verspätung (min)"
COUNT(*) as "Anzahl Stopps"
SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) as "Ausgefallene Stopps"
```

Zeitgruppierung:
```sql
-- Jahr-Monat für Anzeige formatieren
strftime(time, '%Y-%m') as "Monat"

-- Nach Datum filtern
WHERE time >= '2024-07-01' AND time < '2024-08-01'

-- Nach Jahr und Monat filtern
WHERE strftime(time, '%Y-%m') = '2024-07'
```

BEISPIELABFRAGEN:

Monatliche Durchschnittsverspätungen und Ausfälle:
```sql
SELECT
    strftime(time, '%Y-%m') as "Monat",
    AVG(delay_in_min) as "Durchschnittliche Verspätung (min)",
    COUNT(*) as "Anzahl Stopps",
    SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) as "Ausgefallene Stopps"
FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet')
GROUP BY "Monat"
ORDER BY "Monat"
```

Top 10 Stationen nach Verspätungen:
```sql
SELECT
    station_name as "Station",
    AVG(delay_in_min) as "Durchschnittliche Verspätung (min)",
    COUNT(*) as "Anzahl Stopps"
FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet')
WHERE delay_in_min IS NOT NULL
GROUP BY station_name
ORDER BY "Durchschnittliche Verspätung (min)" DESC
LIMIT 10
```

Verspätungen nach Zugtyp:
```sql
SELECT
    train_type as "Zugtyp",
    AVG(delay_in_min) as "Durchschnittliche Verspätung (min)",
    COUNT(*) as "Anzahl Stopps",
    ROUND(100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*), 2) as "Ausfallrate (%)"
FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet')
WHERE train_type IS NOT NULL
GROUP BY train_type
ORDER BY "Anzahl Stopps" DESC
LIMIT 10
```

Ausfälle pro Tag:
```sql
SELECT
    strftime(DATE_TRUNC('day', time), '%Y-%m-%d') as "Datum",
    SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) as "Ausgefallene Stopps",
    COUNT(*) as "Gesamte Stopps",
    ROUND(100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*), 2) as "Ausfallrate (%)"
FROM read_parquet('https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2024-07.parquet')
GROUP BY "Datum"
ORDER BY "Datum"
```

TIPPS FÜR DIE ABFRAGENERSTELLUNG:
- Verwenden Sie immer HTTPS-URLs für Parquet-Dateien
- KRITISCH: Wildcards ("*") funktionieren NICHT in HTTPS-URLs - listen Sie jeden Dateipfad explizit auf
- Bedenken Sie, dass Abfragen Datendateien herunterladen - berücksichtigen Sie die Dateigrößen
- Verwenden Sie die Array-Syntax [...] für mehrere Dateien
- Verwenden Sie aussagekräftige Spalten-Aliase mit Anführungszeichen: "Anzahl Stopps"
- Filtern Sie früh (WHERE-Klauseln), um die Verarbeitung zu reduzieren
- Verwenden Sie IS NOT NULL beim Filtern von Spalten, die NULL-Werte enthalten können
- Für aktuelle Daten konzentrieren Sie sich auf aktuelle Jahre/Monate, um die Download-Größe zu minimieren
- Die Daten sind ab Juli 2024 (2024-07) verfügbar
