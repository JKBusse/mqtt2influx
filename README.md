# MQTT to InfluxDB Bridge

Dieses Projekt ist eine moderne Version des MQTT1.py-Skripts. Es liest MQTT-Nachrichten ein, parst den Topic-Pfad und schreibt die Sensordaten in eine InfluxDB.

Der Container enthält nur das Python-Skript. MQTT-Broker und InfluxDB müssen extern laufen.

## Enthaltene Dateien

- `main.py` — moderner `asyncio`-basierten MQTT-InfluxDB-Bridge.
- `Dockerfile` — Container-Build für das Python-Skript.
- `docker-compose.yml` — startet nur den Bridge-Service.
- `requirements.txt` — Python-Abhängigkeiten.
- `.env.example` — Beispiel-Umgebungsvariablen.

## Schnellstart

1. Passe `.env.example` an deine externen Services an und kopiere zu `.env`:
   ```bash
   cp .env.example .env
   # Bearbeite .env mit deinen MQTT- und InfluxDB-Details
   ```

2. Docker Compose starten:
   ```bash
   docker compose up --build
   ```

Der Bridge-Service verbindet sich mit dem externen MQTT-Broker und schreibt Daten in die externe InfluxDB.

## Konfiguration

Passe die Umgebungsvariablen in `.env` an:

- `MQTT_BROKER_HOST` — Host des MQTT-Brokers
- `MQTT_BROKER_PORT` — Port des MQTT-Brokers
- `MQTT_USERNAME` / `MQTT_PASSWORD` — Falls Authentifizierung erforderlich
- `MQTT_TOPIC_FILTER` — MQTT-Topic-Filter
- `MQTT_TOPIC_REGEX` — Regex zum Parsen des Topics
- `INFLUXDB_URL` — URL der InfluxDB
- `INFLUXDB_TOKEN` — InfluxDB-Token
- `INFLUXDB_ORG` — InfluxDB-Organisation
- `INFLUXDB_BUCKET` — InfluxDB-Bucket
- `LOG_LEVEL` — Logging-Level
# mqtt2influx
