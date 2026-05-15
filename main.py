import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from asyncio_mqtt import Client, MqttError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mqtt_influx_bridge")


@dataclass
class SensorData:
    user: str
    device: str
    measurement: str
    value: Optional[float]
    extra_fields: Dict[str, Any]


def get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


MQTT_BROKER_HOST = get_env("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(get_env("MQTT_BROKER_PORT", "1883"))
MQTT_USERNAME = get_env("MQTT_USERNAME", "")
MQTT_PASSWORD = get_env("MQTT_PASSWORD", "")
MQTT_TOPIC_FILTER = get_env("MQTT_TOPIC_FILTER", "/home/data/+/+/+")
MQTT_TOPIC_REGEX = get_env("MQTT_TOPIC_REGEX", r"/home/data/([^/]+)/([^/]+)/([^/]+)")

INFLUXDB_URL = get_env("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = get_env("INFLUXDB_TOKEN", "supersecrettoken")
INFLUXDB_ORG = get_env("INFLUXDB_ORG", "mqtt-org")
INFLUXDB_BUCKET = get_env("INFLUXDB_BUCKET", "MQTT")


def parse_mqtt_topic(topic: str, payload: str) -> Optional[SensorData]:
    match = re.match(MQTT_TOPIC_REGEX, topic)
    if not match:
        logger.debug("Topic did not match regex: %s", topic)
        return None

    user, device, measurement = match.groups()
    if device == "status":
        logger.debug("Skipping status device message for topic: %s", topic)
        return None

    if payload is None:
        return None

    try:
        numeric = float(payload)
        return SensorData(user=user, device=device, measurement=measurement, value=numeric, extra_fields={})
    except ValueError:
        pass

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return SensorData(user=user, device=device, measurement=measurement, value=None, extra_fields=parsed)
        return SensorData(user=user, device=device, measurement=measurement, value=None, extra_fields={"raw": parsed})
    except json.JSONDecodeError:
        return SensorData(user=user, device=device, measurement=measurement, value=None, extra_fields={"raw": payload})


def build_point(sensor_data: SensorData) -> Point:
    point = Point(sensor_data.measurement).tag("user", sensor_data.user).tag("device", sensor_data.device)

    if sensor_data.value is not None:
        point = point.field("value", sensor_data.value)

    for key, value in sensor_data.extra_fields.items():
        if value is None:
            continue
        if isinstance(value, (int, float, bool, str)):
            point = point.field(key, value)
        else:
            point = point.field(key, json.dumps(value))

    return point


async def write_point(write_api, sensor_data: SensorData) -> None:
    point = build_point(sensor_data)
    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logger.debug("Wrote data to InfluxDB: %s", sensor_data)
    except Exception as exc:
        logger.error("Failed to write to InfluxDB: %s", exc, exc_info=True)


async def handle_messages(write_api) -> None:
    logger.info("Connecting to MQTT broker %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    async with Client(
        hostname=MQTT_BROKER_HOST,
        port=MQTT_BROKER_PORT,
        username=MQTT_USERNAME or None,
        password=MQTT_PASSWORD or None,
    ) as client:
        async with client.unfiltered_messages() as messages:
            await client.subscribe(MQTT_TOPIC_FILTER)
            logger.info("Subscribed to MQTT topic filter: %s", MQTT_TOPIC_FILTER)

            async for msg in messages:
                payload = msg.payload.decode("utf-8", errors="replace")
                logger.info("MQTT message received: %s %s", msg.topic, payload)
                sensor_data = parse_mqtt_topic(msg.topic, payload)
                if sensor_data is None:
                    continue
                await write_point(write_api, sensor_data)


async def main() -> None:
    logger.info("Starting MQTT to InfluxDB bridge")

    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        while True:
            try:
                await handle_messages(write_api)
            except MqttError as exc:
                logger.warning("MQTT error, reconnecting in 5 seconds: %s", exc)
                await asyncio.sleep(5)
            except Exception as exc:
                logger.error("Unexpected error in main loop: %s", exc, exc_info=True)
                await asyncio.sleep(5)
    finally:
        client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("MQTT to InfluxDB bridge stopped by user")
