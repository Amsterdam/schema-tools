"""Kafka consumer."""
from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Consumer, Message
from sqlalchemy.engine import Connection

from schematools.events.full import EventsProcessor
from schematools.types import DatasetSchema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _to_env_name(name: str) -> str:
    """Convert param name to env var name."""
    return name.replace(".", "_").upper()


def _fetch_consumer_params() -> dict:
    """Create the parameters for the consumer.

    A parameter is only included, if the associated env. var is available.
    """
    params = {}
    for name in (
        "bootstrap.servers",
        "sasl.mechanisms",
        "security.protocol",
        "sasl.username",
        "sasl.password",
        "group.id",
        "auto.offset.reset",
    ):
        if (env_name := _to_env_name(name)) is not None:
            if env_name in os.environ:
                params[name] = os.environ[env_name]
    return params


def consume_events(
    dataset_schemas: list[DatasetSchema],
    srid: str,
    connection: Connection,
    topics: tuple(str),
    truncate: bool = False,
):
    """Consume events.

    Main entry point for this module.
    """
    logger.debug("Starting script..")

    # Create Consumer instance 'auto.offset.reset=earliest'
    # to start reading from the beginning of the topic if no committed offsets exist
    consumer = Consumer(_fetch_consumer_params())
    # consumer = Consumer(
    #     {
    #         "bootstrap.servers": BOOTSTRAP_SERVERS,
    #         "auto.offset.reset": AUTO_OFFSET_RESET,
    #         "group.id": "test",
    #     }
    # )

    logger.debug("Consumer made")
    # Transform and Load

    importer = EventsProcessor(dataset_schemas, srid, connection, truncate=truncate)
    logger.debug("Created importer")
    # Subscribe to topic, confluent_kafka explictly needs a list
    consumer.subscribe(list(topics))
    logger.debug("Starting consuming..")
    # Process messages
    try:
        while True:
            msg = consumer.poll(10.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to `session.timeout.ms`
                # for the consumer group to rebalance and start consuming
                logger.debug("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                logger.error("error: {}".format(msg.error()))
            else:
                process_message(importer, msg)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


def process_message(importer: EventsProcessor, msg: Message):
    """Process a message."""
    headers = msg.headers() or {}
    # Convert list of tuples to dict
    header_data = dict(headers)
    # Convert byte values to string values
    for key in header_data.keys():
        header_data[key] = header_data[key].decode("utf8")

    # Convert byte value to string value
    record_value = msg.value().decode("utf8")
    event_data = json.loads(record_value)

    logger.debug(msg.key())

    # Transform and load event into db
    importer.process_event(
        msg.key(),
        header_data,
        event_data,
    )


if __name__ == "__main__":
    consume_events()
