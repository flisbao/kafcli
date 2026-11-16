#!/usr/bin/env python3
"""Send Avro events to an Event Hub, encoded with the Azure Schema Registry.

Part of the kafcli Event Hubs spike (docs/spike/0001-event-hubs-runbook.md).
Authentication uses DefaultAzureCredential (for example `az login`).
No secret is accepted as an argument.
"""

import argparse
import logging
import sys

from azure.core.exceptions import AzureError
from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from azure.identity import DefaultAzureCredential
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder import (
    AvroEncoder,
    InvalidContentError,
    InvalidSchemaError,
)

SCHEMA = (
    '{"type":"record","name":"ProbeEvent","namespace":"kafcli.spike",'
    '"fields":[{"name":"id","type":"int"},{"name":"note","type":"string"}]}'
)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Send Avro events to an Event Hub, encoded with the Azure Schema Registry. "
        "Authenticates with DefaultAzureCredential; no secret arguments."
    )
    parser.add_argument(
        "--namespace",
        required=True,
        metavar="FQDN",
        help="fully qualified namespace, for example <name>.servicebus.windows.net",
    )
    parser.add_argument(
        "--event-hub", required=True, metavar="NAME", help="Event Hub name (the Kafka topic)"
    )
    parser.add_argument(
        "--schema-group", required=True, metavar="NAME", help="Schema Registry group in Avro format"
    )
    parser.add_argument(
        "--count", type=int, default=3, metavar="N", help="number of events to send (default: 3)"
    )
    return parser.parse_args(argv)


def fail(message):
    print(f"error: {message}", file=sys.stderr)
    return 1


def main(argv=None):
    args = parse_args(argv)
    if args.count < 1:
        return fail("--count must be at least 1")
    # The credential chain logs a warning per source; the final error line says what failed.
    logging.getLogger("azure.identity").setLevel(logging.ERROR)

    try:
        credential = DefaultAzureCredential()
        registry = SchemaRegistryClient(args.namespace, credential)
        encoder = AvroEncoder(client=registry, group_name=args.schema_group, auto_register=True)
        with EventHubProducerClient(
            fully_qualified_namespace=args.namespace,
            eventhub_name=args.event_hub,
            credential=credential,
        ) as producer:
            for event_id in range(1, args.count + 1):
                encoded = encoder.encode(
                    {"id": event_id, "note": f"kafcli spike event {event_id}"}, schema=SCHEMA
                )
                event = EventData(encoded["content"])
                event.content_type = encoded["content_type"]
                batch = producer.create_batch()
                batch.add(event)
                producer.send_batch(batch)
                schema_id = encoded["content_type"].split("+", 1)[1]
                print(
                    f"sent {event_id}/{args.count} content_type={encoded['content_type']} "
                    f"schema_id={schema_id}"
                )
    except (AzureError, EventHubError, InvalidSchemaError, InvalidContentError) as exc:
        return fail(" ".join(str(exc).split()) or type(exc).__name__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
