import sys

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

DAY_MS = 24 * 60 * 60 * 1000

def main() -> None:
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})

    topics = [
        NewTopic(
            "weather.forecast.hourly",
            num_partitions=6,
            replication_factor=1,
            config={"retention.ms": str(14 * DAY_MS)},
        ),
        NewTopic(
            "vessels.positions",
            num_partitions=6,
            replication_factor=1,
            config={"retention.ms": str(7 * DAY_MS)},
        ),
        NewTopic(
            "vessels.latest",
            num_partitions=3,
            replication_factor=1,
            config={"cleanup.policy": "compact"},
        ),
        NewTopic(
            "prices.futures.daily",
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": "-1"},
        ),
        NewTopic(
            "dlq.ingestion",
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": str(30 * DAY_MS)},
        ),
    ]
    futures = admin.create_topics(topics)

    for name, future in futures.items():
        try:
            future.result()
            print(f"created: {name}")
        except KafkaException as e:
            error = e.args[0]  # KafkaError
            if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"already exists: {name}")
            else:
                print(f"failed {name}: {error.str()}")
                sys.exit(1)

    existing = admin.list_topics(timeout=10).topics
    print(sorted(existing))


if __name__ == "__main__":
    main()