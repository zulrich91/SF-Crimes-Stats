from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
import logging

logging.getLogger("pykafka.broker").setLevel('ERROR')

client = KafkaClient("localhost:9092")
topic = client.topics["service-calls"]
consumer = topic.get_balanced_consumer(
    consumer_group=b'pykafka-consumer',
    auto_commit_enable=False,
    zookeeper_connect='localhost:2181'
)
for msg in consumer:
    if msg is not None:
        print(msg.offset, msg.value)