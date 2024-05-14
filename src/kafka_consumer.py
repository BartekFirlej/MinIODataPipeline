from confluent_kafka import Consumer, KafkaException, KafkaError
from config.kafka_config import KAFKA_BROKER, KAFKA_GROUP_ID, KAFKA_TOPICS
from src.data_processor import process_and_store_message

def consume_messages():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', [f"{p.topic}:{p.partition}" for p in partitions])

    consumer.subscribe(KAFKA_TOPICS, on_assign=print_assignment)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'Reached end of partition {msg.partition()} at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(
                    f'Received message from topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}: key={msg.key()} value={msg.value().decode("utf-8")}')
                process_and_store_message(msg.value().decode("utf-8"))
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
