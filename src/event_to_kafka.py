import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import logging
import random


def serializer(message):
    """
    Encoding the message into json format.

    :param message: Message
    """
    return json.dumps(message).encode('utf-8')


def event_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=['broker:9092'],
        value_serializer=serializer
    )

    with open('src/data_events.json', 'r') as f:
        datas = json.load(f)

    random.shuffle(datas)

    for item in datas:
        item['timestamp'] = int(datetime.now().timestamp() * 1000)
        producer.send("event_message", item)
        logging.warning(item)
    
        time.sleep(3)
    
    producer.flush()
    producer.close()


if __name__ == '__main__':
    time.sleep(20)
    event_to_kafka()

