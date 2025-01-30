#!/usr/bin/env python
import os
from random import choice
from confluent_kafka import Producer

from dotenv import load_dotenv

load_dotenv()

bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
cluster_api_key = os.environ.get('CLUSTER_API_KEY')
cluster_api_secret = os.environ.get('CLUSTER_API_SECRET')

if __name__ == '__main__':


    config = {
        # User-specific properties that you must set
        'bootstrap.servers': bootstrap_server,
        'sasl.username':     cluster_api_key,
        'sasl.password':     cluster_api_secret,

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "purchases"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()