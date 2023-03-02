import json
import random
import uuid
import datetime

from kafka import KafkaProducer

topic = 'clickstream'
event_types = ['page_view', 'button_click', 'ad_click']

def on_success(metadata):
    print(f"Message produced with the offset: {metadata.offset}")

def on_error(error):
    print(f"An error occurred while publishing the message. {error}")

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

# Produce 10 clickstream events
for i in range(0,10):
    message = {
        "id" : str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "ts": str(datetime.datetime.now())
    }

    future = producer.send(topic, message)

    # Add async callbacks to handle both successful and failed message deliveries
    future.add_callback(on_success)
    future.add_errback(on_error)

producer.flush()
producer.close()


