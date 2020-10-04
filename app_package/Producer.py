# ------------------------ importing packages and libraries ------------------------------
from kafka import KafkaProducer
from json import dumps
import json
from sseclient import SSEClient as EventSource

# ------------------------ Setting up a producer instance with configurations ------------------------------
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# ------------------------ Getting messages from API and pushing into respective topics ------------------------------
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def start_producer():
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                continue

        else:
            if (change['type'] == 'edit'):
                producer.send('raw_edit', value=change)
            else:
                producer.send('raw_other', value=change)