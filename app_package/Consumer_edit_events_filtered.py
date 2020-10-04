#------------------------ importing packages and libraries for consumer ------------------------------
from kafka import KafkaConsumer
from kafka import TopicPartition
from pymongo import MongoClient
from json import loads
from kafka.errors import KafkaError
from datetime import datetime

#------------------------ Setting up a consumer for Edit type filtered events ------------------------------
Consumer_edit_filtered = KafkaConsumer(
    'filtered_edit',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='smallest',
    group_id= 'filtered_data_consumers',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

#------------------------ Getting connection to Mongo server ------------------------------
client = MongoClient('mongodb://localhost:27017')
db = client['DB_filtereddata']
collection = db['Edit_filtered_collection']

#------------------------ Pushing messages to MongoDB collection ------------------------------
def start_consumer_filtered_edit():
    for message in Consumer_edit_filtered:
        message = message.value
        message['timestamp'] = datetime.fromtimestamp(message['timestamp']).strftime("%H:%M")
        message['language'] = message['url'][8:11]
        if (message['language'] == 'www') : message['language'] = 'en.'
        collection.insert_one(message)
        print(message,'added to ',collection)