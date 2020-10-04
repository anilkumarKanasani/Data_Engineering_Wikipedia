#------------------------ importing packages and libraries for consumer ------------------------------
from kafka import KafkaConsumer
from kafka import TopicPartition
from pymongo import MongoClient
from json import loads
from kafka.errors import KafkaError

#------------------------ importing packages and libraries for producer ------------------------------
from kafka import KafkaProducer
from json import dumps
import json
from sseclient import SSEClient as EventSource

#------------------------ Setting up a consumer for Edit type raw data ------------------------------
Consumer_edit_raw = KafkaConsumer(
    'raw_edit',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='smallest',
    group_id= 'rawdata_consumers',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)


#------------------------ Getting connection to Mongo server ------------------------------
client = MongoClient('mongodb://localhost:27017')
db = client['DB_rawdata']
collection = db['Edit_raw_collection']



#------------------------ Setting up a producer for Edit type filtered data ------------------------------
producer_edit_filtered = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))



def start_consumer_raw_edit():
    for message in Consumer_edit_raw:
        message = message.value
        try :
            message['schema'] = message['$schema']
            del message['$schema']
        except: pass
    #------------------------ Inserting all messages into MongoDB Rawdata ------------------------------    
        collection.insert_one(message)
        print(message,'added to ',collection)
    #------------------------ Filtering the same data & pushing into filtered producer ------------------------------    
        newdata = {'url':message['meta']['uri'], 
                        'date':message['meta']['dt'],
                        'type':message['type'],
                        'timestamp':message['timestamp'],
                        'user':message['user'], 
                        'title':message['title'], 
                        'comment':message['comment'], 
                        'bot':message['bot']}
        
        producer_edit_filtered.send('filtered_edit', value=newdata)