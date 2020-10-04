from time import sleep
from app_package.Producer import start_producer
from app_package.Consumer_edit_events_raw_data import start_consumer_raw_edit
from app_package.Consumer_edit_events_filtered import start_consumer_filtered_edit
from app_package.Consumer_other_events_rawdata import start_consumer_raw_others
from app_package.HDFS_streaming import start_hdfs_streaming
from app_package.Snapshots_to_rawDB import snapshot


# Getting messages from API and pushing into respective Kafka topics
start_producer()
sleep(20)


# Starting the consumer for edit_events_raw data and filtering it and pusching to filtered Kafka Topic
start_consumer_raw_edit()
sleep(20)

# Starting the consumer for edit_events_filtered data
start_consumer_filtered_edit()
sleep(20)


# Starting the consumer for other_events_raw data and filtering it and pusching to filtered Kafka Topic
start_consumer_raw_others()
sleep(20)

# Pusching Data into Hadoop server
start_hdfs_streaming()
sleep(20)

# Creating snapshots for Raw data
snapshot()