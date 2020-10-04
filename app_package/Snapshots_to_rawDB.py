#------------------------ importing packages and libraries ------------------------------
import pandas as pd
from pymongo import MongoClient
from os.path import join
from datetime import datetime
import time
from bson.json_util import dumps
import json

#------------------------ Connecting to MongoDB server & Raw Database ------------------------------
client = MongoClient('mongodb://localhost:27017')
db = client['DB_rawdata']

def snapshot():
    #------------------------ Getting all the current records from DB **edit** collection ------------------------------
    collection_edits = db['Edit_raw_collection']
    col_edits = collection_edits.find()
    df_edits = pd.DataFrame(col_edits)
    
    #------------------------ Dumping to local storage ------------------------------
    if df_edits.empty is False:
        print(df_edits.count())
        print(df_edits.head())
        records = df_edits.to_dict(orient = 'records')
        now = datetime.now()
        current_time = now.strftime("%H_%M")
        filename = collection_edits.name + '_' + current_time + '.json'
        jsonpath = join("C:/Backup_Mongo/rawdata" , filename)
        with open (jsonpath , 'w') as jsonfile:
            jsonfile.write(dumps(records))
        print('***** Backup for edit raw records stored in ',jsonpath)
        
        #------------------------ Deleting records from Mongo ------------------------------
        record_ids = [record['_id'] for record in records]
        collection_edits.delete_many({'_id':{'$in':record_ids}})
        print('!!!!!!Deleted Edit records in MongoDB')
    else : print('No records found in' , collection_edits.name)
    
    #------------------------ Getting all the current records from DB **others** collection ------------------------------
    collection_others = db['other_rawdata_collection']
    col_others = collection_others.find()
    df_others = pd.DataFrame(col_others)
    
    #------------------------ Dumping to local storage ------------------------------
    if df_others.empty is False:
        print(df_others.count())
        print(df_others.head())
        records = df_others.to_dict(orient = 'records')
        now = datetime.now()
        current_time = now.strftime("%H_%M")
        filename = collection_others.name + '_' + current_time + '.json'
        jsonpath = join("C:/Backup_Mongo/rawdata" , filename)
        with open (jsonpath , 'w') as jsonfile:
            jsonfile.write(dumps(records))
        print('***** Backup for other raw records stored in ',jsonpath)
        #------------------------ Deleting records from Mongo ------------------------------
        record_ids = [record['_id'] for record in records]
        collection_others.delete_many({'_id':{'$in':record_ids}})
        print('!!!!!!Deleted other records in MongoDB')
    else : 
        print('No records found in' , collection_others.name)
    
    time.sleep(60)
