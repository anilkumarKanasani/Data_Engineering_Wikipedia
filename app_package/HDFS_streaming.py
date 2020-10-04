#------------------------ importing packages and libraries ------------------------------
from pymongo import MongoClient
import pandas as pd
from hdfs import InsecureClient
import time
from bson.json_util import dumps
import json
from os.path import join
from datetime import datetime


#------------------- main class to load data into dataframe, to HDFS --------------------
def start_hdfs_streaming():
    while True:

        #---------------------- connection to MongoDB client ----------------------------
        client = MongoClient()
        db = client['DB_filtereddata']
        collection_edits = db['Edit_filtered_collection']
        collection_others = db['other_filtered_collection']

        #-------------------- loading data into pandas dataframe ------------------------
        col_edits = collection_edits.find()
        df_edits = pd.DataFrame(col_edits)
        
        col_others = collection_others.find()
        df_others = pd.DataFrame(col_others)
        
        df = df_edits.append(df_others, ignore_index=True)
        
        if df.empty is False:
            df = df.drop_duplicates()
            print(df.count())
            print(df.head())

            
            #---------------------- connection to HDFS localhost ----------------------------   
            client_hdfs = InsecureClient('http://localhost:50070/', user="wiki")
            print(client_hdfs)
            filename = 'Filtereddata.csv'

            #------------------- writing data into HDFS from dataframe ----------------------
            exist = client_hdfs.status(filename, strict=False)
            print(f'{filename} existing in: {exist}')
            try:
                if exist == None:
                    with client_hdfs.write(filename, encoding = 'utf-8', overwrite=True) as writer:
                        print(writer)
                        df.to_csv(writer)
                        print(f'Data saved in {filename} in {client_hdfs}')
                else:
                    with client_hdfs.write(filename, encoding = 'utf-8', append=True) as writer:
                        print(writer)
                        df.to_csv(writer)
                        print(f'Data appended to existing file {filename} in {client_hdfs}')
            except ValueError:
                pass

                
            #------------------- creating backup file into local storage ----------------------
            records = df_edits.to_dict(orient = 'records')
            now = datetime. now()
            current_time = now. strftime("%H_%M")
            jsonpath = collection_edits.name + '_'+ current_time + ".json"
            jsonpath = join("C:/Backup_Mongo/filtereddata", jsonpath)
            with open(jsonpath, 'w') as jsonfile:
                jsonfile.write(dumps(records))
            print(f'Backup stored in {jsonpath}')
                
            records = df_others.to_dict(orient = 'records')
            jsonpath = collection_others.name+ '_'+ current_time +  ".json"
            jsonpath = join("C:/Backup_Mongo/filtereddata", jsonpath)
            with open(jsonpath, 'w') as jsonfile:
                jsonfile.write(dumps(records))
            print(f'Backup stored in {jsonpath}')

            #---------------- deleting HDFS stored records from the MongoDB ------------------
            records = df_edits.to_dict(orient = 'records')
            record_ids = [record['_id'] for record in records]
            collection_edits.delete_many({'_id':{'$in':record_ids}})
            print('stored edit records in HDFS and deleted from MongoDB')
                
            records = df_others.to_dict(orient = 'records')
            record_ids = [record['_id'] for record in records]
            collection_others.delete_many({'_id':{'$in':record_ids}})
            print('stored other records in HDFS and deleted from MongoDB')
        
        else:
            print(f"no records found in the {collection_edits}")
        
        time.sleep(60)
