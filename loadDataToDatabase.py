import pandas as pd
from sqlalchemy import create_engine

import pymongo

def storeinsqlDB(addedfile, dburl, batchsize=100):
    try:
        conn = create_engine(dburl)
    except Exception as e:
        print("error ", e)

    try:
        data = pd.read_csv(addedfile)
    except Exception as e:
        print("error in reading file ", e)    
               

    datafiltering = data[(data['Device'] == 'Smartphone') & (data['Age'] > 40)]
    required_columns = datafiltering[['Device', 'Age', 'Country']]

    for i in range(0, len(required_columns), batchsize):

        batch = required_columns.iloc[i:i + batchsize]
        batch.to_sql('netflix_user', conn, if_exists='append', index=False)

        print("Batch "+ str(i//batchsize + 1) + " inserted into the database")
    print("all data inserted in database")

def storeinnosql(file, mongoDburl, dbname, collectionname, batchsize=100):

    client = pymongo.MongoClient(mongoDburl)
    db = client[dbname]
    collection = db[collectionname]

    try:
        data = pd.read_csv(file)
    except Exception as e:
        print("error in reading file ", e)    

    datafiltering = data[(data['Device'] =='Smartphone') &(data['Age'] > 40)]
    required_columns = datafiltering[['Device', 'Age', 'Country']]

    for i in range(0, len(required_columns), batchsize):

        batch = required_columns.iloc[i:i + batchsize]
        collection.insert_many(batch.to_dict('records'))
        print("Batch "+ str(i//batchsize + 1) + " inserted into the no sql database")
    print("all data inserted in no sql db")


inputfile = "NetflixUserbase.csv"

choicedDB = input("Enter storage type (sql/nosql): ").strip().lower()

if choicedDB == "sql":
    dburl = input("Enter SQL DB URL: ")
    storeinsqlDB(inputfile, dburl)

elif choicedDB == "nosql":

    mongoDburl = input("Enter Mongodb URL: ")
    dbname = input("Enter Mongodb Database name")
    collectionname = input("Enter collection name")

    storeinnosql(inputfile, mongoDburl, dbname, collectionname)
else:
    print("Invalid choice choosed")
