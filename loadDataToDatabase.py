import pandas as pd
from sqlalchemy import create_engine

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

inputfile = "NetflixUserbase.csv"

db_url = input("Enter SQL DB URL: ")
storeinsqlDB(inputfile, db_url, 70)
