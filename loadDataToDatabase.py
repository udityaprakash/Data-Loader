import pandas as pd
from sqlalchemy import sql

def storeinsqlDB(addedfile, dburl, batchsize=100):

    conn = sql.create_engine(dburl)

    data = pd.read_csv(addedfile)
    datafiltering = data[(data['Device'] == 'Smartphone') & (data['Age'] > 40)]
    required_columns = datafiltering[['Device', 'Age', 'Country']]

    for i in range(0, len(required_columns), batchsize):

        batch = required_columns.iloc[i:i + batchsize]
        batch.to_sql('netflix_user', conn, if_exists='append', index=False)

        print("Batch "+ (i//batchsize + 1) + " inserted into the database")
    print("all data inserted in database")

inputfile = "NetflixUserbase.csv"

userchoice = input("Enter storage type (sql/nosql): ").strip().lower()

db_url = input("Enter SQL DB URL: ")
storeinsqlDB(inputfile, db_url)
