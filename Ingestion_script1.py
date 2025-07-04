import pandas as pd 
import sqlite3 as sq
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode='a'
)

engine= create_engine('sqlite:///newdata.db')

def ingest_db(chunk,table_name,engine):
    chunk.to_sql(name=table_name,con=engine,if_exists='append',index=False)

def ingestion_db():
    start=time.time()
    
    for file in os.listdir('data'):
        csize=100000
        if(".csv") in file:
            data_1=os.path.join('data',file)
            for chunk in pd.read_csv(data_1, chunksize=csize):
                logging.info(f'ingesting {chunk}in db')
                ingest_db(chunk, file[:-4], engine) 

    end=time.time()
    ttime=end-start/60
    logging.info('Ingestion_completed')
    logging.info(f'time taken {ttime}')

if __name__=='__main__':
    ingestion_db()