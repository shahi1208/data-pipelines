import pandas as pd
import logging
import psycopg2
from sqlalchemy import create_engine

logging.basicConfig(filename='file.log',filemode='w',level=logging.INFO)

try:
    data = pd.read_csv('Uber Request Data.csv')
    logging.info('data loaded')
except:
    logging.error('error in loading the data',exc_info=True)


try:
    data = data.rename({'Request id' :'Request_id',
    'Pickup point':'Pickup_point',
    'Driver id': 'Driver_id',
    'Status':'Status',
    'Request timestamp':'Request_timestamp',
    'Drop timestamp' :'Drop_timestamp'},axis=1)
    logging.info('columns are renamed')

except:
    logging.error('problem in renaming',exc_info=True)

try:
    data['Driver_id'] = pd.to_numeric(data['Driver_id'],errors='coerce')
    data['Request_timestamp'] = pd.to_datetime(data['Request_timestamp'],format='%Y-%m-%d %H:%m:%s',errors='coerce')
    data['Drop_timestamp'] =pd.to_datetime(data['Drop_timestamp'],format='%Y-%m-%d %H:%m:%s',errors='coerce')
    logging.info('data columns are converted')
except:
    logging.error('error in type convertion',exc_info=True)

database = 'postgresql+psycopg2://username:password@localhost:port/dbname'

engine = create_engine(database)
conn = engine.raw_connection()



try:
    data.to_sql('uber',con=engine,index=False,if_exists='replace')
    logging.info('uploaded to database')
except:
    logging.error('error in loading',exc_info=True)
