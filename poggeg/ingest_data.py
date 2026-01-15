#!/usr/bin/env python
# coding: utf-8

# In[18]:
import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# In[19]:
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     prefix + '/yellow_tripdata_2021-01.csv.gz',
#     nrows=100,
#     dtype=dtype,
#     parse_dates=parse_dates
# )


def lookup_etl(engine, chunksize):
    lookupdtype = {
        "LocationID": "Int64",
        "Borough": "string",
        "Zone": "string",
        "service_zone": "string",
    }
    # Logic to switch between CSV and Parquet
    # Note: Many modern NYC Taxi datasets are now provided as .parquet
    
    # We can try to detect which one to use or just set it here
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    # --- INGESTION LOGIC ---
    # CSV uses the iterator approach you already had
    df_iter = pd.read_csv(url, dtype=lookupdtype, iterator=True, chunksize=chunksize)
    print("extracting and transforming lookup dataset...")
    first = True
    for df_chunk in tqdm(df_iter):
        if_exists_val = 'replace' if first else 'append'
        df_chunk.to_sql(name='taxi_zone_lookup', con=engine, if_exists=if_exists_val, index=False)
        first = False

# In[21]:
@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default='5432')
@click.option('--pg-db', default='ny_taxi')
@click.option('--year', default=2021, type=int)
@click.option('--month', default=1, type=int)
@click.option('--chunksize', default=100000, type=int)
@click.option('--target-table', default='yellow_taxi_data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    
    # Logic to switch between CSV and Parquet
    # Note: Many modern NYC Taxi datasets are now provided as .parquet
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    # We can try to detect which one to use or just set it here
    # file_name = f'yellow_tripdata_{year}-{month:02d}.parquet' 
    file_name = f'green_tripdata_{year}-{month:02d}.parquet' 
    url = f'{prefix}/{file_name}'
    print(url)

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    if (target_table == 'taxi_zone_lookup'):
        lookup_etl(engine=engine, chunksize=chunksize)
    else:
        # --- INGESTION LOGIC ---
        if url.endswith('.csv') or url.endswith('.csv.gz'):
            # CSV uses the iterator approach you already had
            df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
            
            first = True
            for df_chunk in tqdm(df_iter):
                if_exists_val = 'replace' if first else 'append'
                df_chunk.to_sql(name=target_table, con=engine, if_exists=if_exists_val, index=False)
                first = False

        elif url.endswith('.parquet'):
            # Parquet doesn't support chunksize in read_parquet easily. 
            # For a learning project, reading it all at once is usually okay for monthly files (~100MB).
            print("Reading Parquet file...")
            df = pd.read_parquet(url)
            # print(len(df))
            # count_max1_mile = (df['trip_distance'] <= 1).sum()
            # print(count_max1_mile)
            # We manually chunk the dataframe to keep the progress bar and memory safe
            # stop etl
            print(f"Ingesting {len(df)} rows in chunks of {chunksize}...")
            for i in tqdm(range(0, len(df), chunksize)):
                df_chunk = df.iloc[i:i+chunksize]
                if_exists_val = 'replace' if i == 0 else 'append'
                df_chunk.to_sql(name=target_table, con=engine, if_exists=if_exists_val, index=False)
    

if __name__ == '__main__':
    run()


# %%

