import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine
import os
#import psycopg2



def extract_data(source):
    print(f'''\nExtracting data''')
    return pd.read_csv(source)



def transform_data(data):
    print(f'''Transforming data''')
    new_data = data.copy()

    # Renaming the column names
    new_data.columns = new_data.columns.str.lower().str.replace(' ', '_')

    # Drop Duplicates
    new_data = new_data.drop_duplicates(subset='animal_id')

    # Cleaning Name 
    new_data['name'] = new_data['name'].str.replace('*', '', regex=False)

    # Splitting monthyear to month and year 
    new_data[['month', 'year']] = new_data['monthyear'].str.split(' ', expand = True)
    new_data[['kind', 'sex']] = new_data['sex_upon_outcome'].str.split(' ', expand=True)
    new_data[['name']] = new_data[['name']].fillna("Nameless")
    new_data[['outcome_subtype']] = new_data[['outcome_subtype']].fillna('NA') 

    new_data['datetime'] = pd.to_datetime(new_data['datetime'], format='%m/%d/%Y %I:%M:%S %p')
    new_data['time'] = new_data['datetime'].dt.time  # Extract the time
    new_data['date'] = new_data['datetime'].dt.date
    new_data = new_data.drop(columns=['datetime']) 

    print(f'''Data Transforming completed''')
    return new_data



def load_data(data):

    # SPLITTING DATASET INTO FACT AND DIMS. 
    df = data.copy()

    # dim_animal
    print('Creating Animal Table.')
    dim_animal = df[['animal_id', 'name', 'date_of_birth', 'animal_type', 'color' , 'kind', 'sex']]
    
    # dim_date
    print("Creating Date Table.")
    dim_date = df[['date', 'time', 'month','year']]
    dim_date['date_id'] = dim_date.index + 1


    # dim_breed
    print("Creating Breed Table")
    dim_breed = df[['breed']]
    dim_breed['breed_id'] = dim_breed.index + 1


    # dim_outcome
    print("Creating Outcome Table")
    dim_outcome = df[['outcome_type', 'outcome_subtype']]
    dim_outcome['outcome_id'] = dim_outcome.index + 1


    # FACT TABLE
    print("Creating FACT Table")

    fact_table = pd.merge(dim_animal, dim_date, left_index=True, right_index=True)
    fct_animal1 = pd.merge(fact_table, dim_breed, left_index=True, right_index=True)
    fact_animal = pd.merge(fct_animal1, dim_outcome, left_index=True, right_index=True)
    fact_animal['fact_id'] = fact_animal.index + 1
    fact_animal = fact_animal[['animal_id', 'date_id', 'breed_id', 'outcome_id', 'fact_id']]

    # Main Code
    print("----------------------------------"*2)
    print('Saving data')
    db_host = os.environ.get('DB_HOST', 'db')
    db_url = 'postgresql+psycopg2://sid:sid@db:5432/shelter'
    conn = create_engine(db_url)
    data.to_sql('sid', conn, if_exists = "append", index = False)
    dim_animal.to_sql('dim_animal', conn, if_exists = "append", index = False)
    dim_date.to_sql('dim_date', conn, if_exists = "append", index = False)
    dim_breed.to_sql('dim_breed', conn, if_exists = "append", index = False)
    dim_outcome.to_sql('dim_outcome', conn, if_exists = "append", index = False)
    fact_animal.to_sql('fact_animal', conn, if_exists = "append", index = False)
    print("Saved")
    print("----------------------------------"*2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help = 'shelter1000.csv')
    #parser.add_argument('target', help = 'shelter1000_transformed.csv' )
    args = parser.parse_args()

    print("----------------------------------"*2)
    print('\nStarting the ETL Process')
    print("Connected to the Db")
    #df = extract_data(args.source)
    # source = 'https://shelterdata.s3.amazonaws.com/shelter1000.csv'
    df = extract_data(args.source)
    new_df = transform_data(df)
    load_data(new_df)
    print('\nProcess Complete....')
