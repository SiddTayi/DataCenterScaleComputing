import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine
import os
import psycopg2
from pathlib import Path




# # MY CODE:
# # def transform_data(url):
# #     data = pd.read_json(url)
# #     print(f'''Transforming data''')
# #     new_data = data.copy()

# #     # Renaming the column names
# #     new_data.columns = new_data.columns.str.lower().str.replace(' ', '_')

# #     # Drop Duplicates
# #     new_data = new_data.drop_duplicates(subset='animal_id')

# #     # Cleaning Name 
# #     new_data['name'] = new_data['name'].str.replace('*', '', regex=False)

# #     # Splitting monthyear to month and year 
# #     #new_data[['month', 'year']] = new_data['monthyear'].str.split(' ', expand = True)
# #         # Splitting monthyear to month and year 
# #     split_data = new_data['monthyear'].str.split(' ', expand=True)
    
# #     # Check if split produced the expected number of columns
# #     if len(split_data.columns) == 2:
# #         new_data[['month', 'year']] = split_data
# #     else:
# #         # Set default values to np.nan
# #         new_data['month'] = np.nan
# #         new_data['year'] = np.nan

# #     new_data[['kind', 'sex']] = new_data['sex_upon_outcome'].str.split(' ', expand=True)
# #     new_data[['name']] = new_data[['name']].fillna("Nameless")
# #     new_data[['outcome_subtype']] = new_data[['outcome_subtype']].fillna('NA') 

# #     new_data['datetime'] = pd.to_datetime(new_data['datetime'], format='%m/%d/%Y %I:%M:%S %p')
# #     new_data['time'] = new_data['datetime'].dt.time  # Extract the time
# #     new_data['date'] = new_data['datetime'].dt.date
# #     new_data = new_data.drop(columns=['datetime']) 

# #     print(f'''Data Transforming completed''')
# #     return new_data



# # TEST CODE
# def transform_data(url, target_dir):
#     data = pd.read_json(url)
#     print(f'''Transforming data''')
#     new_data = data.copy()

#     # Renaming the column names
#     new_data.columns = new_data.columns.str.lower().str.replace(' ', '_')
#     print("Lowered the case")

#     # Drop Duplicates
#     new_data = new_data.drop_duplicates(subset='animal_id')
#     print("Dropped duplicates and Na.")

#     # Cleaning Name 
#     new_data['name'] = new_data['name'].str.replace('*', '', regex=False)

#     # Splitting monthyear to month and year 
#     split_data = new_data['monthyear'].str.split(' ', expand=True)
    
#     # Check if split produced the expected number of columns
#     if len(split_data.columns) == 2:
#         new_data[['month', 'year']] = split_data
#     else:
#         # Set default values to np.nan
#         new_data['month'] = np.nan
#         new_data['year'] = np.nan

#     # Handle other transformations
#     new_data[['kind', 'sex']] = new_data['sex_upon_outcome'].str.split(' ', expand=True)
#     new_data[['name']] = new_data[['name']].fillna("Nameless")
#     new_data[['outcome_subtype']] = new_data[['outcome_subtype']].fillna('NA') 

#     new_data['datetime'] = pd.to_datetime(new_data['datetime'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
#     new_data['time'] = new_data['datetime'].dt.time  # Extract the time
#     new_data['date'] = new_data['datetime'].dt.date
#     new_data = new_data.drop(columns=['datetime']) 

#     print(f'''Data Transformation completed''')
#     print(f'....................................END........................................')
#     return new_data
    




# def load_data(data):

#     # SPLITTING DATASET INTO FACT AND DIMS. 
#     df = data.copy()

#     # dim_animal
#     print('Creating Animal Table.')
#     dim_animal = df[['animal_id', 'name', 'date_of_birth', 'animal_type', 'color' , 'kind', 'sex']]
    
#     # dim_date
#     print("Creating Date Table.")
#     dim_date = df[['date', 'time', 'month','year']]
#     dim_date['date_id'] = dim_date.index + 1


#     # dim_breed
#     print("Creating Breed Table")
#     dim_breed = df[['breed']]
#     dim_breed['breed_id'] = dim_breed.index + 1


#     # dim_outcome
#     print("Creating Outcome Table")
#     dim_outcome = df[['outcome_type', 'outcome_subtype']]
#     dim_outcome['outcome_id'] = dim_outcome.index + 1


#     # FACT TABLE
#     print("Creating FACT Table")

#     fact_table = pd.merge(dim_animal, dim_date, left_index=True, right_index=True)
#     fct_animal1 = pd.merge(fact_table, dim_breed, left_index=True, right_index=True)
#     fact_animal = pd.merge(fct_animal1, dim_outcome, left_index=True, right_index=True)
#     fact_animal['fact_id'] = fact_animal.index + 1
#     fact_animal = fact_animal[['animal_id', 'date_id', 'breed_id', 'outcome_id', 'fact_id']]

#     # Main Code
#     print("----------------------------------"*2)
#     print('Saving data')
#     db_host = os.environ.get('DB_HOST', 'db')
#     db_url = 'postgresql+psycopg2://sid:sid@db:5432/shelter'
#     conn = create_engine(db_url)
#     data.to_sql('sid', conn, if_exists = "append", index = False)
#     dim_animal.to_sql('dim_animal', conn, if_exists = "append", index = False)
#     dim_date.to_sql('dim_date', conn, if_exists = "append", index = False)
#     dim_breed.to_sql('dim_breed', conn, if_exists = "append", index = False)
#     dim_outcome.to_sql('dim_outcome', conn, if_exists = "append", index = False)
#     fact_animal.to_sql('fact_animal', conn, if_exists = "append", index = False)
#     print("Saved")
#     print("----------------------------------"*2)



# Modified code

import pandas as pd
from sqlalchemy import create_engine
import os

def prep_data(data):
    # Perform transformations on the data
    new_data = data.copy()
    print(new_data.shape)
    # Renaming the column names
    new_data.columns = new_data.columns.str.lower().str.replace(' ', '_')

    # Drop Duplicates
    new_data = new_data.drop_duplicates(subset='animal_id')

    # Cleaning Name 
    new_data['name'] = new_data['name'].str.replace('*', '', regex=False)

    # Splitting monthyear to month and year 
    split_data = new_data['monthyear'].str.split(' ', expand=True)
    
    # Check if split produced the expected number of columns
    if len(split_data.columns) == 2:
        new_data[['month', 'year']] = split_data
    else:
        # Set default values to np.nan
        new_data['month'] = np.nan
        new_data['year'] = np.nan

    # Handle other transformations
    new_data[['kind', 'sex']] = new_data['sex_upon_outcome'].str.split(' ', expand=True)
    new_data[['name']] = new_data[['name']].fillna("Nameless")
    new_data[['outcome_subtype']] = new_data[['outcome_subtype']].fillna('NA') 

    new_data['datetime'] = pd.to_datetime(new_data['datetime'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
    new_data['time'] = new_data['datetime'].dt.time  # Extract the time
    new_data['date'] = new_data['datetime'].dt.date
    new_data = new_data.drop(columns=['datetime']) 

    return new_data

def prep_animal(data):
    # Extract relevant columns for dim_animal
    data = data.drop_duplicates(subset='animal_id')
    dim_animal = data[['animal_id', 'name', 'date_of_birth', 'animal_type', 'color', 'kind', 'sex']]
    

    return dim_animal

def prep_date(data):
    # Extract relevant columns for dim_date
    dim_date = data[['date', 'time', 'month', 'year']]
    dim_date['date_id'] = dim_date.index + 1

    return dim_date

def prep_breed(data):
    # Extract relevant columns for dim_breed
    dim_breed = data[['breed']]
    dim_breed['breed_id'] = dim_breed.index + 1

    return dim_breed

def prep_outcome_types(data):
    # Extract relevant columns for dim_outcome_types
    dim_outcome_types = data[['outcome_type', 'outcome_subtype']]
    dim_outcome_types['outcome_id'] = dim_outcome_types.index + 1

    return dim_outcome_types

def prep_outcomes_fct(data):
    # Extract relevant columns for fact table
    fact_table = pd.merge(prep_animal(data), prep_date(data), left_index=True, right_index=True)
    fct_animal1 = pd.merge(fact_table, prep_breed(data), left_index=True, right_index=True)
    fact_animal = pd.merge(fct_animal1, prep_outcome_types(data), left_index=True, right_index=True)
    fact_animal['fact_id'] = fact_animal.index + 1
    fact_animal = fact_animal[['animal_id', 'date_id', 'breed_id', 'outcome_id', 'fact_id']]

    return fact_animal

def transform_data(url, target_dir):
    data = pd.read_json(url)
    print(f'''Transforming data''')

    # Use separate functions for different transformations
    new_data = prep_data(data)
    dim_animal = prep_animal(new_data)
    dim_date = prep_date(new_data)
    dim_breed = prep_breed(new_data)
    dim_outcome_types = prep_outcome_types(new_data)
    fct_outcomes = prep_outcomes_fct(new_data)


    Path(target_dir).mkdir(parents=True, exist_ok=True)

    # Save the dimensions to CSV files
    dim_animal.to_csv(os.path.join(target_dir, 'dim_animals.csv'), index=False)
    dim_date.to_csv(os.path.join(target_dir, 'dim_dates.csv'), index=False)
    dim_breed.to_csv(os.path.join(target_dir, 'dim_breed.csv'), index=False)
    dim_outcome_types.to_csv(os.path.join(target_dir, 'dim_outcome_types.csv'), index=False)

    print(f'''\n........\n\nData Transformation completed\n\n........''')
    print(f'\n....................................\nEND\n........................................')
    return new_data, dim_animal, dim_date, dim_breed, dim_outcome_types, fct_outcomes


def load_data(data):
    # SPLITTING DATASET INTO FACT AND DIMS. 
    df = data.copy()

    # dim_animal
    print('Creating Animal Table.')
    dim_animal = prep_animal(df)
    
    # dim_date
   
