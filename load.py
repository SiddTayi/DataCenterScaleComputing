import pandas as pd

from sqlalchemy import create_engine, insert, MetaData, Table
import os
from sqlalchemy.dialects.postgresql import insert



# def load_data(table_file, table_name, key):

#     db_url = os.environ['DB_URL']  # 'postgresql+psycopg2://sid:sid@db:5432/shelter_lab3'
#     conn = create_engine(db_url)

   
#     def insert_on_conflict_nothing(table, conn, keys, data_iter):
#         # "key" is the primary key in "conflict_table"
#         data = [dict(zip(keys, row)) for row in data_iter]
#         stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
#         result = conn.execute(stmt)
#         return result.rowcount

#     pd.read_csv(table_file).to_sql(table_name, conn, if_exists="append", index=False)

#     print(table_file + " loaded.")


from transform import transform_data
from sqlalchemy import create_engine
import os

def load_data(url, target_dir):
    # Call the transform_data function from transform.py
    transformed_data = transform_data(url, target_dir)

    new_data, dim_animal, dim_date, dim_breed, dim_outcome_types, fct_outcomes = transformed_data

    print("----------------------------------" * 2)
    print('Saving data')

    # Construct the database URL
    db_host = os.environ.get('DB_HOST', 'db')
    db_url = 'postgresql+psycopg2://sid:sid@' + db_host + ':5432/shelter_lab3'

    # Create the database engine
    conn = create_engine(db_url)
    print("Connected")

    # Use df instead of dim_animal (assuming dim_animal is a DataFrame)
    dim_animal = dim_animal.drop_duplicates(subset=['animal_id'])
    print("Dropped dups")

    # Save tables to SQL
    dim_animal.to_sql('dim_animal', conn, if_exists="append", index=False)
    print("animals saved")

    dim_date.to_sql('dim_date', conn, if_exists="append", index=False)
    print("dates saved")

    dim_breed.to_sql('dim_breed', conn, if_exists="append", index=False)
    print("breed saved")

    dim_outcome_types.to_sql('dim_outcome', conn, if_exists="append", index=False)
    print("outcomes saved")

    fct_outcomes.to_sql('fact_animal', conn, if_exists="append", index=False)

    # Commit changes
    conn.commit()

    print("Saved")
    print("----------------------------------" * 2)

