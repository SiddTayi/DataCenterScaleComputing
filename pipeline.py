import pandas as pd
import numpy as np
import argparse

def extract_data(source):
    print("Extracting data...")
    return pd.read_csv('https://raw.githubusercontent.com/SiddTayi/CSCI-5502-DataMining/main/Assignments/Assignment%206/Power.csv')

def transform_data(data):
    df = data.copy()
    print('Splitting Date and Time')
    df[['Date', 'Time']] = df['DateTime'].str.split(' ', expand=True)
    df.drop('DateTime', axis = 1, inplace = True)
    return df

def load_data(data, target):
    print('Saving data to target...')
    data.to_csv(target)
    print('Target file created.. ')


if __name__ == "__main__":
    #print("Enter: ")
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help = 'source.csv')
    parser.add_argument('target', help = 'target.csv' )
    args = parser.parse_args()

    print('Starting')
    #df = extract_data(args.source)
    # source = 'https://raw.githubusercontent.com/SiddTayi/CSCI-5502-DataMining/main/Assignments/Assignment%206/Power.csv'
    df = extract_data(args.source)
    new_df = transform_data(df)
    load_data(new_df, args.target)
    print('Completed')

# EXAMPLE SOURCE: 'https://shelterdata.s3.amazonaws.com/shelter1000_new.csv'