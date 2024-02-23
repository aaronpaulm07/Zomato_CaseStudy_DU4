import os
import pandas as pd
import numpy as np
import ast

# Specify the path to the CSV file you want to convert to a DataFrame
csv_file_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\merged_data.csv"

# Check if the file exists
if os.path.isfile(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

# print(df.columns)

# Check if 'Cuisine' column exists before splitting
if 'CUSINE_CATEGORY' in df.columns:
    # Convert the comma-separated string to a list
    df['CUSINE_CATEGORY'] = df['CUSINE_CATEGORY'].str.split(',')
    # df['CUSINE_CATEGORY'] = df['CUSINE_CATEGORY'].apply(ast.literal_eval) 
    # Write the merged data to a new CSV file
    output_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\cuisine_split.csv"
    df.to_csv(output_path, index=False, sep=',')

    output_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\cuisine_split_COPY.csv"
    df.to_csv(output_path, index=False, sep=',')
    
    print(" completed.  data saved to 'cuisine_split.csv' in the specified directory.")
else:
    print("The 'CUISINE' column does not exist in the DataFrame.")
    