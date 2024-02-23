import os
import pandas as pd
import logging
from SQL_connection import connect_to_database,log_subprocess_message
from datetime import datetime

log_file = r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\Log\LOG-Pipeline.txt"
# Configure logging
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def merge(UPLOAD,connection):
    base_directory = os.listdir(UPLOAD)
    if len(base_directory) == 1 and os.path.isdir(os.path.join(UPLOAD, base_directory[0])):
        folder_name = base_directory[0]
        # logging.info("Folder found: %s", folder_name)
        merge_start_time = datetime.now()
        merged_data = pd.DataFrame()
        base_directory = os.path.join(UPLOAD, folder_name)

        for folder in os.listdir(base_directory):
            folder_path = os.path.join(base_directory, folder)
            if os.path.isdir(folder_path):
                # print(f"Processing folder: {folder}")
                csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

                for csv_file in csv_files:
                    file_path = os.path.join(folder_path, csv_file)
                    data = pd.read_csv(file_path, sep='|')
                    merged_data = pd.concat([merged_data, data], ignore_index=True)

        merge_end_time = datetime.now()
        merge_runtime = merge_end_time - merge_start_time
        log_subprocess_message(connection, "MERGING", merge_start_time, merge_end_time, merge_runtime, "Success")
        logging.info("Count of records: %s", len(merged_data))
        logging.info("Merging completed. Merged data saved to 'merged_data.csv' in the specified directory.")
        return merged_data
    else:
        logging.critical("Error: There should be exactly one folder in the directory.")
        print("UPLOAD FAILED - There should be exactly one folder in the directory.")
        log_subprocess_message(connection, "MERGING FAILED", datetime.now(), None, None, "There should be exactly one folder in the directory.")

def cleaning(merged_data, connection):
    # Data Cleaning
    cleaning_start_time = datetime.now()
    merged_data = merged_data.drop(['PAGE NO', 'TIMING', 'RATING_TYPE'], axis=1)
    merged_data['RATING'] = pd.to_numeric(merged_data['RATING'], errors='coerce').fillna(0.0)
    merged_data['CUSINE TYPE'] = merged_data['CUSINE TYPE'].replace('none', 'Unknown')
    merged_data = merged_data.drop_duplicates(subset=['URL'], keep='last')
    merged_data['VOTES'] = pd.to_numeric(merged_data['VOTES'], errors='coerce')
    merged_data['VOTES'] = merged_data['VOTES'].fillna(0)
    merged_data['CUSINE_CATEGORY'] = merged_data['CUSINE_CATEGORY'].str.split(',')
    cleaning_end_time = datetime.now()
    cleaning_runtime = cleaning_end_time - cleaning_start_time
    log_subprocess_message(connection, "CLEANING", cleaning_start_time, cleaning_end_time, cleaning_runtime, "Success")
    return merged_data


UPLOAD = r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\UPLOAD"
connection = connect_to_database()
if connection:
    try:
        print("Merging Files")
        merged_data = merge(UPLOAD,connection)    
        print("Cleaning")
        merged_data = cleaning(merged_data,connection)       
        output_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\merged_dataset.csv"
        merged_data.to_csv(output_path, index=False, sep=',')    
    finally:
        if connection:
            connection.close()
            # print("Database connection closed.")
else:
        print("Database connection failed.")
