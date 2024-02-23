import os
import pandas as pd


# Specify the base directory containing folders with CSV files
base_directory = r"D:\DU Training\Case Study-Zomato\Zomato_Dataset"


# Initialize an empty DataFrame to store the merged data
merged_data = pd.DataFrame()

# Iterate through each folder in the base directory
for folder in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder)

    # Check if the item in the base directory is a folder
    if os.path.isdir(folder_path):
        print(f"Processing folder: {folder}")

        # List all CSV files in the folder
        csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        # Iterate through each CSV file in the folder
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            # print(f"Reading file: {file_path}")

            # Read the CSV file into a DataFrame
            data = pd.read_csv(file_path, sep='|')
            

            # Merge the data into the main DataFrame
            merged_data = pd.concat([merged_data, data], ignore_index=True)

#CLEANING 
merged_data = merged_data.drop(['PAGE NO', 'TIMING','VOTES','RATING_TYPE'], axis=1)
merged_data['RATING'] = pd.to_numeric(merged_data['RATING'], errors='coerce').fillna(0.0)
merged_data['CUSINE TYPE'] = merged_data['CUSINE TYPE'].replace('none', 'Unknown')

# Write the merged data to a new CSV file
output_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\merged_data.csv"
merged_data.to_csv(output_path, index=False, sep=',')
# COPY 
output_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\merged_data_COPY.csv"
merged_data.to_csv(output_path, index=False, sep=',')

print("Merging completed. Merged data saved to 'merged_data.csv' in the specified directory.")
