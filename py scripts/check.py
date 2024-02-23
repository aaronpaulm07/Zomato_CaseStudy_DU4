import pandas as pd

filename = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\split_cuisine_dataset.csv"
df = pd.read_csv(filename)


print(len(df))

file = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_scrapped_dataset.csv"
df1= pd.read_csv(file)
print(len(df1))
# Assuming df is your DataFrame
# Replace df with the name of your DataFrame if different
# non_numeric_votes = df[~df['VOTES'].str.isnumeric()]
# print(non_numeric_votes)

# distinct_votes = df['VOTES'].unique()
# Convert 'VOTES' column to numeric, replacing non-numeric values with NaN
# df['VOTES'] = pd.to_numeric(df['VOTES'], errors='coerce')

# Replace NaN values with 0
# df['VOTES'] = df['VOTES'].fillna(0)

# count_ = len(df[df['VOTES'] == 0])
   
# print("Count of records where VOTES is 0:", count_)
# print("Count of records :", len(df))
# print("Count of records REGION IS NONE :", len(df[df['REGION'] == None ]))
# print("Count of records :", len(df))
