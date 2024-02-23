import pandas as pd

# Load the dataset containing URLs
csv_file_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\cuisine_split.csv"
df = pd.read_csv(csv_file_path)

df['Longitude'] = None
df['Latitude'] = None

output_csv_file = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\records_with_coordinates_None.csv"
df.to_csv(output_csv_file, index=False)