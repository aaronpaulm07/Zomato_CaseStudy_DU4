import pandas as pd
import os
from get_coordinate_4url import coordinates_extractor
from concurrent.futures import ProcessPoolExecutor

# Specify the path to the CSV file containing the URLs
csv_file_path = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\cuisine_split.csv"

# Specify the path for the output CSV file
output_csv_file = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_kitti.csv"

inter_output_csv_file = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_intermediate.csv"

# Check if the file exists
if os.path.isfile(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    df_first = df[:200]

    # Function to process a single URL and return coordinates
    def process_url(url):
        return coordinates_extractor(url) if url else (None, None)

    # Define the interval to save the progress
    save_interval = 10  # Save progress every 10 URLs processed
    processed_count = 0

    if __name__ == '__main__':
        # Create a ThreadPoolExecutor with a maximum of 5 worker processes
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Submit tasks to the executor for each URL in the DataFrame
            future_to_url = {executor.submit(process_url, url): url for url in df_first['URL']}
            
            # Process the results as they become available
            for future in future_to_url:
                url = future_to_url[future]
                print(url)
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"An error occurred while processing URL '{url}'")
                else:
                    # Get the index of the URL in the DataFrame
                    index = df_first.index[df_first['URL'] == url].tolist()[0]
                    
                    # Convert Latitude and Longitude to numeric types
                    if result is not None:
                        lat, lon = result
                    else:
                        lat, lon = None, None

                    # Set the result in the corresponding row
                    df_first.loc[index, 'Latitude'] = lat
                    df_first.loc[index, 'Longitude'] = lon
                    
                    processed_count += 1
                    print(processed_count)
                    
                    # Save progress if it's time to do so
                    if processed_count % save_interval == 0:
                        df_first.to_csv(inter_output_csv_file, index=False)
                        print(f"Progress saved. Processed {processed_count} URLs..................................")

        # Concatenate the initial DataFrame with the remaining part of the DataFrame
        df_concatenated = pd.concat([df_first, df[200:].reset_index(drop=True)], ignore_index=True)
 

        # Save the final DataFrame to the CSV file
        df_concatenated.to_csv(output_csv_file, index=False)

        print("DataFrame saved to:", output_csv_file)
else:
    print("CSV file not found at:", csv_file_path)
