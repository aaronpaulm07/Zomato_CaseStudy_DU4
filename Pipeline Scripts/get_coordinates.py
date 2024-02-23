import logging
import pandas as pd
import os
from get_coordinate_4url import coordinates_extractor
from concurrent.futures import ProcessPoolExecutor
from SQL_connection import log_subprocess_message , connect_to_database
from datetime import datetime


# Configure logging
log_file = r"Log\LOG-Pipeline.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Specify the path to the CSV file containing the URLs
csv_file_path = r"Pipeline Scripts\Datass\merged_dataset.csv"

# Specify the path for the output CSV file
output_csv_file = r"Pipeline Scripts\Datass\coordinates_scrapped_dataset.csv"

inter_output_csv_file = r"Pipeline Scripts\Datass\coordinates_scrapped_dataset_intermediate.csv"

# Function to process a single URL and return coordinates
def process_url(url):
        try:
            return coordinates_extractor(url) if url else (None, None)
        except Exception as e:
            logging.error(f"Error extracting coordinates for URL '{url}': {e}")
            return None, None

    
    
# Define the interval to save the progress


if __name__ == '__main__': 
    connection = connect_to_database()
    if os.path.isfile(csv_file_path):
        # Read the CSV file into a DataFrame
         df = pd.read_csv(csv_file_path)
         logging.info("get_coordinates - Reading the CSV file and logging the initial dataset size")
         print("Scrapping")
         if os.path.isfile(inter_output_csv_file):
             # Read the intermediate CSV file into a DataFrame
            df_intermediate = pd.read_csv(inter_output_csv_file)
            print(f"records in intermediate {len(df_intermediate)}")
            logging.info(f"records in intermediate {len(df_intermediate)}")
            df_no_coordinates = df_intermediate[df_intermediate['Longitude'].isna()]
            df = df[~df['URL'].isin(df_intermediate['URL'])]
            df_intermediate = df_intermediate.drop(df_no_coordinates.index)
            print(f"records in intermediate without coordinates : {len(df_no_coordinates)}")
            logging.info(f"records in intermediate without coordinates : {len(df_no_coordinates)}")
            df = pd.concat([df, df_intermediate], ignore_index = True)
            # Check the count of records with no coordinates
            logging.info("Count of records with no coordinates in intermediate file: %d", len(df_no_coordinates))
            df_for_coordinates = df_no_coordinates
         else:
            logging.info("Initial dataset - number of records : %d", len(df))

            sorted_df = df.sort_values(by="RATING", ascending=False)
            records_with_zero_rating = sorted_df[sorted_df['RATING'] == 0]
            sorted_df = sorted_df.drop(records_with_zero_rating.index)
            
            size = min(100,int(len(sorted_df)/100))
            
            sample_top_restaurants = sorted_df.head(size)
            sample_low_restaurants = sorted_df.tail(size)
            
            df = df.drop(sample_top_restaurants.index)
            df = df.drop(sample_low_restaurants.index)
            
            df_sample = df.sample(n=(size*8),random_state=42)
            df = df.drop(df_sample.index)
            
            df_for_coordinates = pd.concat([sample_top_restaurants, df_sample, sample_low_restaurants])
        #  df_for_coordinates=df_for_coordinates.head(3)   #comment it .was used for debug  
         count_of_errors=0
         save_interval = 10  # Save progress every 10 URLs processed
         processed_count = 0
         scrap_start_time= datetime.now()
         logging.info(f"Records that need coordinates : {len(df_for_coordinates)}")
        # Create a ThreadPoolExecutor with a maximum of 5 worker processes
         with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Submit tasks to the executor for each URL in the DataFrame
        
          future_to_url = {executor.submit(process_url, url): url for url in df_for_coordinates['URL']}
        
        # Process the results as they become available
          for future in future_to_url:
            url = future_to_url[future]
            logging.info(f"Processing URL: {url}")
            try:
                result = future.result()
            except Exception as exc:
                logging.error(f"An error occurred while processing URL '{url}': {exc}")
                count_of_errors+=1
            else:
                # Get the index of the URL in the DataFrame
                index = df_for_coordinates.index[df_for_coordinates['URL'] == url].tolist()[0]
                
                # Convert Latitude and Longitude to numeric types
                if result is not None:
                    lat, lon = result
                else:
                    lat, lon = None, None

                # Set the result in the corresponding row
                df_for_coordinates.loc[index, 'Latitude'] = lat
                df_for_coordinates.loc[index, 'Longitude'] = lon
                
                processed_count += 1
                # logging.info(f"Processed {processed_count} URLs")
                
                # Save progress if it's time to do so
                if processed_count % save_interval == 0:
                    df_for_coordinates.to_csv(inter_output_csv_file, index=False)
                    logging.info(f"Progress saved. Processed {processed_count} URLs.")

         df_for_coordinates.to_csv(inter_output_csv_file,index=False)
         logging.info("Count of records with coordinates: %d", len(df_for_coordinates[df_for_coordinates['Longitude'].notna()]))
        #  Concatenate the initial DataFrame with the remaining part of the DataFrame
         df_concatenated = pd.concat([df_for_coordinates, df], ignore_index=True)
        # Save the final DataFrame to the CSV file
         df_concatenated.to_csv(output_csv_file, index=False)
        
         scrap_end_time = datetime.now()
         log_subprocess_message( connection ,"SCRAPPING",scrap_start_time,scrap_end_time, scrap_end_time-scrap_start_time,f"Success - {len(df_for_coordinates)} new coordinates added" )
         logging.info("DataFrame with coordinates saved to: %s", output_csv_file)
         logging.info("Count of records : %d", len(df_concatenated))
         print("Scrapping Completed!!!")
         logging.info("Scrapping Completed!!!")
                 
    else:
        logging.error("Scrapping - CSV file not found at: %s", csv_file_path)
        log_subprocess_message( connection ,"Scrapping FAILED", None,None,None,"CSV not found")
        raise FileNotFoundError("CSV not found - get_coordinates")
