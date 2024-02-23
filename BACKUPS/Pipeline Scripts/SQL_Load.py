import psycopg2
import csv
from SQL_connection import connect_to_database,log_subprocess_message
from datetime import datetime


filename = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_scrapped_dataset.csv"
connection = connect_to_database()

def insert_into_restaurants(connection, data):
    try:
        # Validate data format
        if len(data) != 9 and len(data) != 11:
            raise ValueError("Invalid data format: Data tuple must have 9 or 11 elements.")

        # Check if latitude and longitude are provided
        if len(data) == 11:
            latitude = data[9] if data[9] != '' else None
            longitude = data[10] if data[10] != '' else None
        else:
            latitude = None
            longitude = None

        if latitude is not None:
          print(latitude, longitude)
        # Convert Price to integer
        price = int(data[1])
        votes = int(float(data[8]))
        # Convert Rating to float
        rating = float(data[7])

        cursor = connection.cursor()

        # Insert into Restaurants table
        cursor.execute("""
            INSERT INTO Restaurants (Name, Price, City, Region, URL, CuisineType, Rating, Latitude, Longitude, Votes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING ID
        """, (data[0], price, data[3], data[4], data[5], data[6], rating, latitude, longitude, votes))
        restaurant_id = cursor.fetchone()[0]
        
        # Insert into CuisineCategories table
        cuisine_types = data[2].strip("[]").replace("'", "").split(", ")
        category_ids = []
        # Add this code before fetching category IDs
# Insert cuisine types into CuisineCategories table if they do not exist
        for cuisine_type in cuisine_types:
            cursor.execute("""
                INSERT INTO CuisineCategories (CategoryName)
                VALUES (%s)
                ON CONFLICT (CategoryName) DO NOTHING
                RETURNING ID
            """, (cuisine_type.strip(),))
            category_row = cursor.fetchone()
            if category_row:
                category_ids.append(category_row[0])
            else:
                # If cuisine type was not inserted (already exists), fetch its ID
                cursor.execute("""
                    SELECT ID FROM CuisineCategories WHERE CategoryName = %s
                """, (cuisine_type.strip(),))
                category_row = cursor.fetchone()
                if category_row:
                    category_ids.append(category_row[0])
            # print(category_ids)
        # Insert into RestaurantCuisineCategories table
        for category_id in category_ids:
            cursor.execute("""
                INSERT INTO RestaurantCuisineCategories (RestaurantID, CategoryID)
                VALUES (%s, %s)
            """, (restaurant_id, category_id))

            connection.commit()
                # print("Data inserted successfully!")
    except (Exception, psycopg2.Error) as error:
                print("Error while inserting data:", error)
                print("Problematic row:", data)

# Connect to the database

def clear_tables(connection):
    try:
        cursor = connection.cursor()

        # Truncate Restaurants table
        cursor.execute("TRUNCATE TABLE Restaurants RESTART IDENTITY CASCADE")

        # Truncate CuisineCategories table
        cursor.execute("TRUNCATE TABLE CuisineCategories RESTART IDENTITY CASCADE")

        # Truncate RestaurantCuisineCategories table
        cursor.execute("TRUNCATE TABLE RestaurantCuisineCategories RESTART IDENTITY CASCADE")

        connection.commit()
        print("Tables cleared successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while clearing tables:", error)
        log_subprocess_message(connection,"DB LOADING FAILED",datetime.now(),None,None,"Tables were not cleared")


# Insert data into Restaurants table
if connection:
    clear_tables(connection)
    start_time = datetime.now()
    count=0
    error_rows=0
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        # row_count = 0
        for row in reader:
           try:
               insert_into_restaurants(connection, row)
               count+=1
           except Exception as e:
               print(f"Error inserting row: {e}")
               error_rows+=1    
    end_time = datetime.now()
    log_subprocess_message(connection,"DB LOADING",start_time,end_time,end_time-start_time,f"Success - {count} records loaded, {error_rows} skipped")
    connection.close()
    print(f"Loading Done!{count} records")
