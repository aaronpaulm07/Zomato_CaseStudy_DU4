import psycopg2
import csv

# Function to establish connection to the PostgreSQL database
def connect_to_database():
    try:
        # Establish connection parameters
        dbname = "Zomato"
        user = "postgres"
        password = "admin123"
        host = "localhost"  # Update with your host if different
        port = "5432"  # Update with your port if different

        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        print("Connection to PostgreSQL established successfully!")
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def insert_into_restaurants(connection, data):
    try:
        # Validate data format
        if len(data) != 8 and len(data) != 10:
            raise ValueError("Invalid data format: Data tuple must have 7 or 9 elements.")

        # Check if latitude and longitude are provided
        if len(data) == 10:
            latitude = data[8] if data[8] != '' else None
            longitude = data[9] if data[9] != '' else None
        else:
            latitude = None
            longitude = None

        # Convert Price to integer
        price = int(data[1])

        # Convert Rating to float
        rating = float(data[7])

        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO Restaurants (Name, Price, City, Region, URL, CuisineType, Rating, Latitude, Longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data[0], price, data[3], data[4], data[5], data[6], rating, latitude, longitude))
        connection.commit()
        print("Data inserted into Restaurants table successfully!")
        print(latitude,longitude)
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into Restaurants table:", error)
        print("Problematic row:", data)

# Connect to the database
connection = connect_to_database()

# Insert data into Restaurants table
if connection:
    filename = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_kitti.csv"

    # Open the CSV file with UTF-8 encoding and insert each restaurant entry into the database
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        row_count = 0
        for row in reader:
            # Print the length of the current row for debugging purposes
            # print("Length of row:", len(row))
            
            # If the row has 7 elements, append empty strings for latitude and longitude
            if len(row) == 8:
                row.extend(['', ''])
            # If the row has 10 elements, ignore the last element
            elif len(row) == 11:
                row = row[:10]
            # If the row has 9 elements, continue
            elif len(row) == 10:
                pass
            # If the row has neither 7, 9, nor 10 elements, print it as problematic
            else:
                print("Invalid row format:", row)
                continue
            
            # Print the current row for debugging purposes
            # print("Current row:", row)
            
            # Insert the row into the database
            insert_into_restaurants(connection, row)
            
            row_count += 1
            if row_count >= 10:
                break
            

    connection.close()
