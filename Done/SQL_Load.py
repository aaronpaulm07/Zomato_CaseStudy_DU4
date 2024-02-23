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
            raise ValueError("Invalid data format: Data tuple must have 8 or 10 elements.")

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

        # Insert into Restaurants table
        cursor.execute("""
            INSERT INTO Restaurants (Name, Price, City, Region, URL, CuisineType, Rating, Latitude, Longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING ID
        """, (data[0], price, data[3], data[4], data[5], data[6], rating, latitude, longitude))
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
connection = connect_to_database()

# Insert data into Restaurants table
if connection:
    filename = r"D:\DU Training\Case Study-Zomato\dataset-pipeline\coordinates_random.csv"

    # Open the CSV file with UTF-8 encoding and insert each restaurant entry into the database
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        row_count = 0
        for row in reader:
            insert_into_restaurants(connection, row)
            # row_count += 1
            # if row_count >= 10:
            #     break

    connection.close()
    print("Done!")
