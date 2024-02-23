import psycopg2

def connect_to_database():
    try:
        # Establish connection parameters
        dbname = "Zomato"
        user = "postgres"
        password = "admin123"
        host = "localhost"  # By default, it's localhost
        port = "5432"  # By default, it's 5432

        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

if __name__ == "__main__":
    # Attempt to connect to the database
    connection = connect_to_database()
    
    # Check if the connection was successful
    if connection:
        print("Connected to the database successfully!")
        # Remember to close the connection when done
        connection.close()
    else:
        print("Failed to connect to the database.")
