import psycopg2
from datetime import datetime

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

        # print("Connection to PostgreSQL established successfully!")
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def log_subprocess_message(connection, subprocess_name, start_time, end_time, runtime, message ):
    try:
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT MAX(run_id) FROM run_logs")
            run_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO subprocess_logs (run_id,subprocess_name, start_time, end_time, total_runtime, message) VALUES (%s, %s, %s, %s, %s, %s)",
                           (run_id,subprocess_name, start_time, end_time, runtime, message))
            connection.commit()
            # print("Log message inserted into subprocess_logs table.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while inserting log message into database: {error}")
