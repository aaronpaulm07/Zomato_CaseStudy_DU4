import subprocess
import logging
import os
from datetime import datetime
from SQL_connection import connect_to_database
import psycopg2

# Configure logging
log_file = r"Pipeline Scripts\Log\LOG-Pipeline.txt"
try:
    # Create the directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    # Configure logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
except Exception as e:
    print(f"Error occurred while configuring logging: {e}")

def update_last_running_pipeline_status_to_stopped():
    try:
        # Establish connection to the database
        connection = connect_to_database()
        if connection:
            cursor = connection.cursor()
            # Check if there is a running pipeline
            cursor.execute("SELECT run_id,status FROM run_logs ORDER BY run_id DESC LIMIT 1")
            row = cursor.fetchone()
            # print(row)
            if row:
                last_run_id, last_status = row
                if(last_status == "Running"):
                # Update the status of the last running pipeline to "stopped"
                    cursor.execute("UPDATE run_logs SET status = 'STOPPED' WHERE run_id = %s", (last_run_id,))
                    connection.commit()
                # print("Status of the last running pipeline updated to 'STOPPED'.")

    except psycopg2.Error as e:
        logging.error("Error updating last running pipeline status:", e)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Function to insert log messages into the run_logs table
def log_to_database(start_time,message):
    try:
        update_last_running_pipeline_status_to_stopped()
        connection = connect_to_database()
        if connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO run_logs (start_time, status) VALUES (%s, %s)  RETURNING run_id", (start_time, message))
            connection.commit()
            run_id = cursor.fetchone()[0]
            return run_id,connection
    except Exception as e:
        print(f"Error while inserting log message into database: {e}")
        
start_time = datetime.now()
run_id = None
try:
    run_id, connection= log_to_database(start_time,"Running")
    # print(run_id)
    
    print("Execute merge.py")
    logging.info("Executing merge.py...")
    subprocess.run(["python", r"merge.py"],check=True)
    
    print("Execute get_coordinates.py")
    logging.info("Executing get_coordinates.py...")
    # subprocess.run(["python", r"get_coordinates.py"],check=True)

    print("Execute SQL_Load.py")
    logging.info("Executing SQL_Load.py...")
    # subprocess.run(["python", r"SQL_Load.py"],check=True)

    end_time = datetime.now()
    logging.info("Pipeline execution completed successfully.")
    
    total_runtime = end_time - start_time
    
    total_seconds = total_runtime.total_seconds()
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    cursor = connection.cursor()
    status = f"Completed in  {total_runtime}"
    status = status.strip()
    cursor.execute("UPDATE run_logs SET end_time = %s, total_runtime = %s , status = 'Completed' WHERE run_id=%s ", (end_time, total_runtime,run_id))
    connection.commit()
    
    print(f"Total Runtime: {hours} hours, {minutes} minutes, {seconds} seconds")
    logging.info(f"Total Runtime: {hours} hours, {minutes} minutes, {seconds} seconds")
    
except Exception as e:
    if run_id:
        # log_to_database(start_time,f"ERROR Occured - {e}")
        # print(str(e))
        cursor = connection.cursor()
        cursor.execute("UPDATE run_logs SET status = 'FAILED', error_message= %s WHERE run_id=%s", (str(e),run_id,))
        connection.commit()
    else : 
        log_to_database(start_time,f"ERROR - Could not connect")
    logging.error("Pipeline execution failed.")
    print("Pipeline execution failed.")

