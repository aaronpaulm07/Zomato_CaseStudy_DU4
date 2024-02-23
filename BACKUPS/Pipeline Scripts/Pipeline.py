import subprocess
import logging
from datetime import datetime
from SQL_connection import connect_to_database

# Configure logging
log_file = r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\Log\LOG-Pipeline.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to insert log messages into the run_logs table
def log_to_database(start_time,message):
    try:
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
    print(run_id)
    
    # Execute merge.py
    logging.info("Executing merge.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\merge.py"])
    
    # Execute get_coordinates.py
    logging.info("Executing get_coordinates.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\get_coordinates.py"])

    # Execute SQL_Load.py
    logging.info("Executing SQL_Load.py...")
    # subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\SQL_Load.py"])

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
        log_to_database(start_time,f"ERROR Occured - {e}")
        cursor = connection.cursor()
        cursor.execute("UPDATE run_logs SET status = 'FAILED' WHERE run_id=%s", (run_id,))
        connection.commit()
    else : 
        log_to_database(start_time,f"ERROR - Could not connect")
    logging.error("Pipeline execution failed.")
    print("Pipeline execution failed.")

