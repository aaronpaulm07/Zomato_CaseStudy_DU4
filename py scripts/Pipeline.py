import subprocess
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Execute merge.py
    logging.info("Executing merge.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\merge.py"])

    # Execute split_cuisine.py
    logging.info("Executing split_cuisine.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\split_cuisine.py"])

    # Execute get_coordinates.py
    logging.info("Executing get_coordinates.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\get_coordinates.py"])

    # Execute SQL_Load.py
    logging.info("Executing SQL_Load.py...")
    subprocess.run(["python", r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\SQL_Load.py"])

    logging.info("Pipeline execution completed successfully.")

except Exception as e:
    logging.error(f"An error occurred: {e}")
