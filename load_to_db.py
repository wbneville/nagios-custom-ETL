import pyodbc
import json
import shutil
from datetime import datetime
import logging


logging.basicConfig(
    filename='/path/to/data_ETL.log',
    filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# Save a backup file in case the load fails and it's necessary to manually run a load later
def backup_file():
    timestamp = datetime.now().strftime('%Y%m%d')
    backup_filename = f"/path/to/{timestamp}_data_extract.txt"
    shutil.copy('/path/to/data_extract.txt', backup_filename)
    logging.info(f"Backup created: {backup_filename}")

with open('/path/to/data_extract.txt', 'r') as f:
    data = json.load(f)

try:
    # connect to the MSSQL DB and use the variables defined above
    conn = pyodbc.connect('DRIVER={whatever_driver_necessary};SERVER=servername.com;DATABASE=db_name;UID=db_user;PWD=user_pw;TrustServerCertificate=yes')
    cursor = conn.cursor()
    logging.info(f"Kicking off data insertion")
    # Use fast execute because of the size of the file
    cursor.fast_executemany = True 

    # Prepare and execute SQL command
    types = ['cpu','mem','disk','swap']
    for data_type in types:
        filtered_data = [entry for entry in data if data_type in entry['service_name'].lower()] 
        # If any "types" do not exist in the data, print it and move on
        if not filtered_data:
            logging.info(f"No data found for type: {data_type}")
            continue
        for row in filtered_data:
            if len(row) != len(filtered_data[0]):
                logging.error(f"Mismatch in number of fields for {row}")

        # Create lists based on the number of keys
        columns = ', '.join(filtered_data[0].keys())
        placeholders = ', '.join(['?'] * len(filtered_data[0]))
        sql = f"INSERT INTO host_{data_type}_usage ({columns}) VALUES ({placeholders})"
        #print(sql)

        # Prepare data
        values = [tuple(row.values()) for row in filtered_data]
        # Insert using execute, log duplicates and continue inserting
        cursor.executemany(sql, values)

    conn.commit()
    cursor.close()
    conn.close()

except pyodbc.Error as e:
    logging.error(f"Database connection failed: {e}")
    backup_file()
