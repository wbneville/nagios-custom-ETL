import csv
import requests
import json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime, timedelta
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import shutil


# Saving data from the last instance of this script for dropping duplicate data later
def last_run_payload():
    last_run_filename = f"/path/to/data_extract_last.txt"
    shutil.copy('/data/nagios_ETL/data_extract.txt', last_run_filename)
    logging.info(f"Last run payload saved: {last_run_filename}")

# Logging config
logging.basicConfig(filename='/path/to/data_ETL.log',
                    filemode='a',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

# Save current time
ct = datetime.now()
# Get time 25 hours ago to increase the time range of the API call (no missed data)
past_time = ct - timedelta(hours=25)
ts = round(past_time.timestamp())

api_key = "[api_key]"

# Mapping of service names to their value keys (visible in rrd export api call)
# Value keys could be different depending on unit specified in passive check
service_keys = {
    "Memory Usage": ["memory_available_GiB", "memory_total_GiB", "memory_used_percent", "memory_free_GiB", "memory_used_GiB"],
    "Swap Usage": ["swap_used_GiB", "swap_total_GiB", "swap_free_GiB"],
    "Disk Usage root": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage tmp": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage apps": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage boot": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage opt": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage var": ["Used_Gib", "Free_GiB", "Total_GiB"],
    "Disk Usage home": ["Used_Gib", "Free_Gib", "Total_GiB"],
    "CPU Usage": ["percent_used"]
}

services = list(service_keys.keys())

# Convert to int unless it's NaN
def convert_to_int(value):
    try:
        float_value = float(value)
        if math.isnan(float_value):
            return None
        return float(f"{float_value:.2f}")
    #if conversion fails return None 
    except ValueError:
        return None 

# Function to convert timestamps from rrd within api call into datetime2 for insertion into sql server 
def epoch_to_mssql_datetime2(epoch_timestamp):
    dt = datetime.fromtimestamp(epoch_timestamp)
    mssql_datetime2 = dt.strftime('%Y-%m-%d %H:%M:%S')
    return mssql_datetime2

# Function to fetch and process data for a single host and service
def fetch_service_data(session, host_name, service):
    api_url = f"https://server.co/nagiosxi/api/v1/objects/rrdexport?apikey={api_key}&pretty=1&host_name={host_name}&service_description={service}&start={ts}"
    response = session.get(api_url, verify=False)
    results = []

    if response.status_code == 200:
        data = response.json()
        if 'data' in data and 'row' in data['data']:
            expected_keys = ['host_name','timestamp','service_name'] + service_keys.get(service, [])
            for point in data['data']['row']:
                entry = {
                    'host_name': host_name,
                    'timestamp': epoch_to_mssql_datetime2(int(point['t'])),
                    'service_name': service,
                }
                # some perfdata endpoints return a list, convert the list into individual integers 
                if service in service_keys:
                    if isinstance(point['v'], list):
                        for key, value in zip(service_keys[service], point['v']):
                            entry[key] = convert_to_int(value)
                    else:
                        key = service_keys[service][0]
                        value = point['v']
                        entry[key] = convert_to_int(value)
                # skip recent time entries that have not have their rrd data written (ramdisk spool hasn't emptied at time of script run)
                if set(entry.keys()) == set(expected_keys) and not any(value is None or (isinstance(value, float) and math.isnan(value)) for value in entry.values()):
                    results.append(entry)
                else:
                    missing_keys = set(expected_keys) - set(entry.keys())
                    log.error(f"Skipping entry for {host_name} - {service}: Missing keys {missing_keys}")
        else:
            log.error(f"Unexpected JSON structure for {host_name} - {service}: {data}")
    else:
        log.error(f"Error fetching data for {host_name} - {service}: {response.status_code}")

    return results

def process_host(row, session):
    output_data = []
    host_name = row['Host Name']
    for service in services:
        output_data.extend(fetch_service_data(session, host_name, service))
    return output_data

# filter the data since we looked back more than 24 hours (to capture data from previous run that hadn't had ramdisk spool emptied)
def compare_and_filter_data():
    try:
        with open('/path/to/data_extract_last.txt', 'r') as last_file:
            last_data = json.load(last_file)
            last_data_set = {json.dumps(entry, sort_keys=True) for entry in last_data}
    except FileNotFoundError:
        # If no previous file exists, skip comparison
        last_data_set = set()

    with open('/path/to/data_extract.txt', 'r') as current_file:
        current_data = json.load(current_file)
    
    filtered_data = [entry for entry in current_data if json.dumps(entry, sort_keys=True) not in last_data_set]

    with open('/path/to/data_extract.txt', 'w') as output_file:
        json.dump(filtered_data, output_file, indent=4)
    
    logging.info(f"Filtered {len(current_data) - len(filtered_data)} duplicate entries.")

### Main function ###
def main():
    last_run_payload()
    # Open the csv and read it
    with open('/path/to/list_of_hosts.csv', mode='r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        rows = [row for row in csvreader if row['Host Group'] in ("[hostgroup_name]","[can_be_multiple]")]

    logging.info(f"Kicking off data retrieval at {ts}")

    # Open the output file
    with open('/path/to/data_extract.txt', mode='w') as output_file:
        output_data = []

        # Extract rrd files with 5 concurrent API calls 
        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(process_host, row, session): row for row in rows}
                for future in as_completed(futures):
                    output_data.extend(future.result())

        # Write data to output
        output_file.write(json.dumps(output_data, indent=4))
        logging.info(f"Data written to data_extract.txt")

    compare_and_filter_data()    

    logging.info(f"It's done. woohoo!")

if __name__ == "__main__":
    main()
