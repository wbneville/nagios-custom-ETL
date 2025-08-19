import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime
from influxdb import InfluxDBClient
import logging
import csv

logging.basicConfig(
    filename='/path/to/nagios_to_influx.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_KEY = "apikey"
BASE_URL = "https://server.com/nagiosxi/api/v1"
SERVICEGROUP_NAME = "servicegroup1"

INFLUXDB_HOST = "influxhost.com"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "db_name"
CLIENT_CERT_FILE = "/path/to/influxpem.pem"
CLIENT_KEY_FILE = "/path/to/influxkey.key"
MEASUREMENT = "measurement_name"

STATUS_MAP = {
    "0": "OK",
    "1": "WARNING",
    "2": "CRITICAL",
    "3": "UNKNOWN"
}
STATUS_NUMERIC_MAP = {
    "OK": 0,
    "WARNING": 1,
    "CRITICAL": 2,
    "UNKNOWN": 3
}

def fetch_data():
    session = requests.Session()
    session.verify = False

    params = {
        "apikey": API_KEY,
        "pretty": 1,
    }

    # STEP 1: Get list of services in the group
    group_url = f"{BASE_URL}/objects/servicegroupmembers"
    group_resp = session.get(group_url, params={**params, "servicegroup_name": SERVICEGROUP_NAME}, timeout=30)
    group_resp.raise_for_status()
    group_services = group_resp.json().get("servicegroup", [{}])[0].get("members", {}).get("service", [])

    service_keys = {(s["host_name"], s["service_description"]) for s in group_services}
    if not service_keys:
        logging.warning(f"No services found in service group '{SERVICEGROUP_NAME}'")
        return []

    # STEP 2: Get all statuses
    status_url = f"{BASE_URL}/objects/servicestatus"
    status_resp = session.get(status_url, params=params, timeout=30)
    status_resp.raise_for_status()
    all_status = status_resp.json().get("servicestatus", [])
    status_filtered = [s for s in all_status if (s["host_name"], s["service_description"]) in service_keys]

    # STEP 3: Get all details
    details_url = f"{BASE_URL}/objects/service"
    details_resp = session.get(details_url, params={**params, "customvars": 1}, timeout=30)
    details_resp.raise_for_status()
    all_details = details_resp.json().get("service", [])
    details_map = {
        (d["host_name"], d["service_description"]): d for d in all_details if (d["host_name"], d["service_description"]) in service_keys
    }

    # STEP 4: Merge and write to CSV
    combined = []
    CSV_LOG_PATH = "./point_data_latest_run.csv"
    csv_file = open(CSV_LOG_PATH, mode="w", newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["host_name", "service_description", "friendlyname", "crownjewel"])

    for item in status_filtered:
        key = (item["host_name"], item["service_description"])
        details = details_map.get(key, {})
        friendlyname = "unknown"
        crownjewel = "no"
        cv = details.get("customvars", {})

        if isinstance(cv, dict):
            friendlyname = cv.get("FRIENDLYNAME", "unknown")
            crownjewel = cv.get("CROWNJEWEL", "no")
        elif isinstance(cv, list):
            customvars_dict = {var.get("name"): var.get("value") for var in cv if "name" in var and "value" in var}
            friendlyname = customvars_dict.get("FRIENDLYNAME", "unknown")
            crownjewel = customvars_dict.get("CROWNJEWEL", "no")

        last_check = item.get("last_check")
        if not last_check:
            logging.warning(f"Skipping service (missing last check): host='{item['host_name']}', service='{item['service_description']}'")
            continue
        try:
            timestamp = int(datetime.strptime(last_check, "%Y-%m-%d %H:%M:%S").timestamp())
        except:
            logging.warning(f"Invalid timestamp for {key}: {last_check}")
            continue

        status_text = STATUS_MAP.get(item.get("current_state", "3"), "UNKNOWN")
        status_num = STATUS_NUMERIC_MAP.get(status_text, -1)

        point = {
            "measurement": MEASUREMENT,
            "tags": {
                "service_description": item["service_description"],
                "display_name": details.get("display_name", "unknown"),
                "friendlyname": friendlyname,
                "crownjewel": crownjewel
            },
            "fields": {
                "service_status": status_text,
                "service_status_numeric": status_num
            },
            "time": timestamp
        }

        combined.append(point)

        # Write to CSV
        csv_writer.writerow([
            item["host_name"],
            item["service_description"],
            friendlyname,
            crownjewel
        ])

    csv_file.close()
    logging.info(f"Total services in group: {len(service_keys)}")
    logging.info(f"Services with valid status: {len(status_filtered)}")

    return combined

def write_to_influxdb(points):
    client = InfluxDBClient(
        host=INFLUXDB_HOST,
        port=INFLUXDB_PORT,
        ssl=True,
        verify_ssl=False,
        database=INFLUXDB_DATABASE
    )
    client._session.cert = (CLIENT_CERT_FILE, CLIENT_KEY_FILE)

    if not any(db['name'] == INFLUXDB_DATABASE for db in client.get_list_database()):
        raise Exception(f"Database '{INFLUXDB_DATABASE}' does not exist.")

    if points:
        client.write_points(points, time_precision='s')
        logging.info(f"Wrote {len(points)} points to InfluxDB.")
    else:
        logging.info("No valid points to write.")
    logging.info(f"Services written to Influx: {len(points)}")

def main():
    try:
        data = fetch_data()
        write_to_influxdb(data)
        logging.info("Run complete.")
    except Exception as e:
        logging.error(f"Script failed: {e}")

if __name__ == "__main__":
    main()