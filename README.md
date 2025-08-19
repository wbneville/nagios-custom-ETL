# Nagios XI ETL (Extract → Transform → Load)

hosts_to_csv.py, extract.py, load_to_db.py are run one after another on cron to grab the past day's OS metric performance data and insert it into Microsoft SQL Server:
- hosts_to_csv.py - exports hosts names for selected hostgroups to csv, for being read in within extract
- extract.py - fetches 25h of nagios perfdata via rrdexport api and writes it to a file for later insertion into sql server
- load_to_db.py - bulk-inserts the extracted payload into sql server tables by metric type


url_service_status_InfluxDB_insert.py is used to grab the status data of all services in a servicegroup and insert them into InfluxDB. This is used to enable better visualization tools to display the current status of URLs

# Prerequisites

host OS metric data ETL:
Python 3.9+ recommended
- Requests, urllib3, pandas, pyodbc
Connectivity to Nagios server API and sql server

URL nagios status to influx ETL:
Python3.9+ recommended
- Requests, urllib3, datetime, influxdbclient, logging, csv
Connectivity to nagios server API and influxDB 


