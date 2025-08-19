# Nagios XI ETL (Extract → Transform → Load)

Small, scriptable ETL to:
1) build a host inventory CSV for selected host groups,
2) pull recent RRD/perfdata metrics from Nagios XI, and
3) load the extracted metrics into Microsoft SQL Server.

Scripts:
- hosts_to_csv.py - exports hosts names for selected hostgroups to csv, for being read in within extract
- extract.py - fetches 25h of nagios perfdata via rrdexport api and writes it to a file for later insertion into sql server
- load_to_db.py - bulk-inserts the extracted payload into sql server tables by metric type

# Prerequisites

Python 3.9+ recommended
- Requests, urllib3, pandas, pyodbc
Connectivity to Nagios server API and sql server

install Python deps:
python -m pip install requests pandas pyodbc urllib3
