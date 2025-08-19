import requests
import json
import pandas as pd
import logging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logging.basicConfig(
    filename='/path/to/data_ETL.log',
    filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

api_key = "apikey"
host_group_url = f"https://server.co/nagiosxi/api/v1/objects/hostgroupmembers?apikey={api_key}&pretty=1"
response = requests.request("GET", host_group_url, verify=False)
hg_data = response.json()
hostgroups = hg_data['hostgroup']
records = len(hostgroups)

## Initialize dataframe to save values for data export
hg_df = pd.DataFrame(columns=['Host Name','Host Group'])

index = 0

for i in range (0, records):	
	# save hostname to variable for storage later
	hostgroup = hostgroups[i]['hostgroup_name'] 

	# due to dictionary within a list formatting we need to do extraction here to get data in format we want
	host = hostgroups[i]['members']
	hosts = host['host']

	# count of how many host are in a certain hostgroup
	hostgroup_length = len(hosts)

	#iterate through each host in the host group and save to dataframe, but omit LINUX_DEFAULT group
	if (hostgroup in ("hostgroup1","hostgroup2") ):
		for j in range (0, hostgroup_length):
			hg_df.loc[index, ['Host Group']] = hostgroup
			hg_df.loc[index, ['Host Name']] = hosts[j]['host_name']
			index = index + 1

# join the host df and hostgroup df using same host_name and append only to host_df

hg_df.to_csv('/path/to/output_file.csv', index=False)
logging.info(f"Hosts extracted to hosts.csv")

