import sqlite3
import requests
import os
db_connection = sqlite3.connect('database.db')
cursor = db_connection.cursor()

# Replace 'YOUR_API_KEY' with your actual ChartMogul API key
api_key = "YOUR_API_KEY"

# Retrieve datasource information from your database
cursor.execute("SELECT datasource_name, uuid FROM datasources")
datasource_info = cursor.fetchall()

# API endpoint URL
base_url = "https://api.chartmogul.com/v1/data_sources/"

# Iterate through datasource information and delete each datasource
for datasource_name, uuid in datasource_info:
    url = f"{base_url}{uuid}"

    headers = {
        "Content-Type": "application/json",
    }

    response = requests.delete(url,auth=(api_key, ''), headers=headers)

    if response.status_code == 204:
        print(f"Data source '{datasource_name}' with UUID {uuid} deleted successfully.")
    else:
        print(f"Error deleting data source '{datasource_name}' with UUID {uuid}. Status code: {response.status_code}, Response: {response.text}")

# Close the database connection
db_connection.close()

# Delete the database file
database_file_path = 'database.db'
if os.path.exists(database_file_path):
    os.remove(database_file_path)
    print(f"Database file '{database_file_path}' deleted successfully.")
else:
    print(f"Database file '{database_file_path}' not found.")
