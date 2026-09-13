import os
import requests
import json

host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]

response = requests.get(
    f"{host}/api/2.0/sql/warehouses",
    headers={
        "Authorization": f"Bearer {token}"
    }
)

print("Status:", response.status_code)
print(json.dumps(response.json(), indent=2))
