import os
import json
import time
import requests
from datetime import datetime, timezone

# =====================================================
# Configuration
# =====================================================

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

PROCESSED_TABLE = "workspace.earthquake_live_feed.processed_earthquake_ids"

VOLUME_PATH = "/Volumes/workspace/earthquake_live_feed/incoming"

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

print("Host configured:", bool(DATABRICKS_HOST))
print("Warehouse configured:", bool(WAREHOUSE_ID))
print("Warehouse ID length:", len(WAREHOUSE_ID))


# =====================================================
# Execute SQL Statement
# =====================================================

def execute_sql(sql_text):

    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql_text
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json=payload,
        timeout=60
    )

    print("================================")
    print("SQL API STATUS:", response.status_code)
    print("SQL API RESPONSE:")
    print(response.text)
    print("================================")

    response.raise_for_status()

    result = response.json()

    statement_id = result["statement_id"]

    while True:

        status_response = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS,
            timeout=60
        )

        status_response.raise_for_status()

        status_json = status_response.json()

        state = status_json["status"]["state"]

        print("Statement Status:", state)

        if state == "SUCCEEDED":
            return status_json

        if state in ["FAILED", "CANCELED", "CLOSED"]:
            raise Exception(
                json.dumps(status_json, indent=2)
            )

        time.sleep(2)


# =====================================================
# Download Earthquake Feed
# =====================================================

response = requests.get(USGS_URL, timeout=30)
response.raise_for_status()

data = response.json()

features = data.get("features", [])

print("USGS feed records:", len(features))

if not features:
    print("No earthquake data found")
    raise SystemExit(0)


# =====================================================
# Read Processed IDs
# =====================================================

query = f"""
SELECT earthquake_id
FROM {PROCESSED_TABLE}
"""

result = execute_sql(query)

processed_ids = set()

try:
    data_array = result["result"]["data_array"]

    for row in data_array:
        processed_ids.add(row[0])

except Exception:
    print("No existing processed IDs found")

print("Processed IDs:", len(processed_ids))


# =====================================================
# Filter New Records
# =====================================================

new_features = [
    feature
    for feature in features
    if feature["id"] not in processed_ids
]

print("New earthquakes:", len(new_features))


# =====================================================
# Upload File
# =====================================================

if new_features:

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d%H%M%S")

    filename = f"earthquake_{timestamp}.json"

    file_path = f"{VOLUME_PATH}/{filename}"

    json_content = "\n".join(
        json.dumps(feature)
        for feature in new_features
    )

    upload_url = (
        f"{DATABRICKS_HOST}"
        f"/api/2.0/fs/files{file_path}"
    )

    upload_headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream"
    }

    upload_response = requests.put(
        upload_url,
        headers=upload_headers,
        data=json_content.encode("utf-8"),
        timeout=120
    )

    print("Upload Status:", upload_response.status_code)
    print("Upload Response:", upload_response.text)

    upload_response.raise_for_status()

    print("File uploaded:", file_path)

else:

    print("No new earthquake events")
    print("No file created")


# =====================================================
# Record Processed IDs
# =====================================================

if new_features:

    ingestion_time = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")

    values = []

    for feature in new_features:

        earthquake_id = feature["id"].replace("'", "''")

        values.append(
            f"('{earthquake_id}', TIMESTAMP '{ingestion_time}')"
        )

    insert_sql = f"""
    INSERT INTO {PROCESSED_TABLE}
    (earthquake_id, ingestion_time)
    VALUES
    {','.join(values)}
    """

    execute_sql(insert_sql)

    print("process_id recorded:", len(new_features))
