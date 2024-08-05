import requests
import json
import pandas as pd
import logging



def run_query():
    REDASH_HOST = "https://redash.truckitin.ai"
    API_KEY = "loMLO5S6tcTusPWt5dcExEA4qMaRRQBbrkbcSuLx"
    DATA_SOURCE_ID = "14"

    # Ad-hoc SQL query
    query = "SELECT * FROM OrderDetails LIMIT 10"

    # URL for running the query
    url = f"{REDASH_HOST}/api/query_results"

    # Headers with the API key
    headers = {"Authorization": f"Key {API_KEY}", "Content-Type": "application/json"}

    # Payload with the data source ID and query
    payload = {"data_source_id": DATA_SOURCE_ID, "query": query}
    
    try:
        # Make the request
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # Check for a successful response
        response.raise_for_status()

        # Parse the JSON response
        result = response.json()

        # Extract the query results
        query_results = result["query_result"]["data"]["rows"]

        # Convert the query results to a Pandas DataFrame
        df = pd.DataFrame(query_results)

        print(df.head())

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None
    except KeyError as e:
        logging.error(f"Unexpected response format: {e}")
        return None
