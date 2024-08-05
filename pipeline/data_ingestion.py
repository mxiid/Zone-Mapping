import requests
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


class DataIngestionPipeline:
    def __init__(self):
        self.REDASH_HOST = "https://redash.truckitin.ai"
        self.API_KEY = "loMLO5S6tcTusPWt5dcExEA4qMaRRQBbrkbcSuLx"
        self.DATA_SOURCE_ID = "14"
        self.headers = {
            "Authorization": f"Key {self.API_KEY}",
            "Content-Type": "application/json",
        }

        # Configure logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def submit_query(self, query):
        url = f"{self.REDASH_HOST}/api/query_results"
        payload = {"data_source_id": self.DATA_SOURCE_ID, "query": query, "max_age": 0}
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()["job"]["id"]

    def check_job_status(self, job_id):
        url = f"{self.REDASH_HOST}/api/jobs/{job_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()["job"]

    def get_query_results(self, query_result_id):
        url = f"{self.REDASH_HOST}/api/query_results/{query_result_id}.json"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()["query_result"]["data"]["rows"]

    def run_query(self, query):
        try:
            job_id = self.submit_query(query)
            self.logger.info(f"Job ID: {job_id}")

            while True:
                job = self.check_job_status(job_id)
                status = job["status"]
                self.logger.info(f"Job Status: {status}")

                if status == 3:  # completed
                    return self.get_query_results(job["query_result_id"])
                elif status == 4:  # failed
                    self.logger.error(
                        f"Query execution failed: {job.get('error', 'No error message provided')}"
                    )
                    return None

                # Use ThreadPoolExecutor for non-blocking wait
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(self.check_job_status, job_id)
                    job = future.result(timeout=1)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
        except KeyError as e:
            self.logger.error(f"Unexpected response format: {e}")
        return None

    def get_order_details(self):
        query = """
            SELECT id,
                   consignment_id,
                   origin_city_id,
                   origin_city_name,
                   city_id,
                   dest_city_name,
                   warehouse_id,
                   warehouse_title,
                   area_id,
                   area_title,
                   sort_addr_id,
                   sort_addr_title,
                   CONCAT(area_title, ' > ', sort_addr_title) AS L3_L4
            FROM OrderDetails
            LIMIT 10"""

        results = self.run_query(query)
        if results:
            df = pd.DataFrame(results)
            self.logger.info("Query executed successfully.")
            return df
        else:
            self.logger.error("Query execution failed.")
            return None

    def save_data(self, data, filename):
        filepath = Path("artifacts") / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(filepath, index=False)
        self.logger.info(f"Data saved to {filepath}")

    def main(self):
        self.logger.info("Starting data ingestion process")
        df = self.get_order_details()
        if df is not None:
            self.save_data(df, "order_details.csv")
            self.logger.info("Data ingestion completed successfully")
        else:
            self.logger.error("Data ingestion failed")


if __name__ == "__main__":
    try:
        obj = DataIngestionPipeline()
        obj.main()
    except Exception as e:
        logging.error("An error occurred during data ingestion")
        logging.exception(e)
        raise e
