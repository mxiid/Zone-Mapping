import logging
from pipeline.data_ingestion import run_query

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    print(">>>>>DATA INGESTION STARTED<<<<<")
    # Run the query and get the DataFrame
    df = run_query()

    if df is not None:
        logging.info("Query executed successfully.")
except Exception as e:
    logging.error(">>>>>DATA INGESTION FAILED<<<<<")
    logging.error(f"An error occurred: {e}")
    raise e
