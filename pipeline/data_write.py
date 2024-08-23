import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataWritingPipeline:
    def __init__(self):
        self.DB_CONFIG = {
            "host": "34.126.120.50",
            "user": "masteruser1",
            "password": "lsU^$ld55UR$110",
            "database": "rider_db_orders",
        }
        self.input_file = "artifacts/warehouse_mapping/mapped_data_details.csv"
        self.batch_size = 100  # Adjust this based on your system's capacity

    def load_data(self):
        try:
            df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(df)} rows from CSV file")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None

    def connect_to_db(self):
        try:
            connection = mysql.connector.connect(**self.DB_CONFIG)
            logger.info("Successfully connected to the database")
            return connection
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to the database: {err}")
            return None

    def update_batch(self, connection, batch):
        cursor = connection.cursor()
        try:
            # Prepare the update query
            update_query = """
            UPDATE STAGING_db_orders.OrderDetails
            SET 
                area_id_old = area_id,
                area_title_old = area_title,
                sort_addr_id_old = sort_addr_id,
                sort_addr_title_old = sort_addr_title,
                warehouse_id_old = warehouse_id,
                warehouse_title_old = warehouse_title,
                area_id = %s,
                area_title = %s,
                sort_addr_id = %s,
                sort_addr_title = %s,
                warehouse_id = %s,
                warehouse_title = %s,
                sorted_flag = 1
            WHERE id = %s
            """

            # Prepare batch data
            batch_data = [
                (
                    row["mapped_l3_id"] if pd.notna(row["mapped_l3_id"]) else None,
                    row["L3_L4"] if pd.notna(row["L3_L4"]) else None,
                    row["mapped_l3_id"] if pd.notna(row["mapped_l3_id"]) else None,
                    row["L3_L4"] if pd.notna(row["L3_L4"]) else None,
                    (
                        row["mapped_warehouse_id"]
                        if pd.notna(row["mapped_warehouse_id"])
                        else None
                    ),
                    (
                        row["Mapped_Warehouse_Title"].strip()
                        if pd.notna(row["Mapped_Warehouse_Title"])
                        else None
                    ),
                    row["id"],
                )
                for _, row in batch.iterrows()
            ]

            # Execute batch update
            cursor.executemany(update_query, batch_data)
            connection.commit()

            return len(batch)
        except Error as e:
            logger.error(f"Error updating batch: {e}")
            connection.rollback()
            return 0
        finally:
            cursor.close()

    def update_database(self, df):
        connection = self.connect_to_db()
        if not connection:
            return

        try:
            total_updated = 0
            with ThreadPoolExecutor(
                max_workers=5
            ) as executor:  # Adjust max_workers as needed
                futures = []
                for i in range(0, len(df), self.batch_size):
                    batch = df.iloc[i : i + self.batch_size]
                    futures.append(
                        executor.submit(self.update_batch, connection, batch)
                    )

                for future in as_completed(futures):
                    total_updated += future.result()

            logger.info(f"Updated {total_updated} rows in the database.")
        finally:
            if connection.is_connected():
                connection.close()

    def main(self):
        logger.info("Starting warehouse mapping data write process...")
        df = self.load_data()

        if df is not None:
            self.update_database(df)
            logger.info("Warehouse mapping data write completed.")
        else:
            logger.error(
                "Warehouse mapping data write failed due to data loading error."
            )


if __name__ == "__main__":
    try:
        obj = DataWritingPipeline()
        obj.main()
    except Exception as e:
        logging.error("An error occurred")
        logging.exception(e)
        raise e
