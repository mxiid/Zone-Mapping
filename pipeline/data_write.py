import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

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

    def load_data(self):
        try:
            df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(df)} rows from CSV file")
            logger.info(
                f"Sample of Mapped_Warehouse_Title: {df['Mapped_Warehouse_Title'].head()}"
            )
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

    def execute_query(self, cursor, query, params=None):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return True
        except mysql.connector.Error as err:
            logger.error(f"Error executing query: {err}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            return False

    def update_database(self, df):
        connection = self.connect_to_db()
        if connection:
            cursor = connection.cursor()
            try:
                updated_rows = 0
                for _, row in df.iterrows():
                    # Log row data for debugging
                    logger.info(f"Row data: {row.to_dict()}")

                    # Prepare values with fallback for None
                    mapped_l4_id = (
                        row["Mapped_L4_Id"] if pd.notna(row["Mapped_L4_Id"]) else None
                    )
                    l3_l4 = row["L3_L4"] if pd.notna(row["L3_L4"]) else None
                    mapped_warehouse_title = (
                        row["Mapped_Warehouse_Title"].strip() 
                        if pd.notna(row["Mapped_Warehouse_Title"]) else None
                    )

                    # Log the values to be updated for verification
                    logger.info(
                        f"Updating id {row['id']} with area_id={mapped_l4_id}, "
                        f"area_title={l3_l4}, sort_addr_id={mapped_l4_id}, "
                        f"sort_addr_title={l3_l4}, warehouse_title={mapped_warehouse_title}"
                    )

                    # Update old fields
                    update_old_query = """
                    UPDATE STAGING_db_orders.OrderDetails
                    SET area_id_old = area_id,
                        area_title_old = area_title,
                        sort_addr_id_old = sort_addr_id,
                        sort_addr_title_old = sort_addr_title,
                        warehouse_id_old = warehouse_id,
                        warehouse_title_old = warehouse_title
                    WHERE id = %s
                    """
                    if not self.execute_query(cursor, update_old_query, (row["id"],)):
                        logger.error(f"Failed to update old fields for id {row['id']}")

                    # Update new fields
                    update_new_query = """
                    UPDATE STAGING_db_orders.OrderDetails
                    SET area_id = %s,
                        area_title = %s,
                        sort_addr_id = %s,
                        sort_addr_title = %s,
                        warehouse_title = %s,
                        sorted_flag = 1
                    WHERE id = %s
                    """
                    if not self.execute_query(
                        cursor,
                        update_new_query,
                        (
                            mapped_l4_id,
                            l3_l4,
                            mapped_l4_id,
                            l3_l4,
                            mapped_warehouse_title,
                            row["id"],
                        ),
                    ):
                        logger.error(f"Failed to update new fields for id {row['id']}")
                    else:
                        updated_rows += 1
                        logger.info(f"Successfully updated new fields for id {row['id']}")

                    # Verify update
                    verify_query = """
                    SELECT area_id, area_title, sort_addr_id, sort_addr_title, warehouse_title, sorted_flag
                    FROM STAGING_db_orders.OrderDetails
                    WHERE id = %s
                    """
                    if self.execute_query(cursor, verify_query, (row["id"],)):
                        result = cursor.fetchone()
                        logger.info(f"Verification for id {row['id']}: {result}")

                connection.commit()
                logger.info(f"Updated {updated_rows} rows in the database.")

            except Error as e:
                logger.error(f"Error updating database: {e}")
                connection.rollback()

            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()

        return None


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
