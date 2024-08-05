import re
import json
import pandas as pd
from pathlib import Path
import logging


class RegexProcessingPipeline:
    def __init__(self):
        # Define file paths
        self.zones_file = Path("components/city_hierarchy.json")
        self.input_file = Path("artifacts/data_ingestion/order_details.csv")
        self.output_file = Path(
            "artifacts/regex_processing/processed_order_details.csv"
        )

        # Configure logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Ensure the artifacts directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_zones(self):
        try:
            with open(self.zones_file, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load zones file: {e}")
            raise

    def compile_patterns(self, zones):
        return {
            zone: [
                re.compile(r"\b{}\b".format(variation), re.IGNORECASE)
                for variation in variations
            ]
            for zone, variations in zones.items()
        }

    def extract_zones(self, address, patterns):
        if pd.isna(address):
            return {}
        matched_zones = {}
        for zone, patterns in patterns.items():
            matched_terms = [
                pattern.pattern for pattern in patterns if pattern.search(address)
            ]
            if matched_terms:
                matched_zones[zone] = matched_terms
        return matched_zones

    def process_data(self):
        try:
            # Load data
            data = pd.read_csv(self.input_file)
            data = data.dropna()
            data["delivery_address"] = data["delivery_address"].fillna("").astype(str)

            # Load zones and compile patterns
            zones = self.load_zones()
            patterns = self.compile_patterns(zones)

            # Prepare a DataFrame for zone columns
            zone_columns = {zone: 0 for zone in zones.keys()}

            # Process data
            data["Matched Zones"] = data["delivery_address"].apply(
                lambda addr: self.extract_zones(addr, patterns)
            )
            data["Matched Terms"] = ""
            data["Count of Zones matched"] = 0

            # Create a DataFrame for the zone columns
            zone_df = pd.DataFrame(columns=zone_columns.keys())

            # Add the zone columns to the original data
            data = pd.concat([data, zone_df], axis=1)

            for i, row in data.iterrows():
                matched_zones = row["Matched Zones"]
                matched_terms_list = []
                for zone, terms in matched_zones.items():
                    data.at[i, zone] = 1
                    matched_terms_list.extend(terms)
                data.at[i, "Count of Zones matched"] = len(matched_zones)
                data.at[i, "Matched Terms"] = ", ".join(matched_terms_list)

            data.drop(columns=["Matched Zones"], inplace=True)
            return data

        except Exception as e:
            self.logger.error(f"Failed to process data: {e}")
            raise

    def save_data(self, data):
        try:
            data.to_csv(self.output_file, index=False)
            self.logger.info(f"Data saved to {self.output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            raise

    def main(self):
        self.logger.info("Starting regex processing pipeline")
        try:
            df = self.process_data()
            if df is not None:
                self.save_data(df)
                self.logger.info("Regex processing completed successfully")
            else:
                self.logger.error("Regex processing failed")
        except Exception as e:
            self.logger.error("An error occurred during regex processing")
            self.logger.exception(e)
            raise


if __name__ == "__main__":
    try:
        obj = RegexProcessingPipeline()
        obj.main()
    except Exception as e:
        logging.error("An error occurred")
        logging.exception(e)
        raise e
