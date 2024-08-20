import re
import json
import pandas as pd
from pathlib import Path
import logging
from functools import lru_cache


class RegexProcessingPipeline:
    def __init__(self):
        self.zones_file = Path("components/city_hierarchy.json")
        self.input_file = Path("artifacts/data_ingestion/order_details.csv")
        self.output_file = Path("artifacts/regex_processing/processed_data_details.csv")

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        self.city_hierarchy = self.load_zones()
        self.patterns = self.compile_patterns(self.city_hierarchy)

    def load_zones(self):
        try:
            with open(self.zones_file, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load zones file: {e}")
            raise

    def compile_patterns(self, city_hierarchy):
        patterns = {}
        for city, areas in city_hierarchy.items():
            for area, localities in areas.items():
                area_name = area.split(" - ")[-1]
                pattern = re.compile(
                    r"(?i)"
                    + r"(?:\b|(?<=\W)){}(?:\b|(?=\W))|".format(re.escape(area_name))
                    + "|".join(
                        r"(?:\b|(?<=\W)){}(?:\b|(?=\W))".format(re.escape(locality))
                        for locality in localities
                    )
                )
                patterns[f"{city} - {area_name}"] = pattern
        return patterns

    @lru_cache(maxsize=10000)
    def extract_zones(self, address, city):
        if pd.isna(address) or pd.isna(city):
            return {}

        address = self.preprocess_address(address)
        matched_areas = {
            area.split(" - ")[1]: pattern.pattern
            for area, pattern in self.patterns.items()
            if area.startswith(f"{city} - ") and pattern.search(address)
        }

        # if not matched_areas:
        #     self.logger.warning(
        #         f"No matches found for address: {address} in city: {city}"
        #     )

        return matched_areas

    def preprocess_address(self, address):
        address = " ".join(address.lower().split())
        address = re.sub(r"[^a-z0-9\s,]", "", address)
        return address

    def process_chunk(self, chunk):
        chunk['original_delivery_address'] = chunk['delivery_address']

        chunk["delivery_address"] = (
            chunk["delivery_address"].fillna("").apply(self.preprocess_address)
        )

        chunk["Matched Zones"] = chunk.apply(
            lambda row: self.extract_zones(
                row["delivery_address"], row["dest_city_name"]
            ),
            axis=1,
        )
        
        chunk["Count of Zones matched"] = chunk["Matched Zones"].apply(len)
        chunk["Matched Terms"] = chunk["Matched Zones"].apply(
            lambda x: ", ".join(x.values())
        )

        # **Add this debug statement**
        self.logger.info("Matched Zones: %s", chunk["Matched Zones"].tolist())

        chunk["L3_L4"] = chunk.apply(
            lambda row: (
                next(iter(row["Matched Zones"].keys()))
                if row["Count of Zones matched"] == 1
                else ""
            ),
            axis=1,
        )

        # **Add this debug statement**
        self.logger.info("Assigned L3_L4: %s", chunk["L3_L4"].tolist())

        chunk['delivery_address'] = chunk['original_delivery_address']
        chunk = chunk.drop(columns=["Matched Zones", "Count of Zones matched", "Matched Terms", "original_delivery_address"])

        return chunk


    def process_data(self):
        try:
            df = pd.read_csv(self.input_file)
            processed_df = self.process_chunk(df)
            return processed_df
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
