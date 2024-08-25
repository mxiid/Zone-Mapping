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
                pattern_parts = [re.escape(area)]  # Start with the area name
                for locality in localities:
                    if locality.startswith('\\'):
                        # This is already a regex pattern, don't escape it
                        pattern_parts.append(locality)
                    else:
                        # This is a normal string, escape it
                        pattern_parts.append(re.escape(locality))
                
                pattern = re.compile(
                    r"(?i)"  # Case-insensitive
                    + r"(?:\b|(?<=\W))(?:"  # Word boundary or preceded by non-word char
                    + "|".join(pattern_parts)  # Join all patterns
                    + r")(?:\b|(?=\W))"  # Word boundary or followed by non-word char
                )
                patterns[f"{city} - {area}"] = pattern
        return patterns

    @lru_cache(maxsize=10000)
    def extract_zones(self, address, city):
        if pd.isna(address) or pd.isna(city):
            return {}

        address = self.preprocess_address(address)
        matched_areas = {}
        
        for area, pattern in self.patterns.items():
            if area.startswith(f"{city} - ") and pattern.search(address):
                matched_areas[area] = pattern.pattern

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
        
        # Add this debug logging
        self.logger.info("Sample of Matched Zones: %s", chunk["Matched Zones"].head().to_dict())

        chunk["Count of Zones matched"] = chunk["Matched Zones"].apply(len)
        chunk["Matched Terms"] = chunk["Matched Zones"].apply(
            lambda x: ", ".join(x.values())
        )

        chunk["L3_L4"] = chunk.apply(
            lambda row: (
                next(iter(row["Matched Zones"].keys())).split(" - ", 1)[1]
                if row["Count of Zones matched"] == 1
                else ""
            ),
            axis=1,
        )

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