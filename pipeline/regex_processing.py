import re
import json
import pandas as pd
from pathlib import Path


class RegexProcessor:
    def __init__(self, zones_file):
        self.zones = self.load_zones(zones_file)
        self.patterns = self.compile_patterns()

    def load_zones(self, zones_file):
        with open(zones_file, "r") as f:
            return json.load(f)

    def compile_patterns(self):
        return {
            zone: [
                re.compile(r"\b{}\b".format(variation), re.IGNORECASE)
                for variation in variations
            ]
            for zone, variations in self.zones.items()
        }

    def extract_zones(self, address):
        if pd.isna(address):
            return {}
        matched_zones = {}
        for zone, patterns in self.patterns.items():
            matched_terms = [
                pattern.pattern for pattern in patterns if pattern.search(address)
            ]
            if matched_terms:
                matched_zones[zone] = matched_terms
        return matched_zones

    def process_data(self, input_file, output_file):
        data = pd.read_csv(input_file)
        data = data.dropna()
        data["delivery_address"] = data["delivery_address"].fillna("").astype(str)

        for zone in self.zones.keys():
            data[zone] = 0

        data["Matched Zones"] = data["delivery_address"].apply(self.extract_zones)
        data["Matched Terms"] = ""
        data["Count of Zones matched"] = 0

        for i, row in data.iterrows():
            matched_zones = row["Matched Zones"]
            matched_terms_list = []
            for zone, terms in matched_zones.items():
                data.at[i, zone] = 1
                matched_terms_list.extend(terms)
            data.at[i, "Count of Zones matched"] = len(matched_zones)
            data.at[i, "Matched Terms"] = ", ".join(matched_terms_list)

        data.drop(columns=["Matched Zones"], inplace=True)
        data.to_csv(output_file, index=False)
        print(f"Updated file saved successfully: {output_file}")
