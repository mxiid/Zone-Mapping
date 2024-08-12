import pandas as pd
from pathlib import Path


class WarehouseMappingPipeline:
    def __init__(self):
        self.input_file = Path("artifacts/api_processing/api_data_details.csv")
        self.mapping_file = Path("components/L3 Mapping.csv")
        self.output_file = Path("artifacts/warehouse_mapping/mapped_data_details.csv")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_data(self):
        try:
            data_df = pd.read_csv(self.input_file)
            mapping_df = pd.read_csv(self.mapping_file)
            return data_df, mapping_df
        except Exception as e:
            print(f"Error loading data: {e}")
            return None, None

    def normalize_city_name(self, city_name):
        city_name = city_name.strip().lower()
        if city_name in ["khi", "karachi"]:
            return "karachi"
        return city_name

    def map_warehouse(self, row, mapping_df):
        # Normalize city names for comparison
        normalized_city = self.normalize_city_name(row["dest_city_name"])
        row["dest_city_name"] = normalized_city

        # First attempt to match using L3_Area
        l3_match = mapping_df[
            (
                mapping_df["dest_city_name"].apply(self.normalize_city_name)
                == normalized_city
            )
            & (mapping_df["L3_Area"].str.lower() == row["L3_L4"].strip().lower())
        ]

        if not l3_match.empty:
            return pd.Series(
                {
                    "Mapped_L4_Id": l3_match["L4_Id"].values[0],
                    "Mapped_Warehouse_Title": l3_match[
                        "Correct Warehouse Title"
                    ].values[0],
                }
            )

        # Fallback to matching with L4_Zone
        l4_match = mapping_df[
            (
                mapping_df["dest_city_name"].apply(self.normalize_city_name)
                == normalized_city
            )
            & (mapping_df["L4_Zone"].str.lower() == row["L3_L4"].strip().lower())
        ]

        if not l4_match.empty:
            return pd.Series(
                {
                    "Mapped_L4_Id": l4_match["L4_Id"].values[0],
                    "Mapped_Warehouse_Title": l4_match[
                        "Correct Warehouse Title"
                    ].values[0],
                }
            )

        return pd.Series({"Mapped_L4_Id": None, "Mapped_Warehouse_Title": None})

    def process_data(self, data_df, mapping_df):
        # Apply the mapping function row by row
        mapped_columns = data_df.apply(
            self.map_warehouse, axis=1, mapping_df=mapping_df
        )

        # Concatenate the original data with the new mapped columns
        result_df = pd.concat([data_df, mapped_columns], axis=1)

        return result_df

    def save_data(self, df):
        df.to_csv(self.output_file, index=False)
        print(f"Mapped data saved to {self.output_file}")

    def main(self):
        print("Starting warehouse mapping process...")
        data_df, mapping_df = self.load_data()

        if data_df is not None and mapping_df is not None:
            mapped_df = self.process_data(data_df, mapping_df)
            self.save_data(mapped_df)
            print(
                "Warehouse mapping completed. Updated file saved with mapping information."
            )
        else:
            print("Warehouse mapping failed due to data loading error.")


if __name__ == "__main__":
    try:
        obj = WarehouseMappingPipeline()
        obj.main()
    except Exception as e:
        import logging

        logging.error("An error occurred")
        logging.exception(e)
        raise e
