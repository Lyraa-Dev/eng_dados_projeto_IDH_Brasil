import pandas as pd
import numpy as np

class DataProcessor:
    def __init__(self):
        self.processed_data_path = "src/data/processed/"

    def clean_data(self, df):
        """Clean the DataFrame by handling missing values and duplicates."""
        "remove duplicates and fill missing values"
        df_clean = df.drop_duplicates()
        df_clean = df_clean.fillna({
            'numeric_columns': 0,
            'text_columns': 'Unknown'
        })
        return df_clean
    
    def transform_data(self, df, transformations):
        """Apply a series of transformations to the DataFrame."""

        df_transformed = df.copy()

        for col, operation in transformations.items():
            if operation == 'lowercase':
                df_transformed[col] = df_transformed[col].str.lower()
            elif operation == 'uppercase':
                df_transformed[col] = df_transformed[col].str.upper()
            elif operation == 'strip':
                df_transformed[col] = df_transformed[col].str.strip()
                
        return df_transformed
    
    def aggregate_data(self, df, group_by_cols, agg_dict):
        """Aggregate the DataFrame based on specified columns and aggregation functions."""
        df_aggregated = df.groupby(group_by_cols).agg(agg_dict).reset_index()
        return df_aggregated