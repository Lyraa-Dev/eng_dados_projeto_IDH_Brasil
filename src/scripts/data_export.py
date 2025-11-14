import pandas as pd
import os
from sqlalchemy import create_engine

class DataExporter:
    def __init__(self):
        self.output_path = "src/data/output/"
    
    def to_csv(self, df, filename, index=False):
        """Export DataFrame to CSV"""
        file_path = os.path.join(self.output_path, filename)
        df.to_csv(file_path, index=index)
        print(f"Exported to CSV: {filename}")
    
    def to_excel(self, df, filename, sheet_name='Sheet1', index=False):
        """Export DataFrame to Excel"""
        file_path = os.path.join(self.output_path, filename)
        df.to_excel(file_path, sheet_name=sheet_name, index=index)
        print(f"Exported to Excel: {filename}")
    
    def to_database(self, df, table_name, connection_string):
        """Export DataFrame to database"""
        try:
            engine = create_engine(connection_string)
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Exported to database table: {table_name}")
        except Exception as e:
            print(f"Error exporting to database: {e}")