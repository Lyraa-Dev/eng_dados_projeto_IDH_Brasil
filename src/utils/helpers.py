import pandas as pd
import logging
from datetime import datetime

def setup_logging():
    """Setup basic logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_pipeline.log'),
            logging.StreamHandler()
        ]
    )

def validate_dataframe(df, required_columns=None):
    """Validate DataFrame structure"""
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
    
    return True

def get_data_summary(df):
    """Generate basic data summary"""
    summary = {
        'rows': len(df),
        'columns': len(df.columns),
        'memory_usage': df.memory_usage(deep=True).sum(),
        'missing_values': df.isnull().sum().to_dict(),
        'data_types': df.dtypes.to_dict()
    }
    return summary