import unittest
import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from scripts.data_processing import DataProcessor

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        self.processor = DataProcessor()
        self.sample_data = pd.DataFrame({
            'A': [1, 2, 2, 3],
            'B': ['x', 'y', 'y', 'z'],
            'C': [1.0, 2.0, None, 4.0]
        })
    
    def test_clean_data(self):
        cleaned = self.processor.clean_data(self.sample_data)
        self.assertEqual(len(cleaned), 3)  # Remove duplicates
    
    def test_transform_data(self):
        transformations = {'B': 'uppercase'}
        transformed = self.processor.transform_data(self.sample_data, transformations)
        self.assertEqual(transformed['B'].iloc[0], 'X')

if __name__ == '__main__':
    unittest.main()