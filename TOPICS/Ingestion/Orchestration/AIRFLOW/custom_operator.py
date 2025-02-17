from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class DataAnalysisOperator(BaseOperator):
    @apply_defaults
    def __init__(self, dataset_path, output_path, analysis_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_path = dataset_path
        self.output_path = output_path
        self.analysis_type = analysis_type
        
    def execute(self, context):
        # Load the input dataset into a pandas DataFrame.
        data = pd.read_csv(self.dataset_path)
        
        # Perform the requested analysis.
        if self.analysis_type == 'mean':
            result = np.mean(data)
        elif self.analysis_type == 'std':
            result = np.std(data)
        else:
            raise ValueError(f"Invalid analysis type '{self.analysis_type}'")
        
        # Write the result to a file.
        with open(self.output_path, 'w') as f:
            f.write(str(result))



from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import requests

class EndpointSensorOperator(BaseSensorOperator):

    @apply_defaults
    def __init__(self, url, expected_text, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.expected_text = expected_text

    def poke(self, context):
        response = requests.get(self.url)
        if response.status_code != 200:
            return False
        return self.expected_text in response.text