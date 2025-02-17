from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class DataAnalysisOperator(BaseOperator):
    @apply_defaults
    def __init__(self,dataset_path, output_path, analysis_type, *args, **kwargs):
        super().init__(*args,**kwargs)
        self.dataset_path=dataset_path
        self.output_path=output_path
        self.analysis_type=analysis_type
    
    def execute(self,context):
        data=pd.read_csv(self.dataset_path)

        if self.analysis_type =="mean":
            result=np.mean(data)
        elif self.analysis_type =='std':
            result=np.std(data)
        else:
            raise ValueError(f"Invalid analysis type '{self.analysis_type}'")
        

        with open(self.output_path,'w') as f:
            f.write(str(result))