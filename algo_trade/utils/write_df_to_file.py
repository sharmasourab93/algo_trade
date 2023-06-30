from algo_trade.utils.constants import ROOT_DIR
import os
import pandas as pd


def write_df_to_file(data: pd.DataFrame, file_name: str, file_type: str = 'csv'):
    file_path = os.path.join(ROOT_DIR, 'data', file_name + '.' + file_type)
    
    if file_type == 'csv':
        data.to_csv(file_path)
    
    else:
        data.to_excel(file_path)
