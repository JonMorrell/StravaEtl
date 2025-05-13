import pandas as pd
import numpy as np

from utilities.database_utils import get_columns_dictionary

def clean_data(df:pd.DataFrame) -> pd.DataFrame:
    
    correct_cols = get_columns_dictionary()
    for c in df.columns:
        correct_type = correct_cols[c]

        if correct_type == 'int64' or correct_type == 'float64':
            df[c].replace([np.inf, -np.inf], np.nan, inplace=True)
            df[c].fillna(0,inplace=True)
            df[c].replace('', 0, inplace=True)
            df[c].apply(pd.to_numeric, errors='coerce')

        elif correct_type == 'object':
            df[c].fillna('',inplace=True)            
    
    df['average_speed'] = df['average_speed'] * 3.6
    df['max_speed'] = df['max_speed'] * 3.6

    df = df.astype(get_columns_dictionary())
    df = df.rename(columns={'id': 'activity_id'})

    return df