import pandas as pd
import numpy as np
from datetime import datetime

from analystcommunity.write_connection_data_warehouse import runQuery, to_sql

def put_products_demand_estimations(CLTV_df: pd.DataFrame, region_code: str, remove_previous_records: bool = False):
    
    if remove_previous_records:
        del_test_sql = f"""

        DELETE FROM
            lnd_ops.customer_cltv
        WHERE
            region_code = '{region_code}'
        """

        runQuery('ops', del_test_sql)
    
    to_sql(vertical='ops', 
            df_params=CLTV_df,
            table='customer_cltv')
    

if __name__ == "__main__":
    df = pd.read_csv("/home/jupyter-felipe.guevara/Growth/CLTV/ALL_SAC_tiers.csv")

    # Get the current date in the desired format
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Add the current date as a new column to the dataframe
    df['created_at'] = current_date

    df2 = df.loc[:,['created_at','city','customer_id','tier']]
    df2.columns = ['created_at', 'region_code', 'customer_id', 'cltv']
    
    for city in ['CWB','SPO','BHZ','VCP']:
        put_products_demand_estimations(df2.loc[df2.region_code == city], city, False)