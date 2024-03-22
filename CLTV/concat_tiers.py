import pandas as pd

def concat_csv(file_list):
    df = pd.concat([pd.read_csv(f) for f in file_list])
    return df

if __name__ == "__main__":
    
    file_list = [f'/home/jupyter-felipe.guevara/Growth/CLTV/{city}_SAC_tiers.csv' for city in  ['SPO','BHZ','CWB','VCP']]
    df = concat_csv(file_list)
    
    df.to_csv("/home/jupyter-felipe.guevara/Growth/CLTV/ALL_SAC_tiers.csv",index=False)

