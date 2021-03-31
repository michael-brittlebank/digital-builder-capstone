import pandas as pd


def get_dataframe_from_list(data):
    headers = data.pop(0)
    return pd.DataFrame(data, columns=headers)
