import pandas as pd


def multiclass_target(data: pd.DataFrame) -> pd.DataFrame:
    """multiclass"""
    data['is_freq'] = data['is_freq'].fillna(0)
    data['target'] = (data['is_freq'] + data['rh']).astype(int)
    return data


def rh_vs_rest_target(data: pd.DataFrame) -> pd.DataFrame:
    """binary"""
    data['target'] = data['rh'].astype(int)
    return data


def pattern_vs_rest_target(data:pd.DataFrame) -> pd.DataFrame:
    """binary"""
    data['target'] = data['is_freq'].astype(int)
    return data
