import pandas as pd
from typing import Iterable


def only_successful_trips(data: pd.DataFrame) -> pd.DataFrame:
    return data[~((data.is_trip_ended == 0) & (data.rh == 1))]


def only_successful_freq(data: pd.DataFrame) -> pd.DataFrame:
    return data[~((data.is_trip_ended == 0) & (data.is_freq == 1))]


def drop_bad_users(data: pd.DataFrame, loss_df: pd.DataFrame, threshold: float = 1.0) -> pd.DataFrame:
    return data[~data.customer_id.isin(loss_df[loss_df.loss > threshold].customer_id)]


def remove_linear_dependency(data: pd.DataFrame) -> pd.DataFrame:
    return data.drop([x for x in data.columns if x[-2:] == ':1'], axis=1)


def filter_invalid_locations(data: pd.DataFrame) -> pd.DataFrame:
    uae_bounds = [(22.5, 27), (52.2, 56.5)]
    jordan_bounds = [(30, 33), (34, 37)]

    data = (
        data[
            ((data.country_name == 'United Arab Emirates') &
             (data.latitude <= uae_bounds[0][1]) &
             (data.latitude >= uae_bounds[0][0]) &
             (data.longitude <= uae_bounds[1][1]) &
             (data.longitude >= uae_bounds[1][0])) |
            ((data.country_name == 'Jordan') &
             (data.latitude <= jordan_bounds[0][1]) &
             (data.latitude >= jordan_bounds[0][0]) &
             (data.longitude <= jordan_bounds[1][1]) &
             (data.longitude >= jordan_bounds[1][0]))
            ]
    )

    return data


def filter_invalid_service_area_id(
        data: pd.DataFrame,
        valid: Iterable[str] = ('1', '21', '64', '68', '111', '87', '49', '47')
) -> pd.DataFrame:
    return data[data.service_area_id.isin(valid)]
