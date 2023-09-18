import math
import json
import numpy as np
import pandas as pd
import pytz

from datetime import datetime as dt
from typing import Optional, Tuple
from pandarallel import pandarallel
from sklearn.preprocessing import normalize


pandarallel.initialize(progress_bar=False)


def timezones_conversion(row: pd.Series) -> Optional[dt]:
    if row['country_name'] == 'United Arab Emirates':
        return row['ts'].tz_convert(tz='Asia/Dubai')
    elif row['country_name'] == 'Jordan':
        return row['ts'].tz_convert(tz='Asia/Amman')
    else:
        return np.nan


def convert_timestamp(data: pd.DataFrame, time_column: str = 'ts') -> pd.DataFrame:
    data[time_column] = pd.to_datetime(data[time_column], unit='s', utc=True)
    data[time_column] = data.parallel_apply(timezones_conversion, axis=1)
    data = data.dropna(subset=['ts'])
    data[time_column] = pd.to_datetime(data.ts.astype(str).parallel_apply(lambda x: x.split('+')[0]))
    return data


def distance_known_location(row: pd.Series) -> Tuple[float, int]:
    p1 = row['latitude'], row['longitude']
    p2s = [(float(x.split('|')[0]), float(x.split('|')[1])) for x in row['locations'].keys()]
    v = list(row['locations'].values())

    min_d = None
    ind = None

    for i, p2 in enumerate(p2s):
        dist = math.hypot(p2[0] - p1[0], p2[1] - p1[1])
        if min_d is None:
            min_d = dist
            ind = i
        else:
            if dist < min_d:
                min_d = dist
                ind = i

    return min_d, v[ind]


def is_from_freq(row: pd.Series) -> Optional[bool]:
    if np.isnan(row['dropoff_lat']) or np.isnan(row['dropoff_long']):
        return np.nan
    else:
        coord = (round(row['dropoff_lat'], 3), round(row['dropoff_long'], 3))
        freq_loc = [(float(x.split('|')[0]), float(x.split('|')[1])) for x in row['locations'].keys()]
        return coord in freq_loc


def most_freq_dist(row: pd.Series) -> (float, float):
    lat = round(row['latitude'], 3)
    long = round(row['longitude'], 3)

    freq = sorted(
        [((float(k.split('|')[0]), float(k.split('|')[1])), v) for k, v in row['locations'].items()],
        key=lambda x: x[1],
        reverse=True
    )

    return math.hypot(freq[0][0][0] - lat, freq[0][0][1] - long)


def preprocess_locations(data: pd.DataFrame) -> pd.DataFrame:
    data['locations'] = data['locations'].parallel_apply(json.loads)
    data = data.dropna(subset='locations')
    data['min_dist_to_known_loc'], data['known_loc_occ'] = zip(*data.parallel_apply(distance_known_location, axis=1))
    data['is_freq'] = data.parallel_apply(is_from_freq, axis=1)
    data['dist_to_most_freq'] = data.parallel_apply(most_freq_dist, axis=1)
    return data.drop(['locations', 'dropoff_lat', 'dropoff_long'], axis=1)


def dict_stats_to_cols(data: pd.DataFrame, col: str, prefix: str, include_norm: bool) -> pd.DataFrame:
    data[col] = data[col].parallel_apply(json.loads)
    data = data.dropna(subset=col)

    data[col] = data[col].parallel_apply(lambda x: {f'{prefix}:{k}': v for k, v in x.items()})
    df = data[col].parallel_apply(pd.Series).fillna(0)

    if include_norm:
        df = pd.DataFrame(data=normalize(df.values), columns=[f'norm_{x}' for x in df.columns])
    else:
        df = pd.DataFrame(data=df.values, columns=df.columns)

    return pd.concat([data.drop(col, axis=1).reset_index(drop=True), df.reset_index(drop=True)], axis=1)


def _case_when(row: pd.Series) -> Tuple[float, float]:
    hour = row['ts'].hour
    week = row['ts'].weekday() + 1
    return row[f'norm_week:{week}'], row[f'norm_hour:{hour}']


def melt_stats(data: pd.DataFrame) -> pd.DataFrame:
    data['norm_week'], data['norm_hour'] = zip(
        *data.parallel_apply(_case_when, axis=1)
    )
    data = data.drop([x for x in data.columns if ':' in x], axis=1)
    return data


def encode_cyclical_time(data: pd.DataFrame, col: str, max_val: int) -> pd.DataFrame:
    data[col + '_sin'] = np.sin(2 * np.pi * data[col]/max_val)
    data[col + '_cos'] = np.cos(2 * np.pi * data[col]/max_val)
    return data.drop(col, axis=1)


def minute_cyclical(data: pd.DataFrame) -> pd.DataFrame:
    assert 'ts' in data.columns, 'Time column should be named "ts"'
    data['minutes'] = data['ts'].astype(str).parallel_apply(
        lambda x: (dt.strptime(x.split(' ')[1], '%H:%M:%S') - dt.strptime('00:00:00', '%H:%M:%S')).seconds
    ) // 60
    return encode_cyclical_time(data, 'minutes', 60*24)


def fill_service_area_id(data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy()
    users = frame[['customer_id', 'service_area_id']].assign(count=1) \
        .groupby(['customer_id', 'service_area_id'], as_index=False) \
        .sum() \
        .sort_values('count', ascending=False) \
        .drop_duplicates(subset='customer_id', keep='first')

    df = frame[frame.service_area_id.isna()].copy()
    frame = frame.dropna(subset='service_area_id')
    df = df.drop('service_area_id', axis=1).merge(users.drop('count', axis=1), on='customer_id', how='left')
    frame = pd.concat([frame, df]).sort_index()
    frame = frame.dropna(subset='service_area_id')
    return frame


def get_last_ride(data: pd.DataFrame) -> pd.DataFrame:
    """not feasible right now"""
    data = data.reset_index(drop=True)
    frame = data[data.is_trip_ended == 1][['ts', 'customer_id']].copy()
    frame['prev_ride_ts'] = frame.sort_values(['customer_id', 'ts']).groupby(by='customer_id')['ts'].shift()
    frame = frame.dropna(subset='prev_ride_ts')

    data = data.merge(frame, on=['customer_id', 'ts'], how='left').sort_values(['customer_id', 'ts'])
    data['prev_ride_ts'] = data.groupby(by='customer_id')['prev_ride_ts'].bfill()

    na_data = data[data.prev_ride_ts.isna()].drop('prev_ride_ts', axis=1).copy()
    data = data[~data.prev_ride_ts.isna()]
    last_ts = data[['customer_id', 'prev_ride_ts']].groupby(by='customer_id', as_index=False).max()
    na_data = na_data.merge(last_ts, on='customer_id', how='left')
    data = pd.concat([data, na_data], ignore_index=True)

    data = data[data.ts > data.prev_ride_ts]
    data = data.dropna(subset=['prev_ride_ts'])
    data['last_ride'] = (data['ts'] - data['prev_ride_ts']).dt.seconds / 3600

    return data.drop('prev_ride_ts', axis=1)
