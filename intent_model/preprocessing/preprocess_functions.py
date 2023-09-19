import json
import math
import numpy as np
import polars as pl

from typing import Tuple
from datetime import datetime
from sklearn.preprocessing import normalize

try:
    from ._utils import TZ_DICT
except ImportError:
    from _utils import TZ_DICT


def dict_stats_to_norm_cols(data: pl.DataFrame, col: str, prefix: str) -> pl.DataFrame:
    data = data.with_columns(pl.col(col).str.json_extract())
    df = pl.from_dicts(data[col].to_list()).fill_null(0)
    df = pl.DataFrame(
        data=normalize(df.to_numpy()),
        schema={f'norm_{prefix}:{col}': pl.Float64 for col, _ in df.schema.items()}
    )
    return pl.concat([data, df], how='horizontal').drop(col)


def _encode_cyclical_time(data: pl.DataFrame, col: str, max_val: int) -> pl.DataFrame:
    data = data.with_columns(
        pl.col(col).map_elements(
            lambda x: {
                col+'_sin': np.sin(2 * np.pi * x/max_val),
                col+'_cos': np.cos(2 * np.pi * x/max_val)
            }
        ).alias("result")
    ).unnest("result")
    return data.drop("col")


def _minute_cyclical(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(pl.col('ts').cast(str).str.split(' ').map_elements(
        lambda x: (datetime.strptime(x[1].split('.')[0], '%H:%M:%S') - datetime.strptime('00:00:00', '%H:%M:%S')).seconds // 60
    ).alias('minutes'))
    return _encode_cyclical_time(data, 'minutes', 60*24)


def process_time(data: pl.DataFrame) -> pl.DataFrame:
    assert 'ts' in data.columns, 'Time column should be named "ts"'
    data = data.with_columns(pl.from_epoch("ts", time_unit="s").dt.replace_time_zone("UTC"))
    data = data.with_columns(pl.col('country_name').map_elements(lambda x: TZ_DICT.get(x, None)).alias('tz'))
    data = data.drop_nulls(subset=['tz'])

    pl_data = []

    for tz in data['tz'].unique().to_numpy().flatten():
        pl_data.append(data.filter(pl.col('tz') == tz).with_columns(
            pl.col('ts').dt.convert_time_zone(tz).dt.replace_time_zone(None)))

    data = pl.concat(pl_data, how='vertical').drop('tz')
    data = data.with_columns(pl.col('ts').dt.hour().alias('hour'))
    data = data.with_columns(pl.col('ts').dt.weekday().alias('weekday'))
    data = _minute_cyclical(data)

    return data


def _is_from_freq(row: dict, locations: dict) -> int:
    if row['dropoff_lat'] == 0 or row['dropoff_long'] == 0:
        return 0
    else:
        freq_loc = [(float(x.split('|')[0]), float(x.split('|')[1])) for x in locations.keys()]
        return int((row['dropoff_lat'], row['dropoff_long']) in freq_loc)


def _distance_known_location(row: dict, locations: dict) -> Tuple[float, int]:
    p2s = [(float(x.split('|')[0]), float(x.split('|')[1])) for x in locations.keys()]
    v = list(locations.values())

    min_d = None
    ind = None

    for i, p2 in enumerate(p2s):
        dist = math.hypot(p2[0] - row['latitude'], p2[1] - row['longitude'])
        if min_d is None:
            min_d = dist
            ind = i
        else:
            if dist < min_d:
                min_d = dist
                ind = i

    return min_d, v[ind]


def _most_freq_dist(row: dict, locations: dict) -> float:
    freq = sorted(
        [((float(k.split('|')[0]), float(k.split('|')[1])), v) for k, v in locations.items()],
        key=lambda x: x[1],
        reverse=True
    )

    return math.hypot(freq[0][0][0] - row['latitude'], freq[0][0][1] - row['longitude'])


def _get_locations_features(row: dict) -> dict:
    locations = json.loads(row['locations'])
    min_d, v = _distance_known_location(row, locations)
    return {
        'min_dist_to_known_loc': min_d,
        'known_loc_occ': v,
        'is_freq': _is_from_freq(row, locations),
        'dist_to_most_freq': _most_freq_dist(row, locations)
    }


def process_locations(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(pl.col('dropoff_lat').fill_null(0.0)) \
        .with_columns(pl.col('dropoff_long').fill_null(0.0))

    for loc_col in ['latitude', 'longitude', 'dropoff_lat', 'dropoff_long']:
        data = data.with_columns(pl.col(loc_col).cast(pl.Float64).round(3))

    data = data.with_columns(pl.struct(pl.all()).map_elements(_get_locations_features).alias("result")).unnest("result")
    return data.drop(['dropoff_lat', 'dropoff_long', 'locations'])


def melt_stats(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(
        pl.struct(pl.all()).map_elements(
            lambda row: {
                'norm_week': row[f'norm_week:{row["weekday"]}'],
                'norm_hour': row[f'norm_hour:{row["hour"]}']
            }
        ).alias("result")
    ).unnest("result")
    return data.drop([x for x in data.columns if ':' in x] + ['hour'])
