import json
import numpy as np
import polars as pl
import geopy.distance

from typing import Tuple
from datetime import datetime
from geopy.point import Point
from numba import jit

try:
    from ._utils import TZ_DICT, D_THRESHOLD, WEEKEND_DICT
except ImportError:
    from _utils import TZ_DICT, D_THRESHOLD, WEEKEND_DICT


@jit(nopython=True)
def fast_normalize(v: np.ndarray) -> np.ndarray:
    assert len(v.shape) == 2, "Single dimention array is not supported"
    # gives 40% better performance than sklearn.preprocessing.normalize
    return np.divide(v, np.sqrt(np.sum(np.power(v, 2), axis=1)).reshape(-1, 1))


def _create_backward_hour(hour: int) -> str:
    if hour - 1 < 0:
        return str((hour - 1) + 24)
    else:
        return str(hour - 1)


def denoise_hour_stats(data: pl.DataFrame, col: str = 'hour_stats') -> pl.DataFrame:
    sub = data.select(['customer_id', col])
    sub = sub.with_columns(pl.col(col).str.json_extract(infer_schema_length=None))\
        .unnest(col)\
        .fill_null(0)

    sub = sub.melt(
        id_vars=['customer_id'],
        value_vars=[x for x in sub.columns if x != 'customer_id'],
        variable_name='hour',
        value_name='num_trips'
    )

    sub = sub.with_columns(pl.col('hour').cast(pl.Int64).map_elements(_create_backward_hour).alias('hour_start')) \
        .with_columns(pl.col('hour').cast(pl.Int64).map_elements(lambda x: ((x + 1) % 24)).cast(str).alias('hour_end'))

    sub_back = sub.select(pl.col('customer_id'), pl.col('hour_start').alias('hour'),
                          pl.col('num_trips').alias('num_trips_start'))
    sub_forward = sub.select(pl.col('customer_id'), pl.col('hour_end').alias('hour'),
                             pl.col('num_trips').alias('num_trips_end'))

    sub = sub.join(sub_back, on=['customer_id', 'hour'], how='left') \
        .join(sub_forward, on=['customer_id', 'hour'], how='left') \
        .fill_null(0) \
        .select(pl.col('customer_id'), pl.col('hour'),
                (pl.col('num_trips') + pl.col('num_trips_start') + pl.col('num_trips_end')).alias('num_trips')) \
        .pivot(values='num_trips', index='customer_id', columns='hour')

    sub = sub.with_columns(
        pl.struct(pl.all()).map_elements(
            lambda row: json.dumps({str(x): row[x] for x in sub.columns if x != 'customer_id'})
        ).alias(
            'hour_denoised_stats')) \
        .select(['customer_id', 'hour_denoised_stats'])

    data = data.join(sub, on='customer_id', how='left')
    return data


def dict_stats_to_norm_cols(data: pl.DataFrame, col: str, prefix: str) -> pl.DataFrame:
    data = data.with_columns(pl.col(col).str.json_extract(infer_schema_length=None))
    df = data.select(col).unnest(col).fill_null(0)
    df = pl.DataFrame(
        data=fast_normalize(df.to_numpy()),
        schema={f'norm_{prefix}:{col}': pl.Float64 for col, _ in df.schema.items()}
    )
    return pl.concat([data.drop(col), df], how='horizontal')


def _encode_cyclical_time(data: pl.DataFrame, col: str, max_val: int) -> pl.DataFrame:
    data = data.with_columns(
        pl.col(col).map_elements(
            lambda x: {
                col+'_sin': np.sin(2 * np.pi * x/max_val),
                col+'_cos': np.cos(2 * np.pi * x/max_val)
            }
        ).alias("result")
    ).unnest("result")
    return data.drop(col)


def _minute_cyclical(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(pl.col('ts').cast(str).str.split(' ').map_elements(
        lambda x: (datetime.strptime(x[1].split('.')[0], '%H:%M:%S') - datetime.strptime('00:00:00', '%H:%M:%S')).seconds // 60
    ).alias('minutes'))
    return _encode_cyclical_time(data, 'minutes', 60*24)


def process_time(data: pl.DataFrame) -> pl.DataFrame:
    assert 'ts' in data.columns, 'Time column should be named "ts"'
    data = data.with_columns(pl.from_epoch("ts", time_unit="s").dt.replace_time_zone("UTC"))
    data = data.with_columns(pl.col('country_name').map_elements(lambda x: TZ_DICT.get(x, None)).alias('tz'))

    pl_data = []

    for tz in TZ_DICT.values():
        pl_data.append(data.filter(pl.col('tz') == tz).with_columns(
            pl.col('ts').dt.convert_time_zone(tz).dt.replace_time_zone(None)))

    data = pl.concat(pl_data, how='vertical').drop('tz')
    data = data.with_columns(pl.col('ts').dt.hour().cast(str).alias('hour'))
    data = data.with_columns(pl.col('ts').dt.weekday().cast(str).alias('weekday'))
    data = _minute_cyclical(data)
    data = data.with_columns(pl.struct(pl.all()).map_elements(
        lambda row: int(row['weekday'] in WEEKEND_DICT.get(row['country_name'], ['6', '7']))
    ).alias('is_weekend'))

    return data


def _is_from_freq(row: dict, locations: dict) -> int:
    if row['dropoff_lat'] == 0 or row['dropoff_long'] == 0:
        return 0
    else:
        dropoff = Point(latitude=row['dropoff_lat'], longitude=row['dropoff_long'])
        freq_loc = [Point(latitude=float(x.split('|')[0]), longitude=float(x.split('|')[1])) for x in locations.keys()]
        distances = [geopy.distance.great_circle(x, dropoff).km for x in freq_loc]
        return int(min(distances) <= D_THRESHOLD)


def _distance_known_location(locations: dict, current_location: Point) -> Tuple[float, float]:
    p2s = [Point(latitude=float(x.split('|')[0]), longitude=float(x.split('|')[1])) for x in locations.keys()]
    v = fast_normalize(np.expand_dims(np.array(list(locations.values())), axis=0))[0]  # list(locations.values())

    min_d = None
    ind = None

    for i, p2 in enumerate(p2s):
        dist = geopy.distance.great_circle(p2, current_location).km
        if min_d is None:
            min_d = dist
            ind = i
        else:
            if dist < min_d:
                min_d = dist
                ind = i

    if min_d <= D_THRESHOLD:
        return min_d, v[ind]
    else:
        return min_d, 0.0


def _most_freq_dist(locations: dict, current_location: Point) -> Tuple[float, float]:
    freq = sorted(
        [((float(k.split('|')[0]), float(k.split('|')[1])), v) for k, v in locations.items()],
        key=lambda x: x[1],
        reverse=True
    )
    most_freq = Point(latitude=freq[0][0][0], longitude=freq[0][0][1])
    second_freq = Point(latitude=freq[1][0][0], longitude=freq[1][0][1])
    return (
        geopy.distance.great_circle(most_freq, current_location).km,
        geopy.distance.great_circle(second_freq, current_location).km
    )


def _get_locations_features(row: dict) -> dict:
    locations = json.loads(row['locations'])
    current = Point(latitude=row['latitude'], longitude=row['longitude'])
    min_d, v = _distance_known_location(locations=locations, current_location=current)
    most_freq_d, second_freq_d = _most_freq_dist(locations=locations, current_location=current)
    return {
        'min_dist_to_known_loc': min_d,
        'norm_trips_curr_location': v,
        'is_freq': _is_from_freq(row, locations),
        'dist_to_most_freq': most_freq_d,
        'dist_to_second_freq': second_freq_d
    }


def _saved_locations_process(row: dict) -> dict:
    current = Point(latitude=row['latitude'], longitude=row['longitude'])
    home_coords = Point(
        latitude=row['home_work_coords']['home']['lat'],
        longitude=row['home_work_coords']['home']['long']
    )
    work_coords = Point(
        latitude=row['home_work_coords']['work']['lat'],
        longitude=row['home_work_coords']['work']['long']
    )

    result = {'is_home': 0, 'is_work': 0, 'has_saved': 0}
    home_dist = np.inf
    work_dist = np.inf

    if home_coords.latitude != 0.0:
        home_dist = geopy.distance.great_circle(home_coords, current).km

    elif work_coords.latitude != 0.0:
        work_dist = geopy.distance.great_circle(work_coords, current).km

    else:
        return result

    result['has_saved'] = 1
    result['is_home'] = int(home_dist <= D_THRESHOLD)
    result['is_work'] = int(work_dist <= D_THRESHOLD)

    return result


def process_locations(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(pl.col('dropoff_lat').fill_null(0.0)) \
        .with_columns(pl.col('dropoff_long').fill_null(0.0))

    for loc_col in ['latitude', 'longitude', 'dropoff_long', 'dropoff_lat']:
        data = data.with_columns(pl.col(loc_col).cast(pl.Float64))

    data = data.with_columns(pl.struct(pl.all()).map_elements(_get_locations_features).alias("result"))\
        .unnest("result")

    data = data.with_columns(pl.col('home_work_coords').str.json_extract(infer_schema_length=None))
    data = data.with_columns(pl.struct(pl.all()).map_elements(_saved_locations_process).alias('result'))\
        .unnest('result')

    return data.drop(['dropoff_lat', 'dropoff_long', 'locations', 'home_work_coords'])


def melt_stats(data: pl.DataFrame) -> pl.DataFrame:
    data = data.with_columns(
        pl.struct(pl.all()).map_elements(
            lambda row: {
                'norm_week': row[f'norm_week:{row["weekday"]}'],
                'norm_hour': row[f'norm_hour:{row["hour"]}'],
                'norm_hour_denoised': row[f'norm_hour_denoised:{row["hour"]}']
            }
        ).alias("result")
    ).unnest("result")
    return data.drop([x for x in data.columns if ':' in x] + ['hour'])
