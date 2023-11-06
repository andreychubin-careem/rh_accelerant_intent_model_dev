import os
import polars as pl
import pandas as pd

from tqdm import tqdm

try:
    from .filters import filter_invalid_locations, filter_invalid_service_area_id
    from .preprocess_functions import (
        process_time,
        process_locations,
        melt_stats,
        dict_stats_to_norm_cols,
        denoise_hour_stats
    )
except ImportError:
    from filters import filter_invalid_locations, filter_invalid_service_area_id
    from preprocess_functions import (
        process_time,
        process_locations,
        melt_stats,
        dict_stats_to_norm_cols,
        denoise_hour_stats
    )


def _deduplicate_data(frame: pl.DataFrame) -> pl.DataFrame:
    rh_frame = frame.filter(pl.col('booking_id').ne(0))
    sa_frame = frame.filter(pl.col('booking_id').eq(0))

    sa_frame = sa_frame.filter(~pl.col('sessionuuid').is_in(rh_frame['sessionuuid'].to_list())) \
        .sort(by=['sessionuuid', 'ts']) \
        .unique(subset=['sessionuuid'], keep='first')

    rh_frame = rh_frame.sort(by=['sessionuuid', 'is_trip_ended', 'ts'], descending=[False, True, False]) \
        .unique(subset=['sessionuuid'], keep='first')

    return pl.concat([rh_frame, sa_frame], how='vertical').sort(by=['ts'])


def process_day(frame: pl.DataFrame, melt_dicts: bool) -> pl.DataFrame:
    frame = process_time(frame)
    frame = frame.with_columns(pl.col('booking_id').ne(0).cast(pl.Int64).alias('rh'))

    if melt_dicts:
        frame = melt_stats(frame)

    frame = process_locations(frame)
    frame = frame.with_columns((pl.col('num_trips') / pl.col('trx_amt')).alias('rh_frac'))
    frame = frame.drop(['num_trips', 'trx_amt'])
    return frame


def read_data(
        path: str,
        min_index: int = None,
        max_index: int = None,
        melt_dicts: bool = False
) -> pd.DataFrame:
    features_filenames = sorted([x for x in os.listdir(os.path.join(path, 'features')) if '.pq' in x])
    sessions_filenames = sorted([x for x in os.listdir(os.path.join(path, 'sessions')) if '.pq' in x])

    print(f'Features: {len(features_filenames)}; Sessions: {len(sessions_filenames)}')

    if min_index is not None or max_index is not None:
        print(f'reading from {min_index} to {max_index}')
        features_filenames = features_filenames[min_index:max_index]
        sessions_filenames = sessions_filenames[min_index:max_index]
    else:
        pass

    assert len(features_filenames) == len(sessions_filenames), 'Files count does not match!'

    df = []

    for f_filename, s_filename in tqdm(
            zip(features_filenames, sessions_filenames), 'Reading and processing data...', total=len(features_filenames)
    ):
        assert f_filename == s_filename, f'Filenames {f_filename} and {s_filename} do not match!'

        sessions = pl.read_parquet(os.path.join(os.path.join(path, 'sessions'), s_filename))
        features = pl.read_parquet(os.path.join(os.path.join(path, 'features'), f_filename))

        features = denoise_hour_stats(features)

        for col in ['week_stats', 'hour_stats', 'hour_denoised_stats']:
            if col in features.columns:
                features = dict_stats_to_norm_cols(features, col=col, prefix=col.replace('_stats', ''))

        sub = sessions.join(features, on=['valid_date', 'customer_id'], how='inner')
        sub = process_day(sub, melt_dicts)
        sub = sub.filter(pl.col('min_dist_to_known_loc') <= 40)  # user is too far away from usual location
        df.append(sub)

    frame = pl.concat(df, how='vertical')

    print('Removing duplicated data...')
    frame = _deduplicate_data(frame)

    print('Filtering invalid data...')
    frame = filter_invalid_service_area_id(frame)
    frame = filter_invalid_locations(frame)
    frame = frame.drop(['country_name', 'service_area_id'])

    print('Done.')
    return frame.to_pandas()
