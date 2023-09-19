import os
import pyarrow.parquet as pq
import polars as pl
import pandas as pd

from tqdm import tqdm

try:
    from .filters import filter_invalid_locations, filter_invalid_service_area_id
    from .preprocess_functions import (
        process_time,
        process_locations,
        melt_stats,
        dict_stats_to_norm_cols
    )
except ImportError:
    from filters import filter_invalid_locations, filter_invalid_service_area_id
    from preprocess_functions import (
        process_time,
        process_locations,
        melt_stats,
        dict_stats_to_norm_cols
    )


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

    assert len(features_filenames) == len(sessions_filenames), 'File count does not match!'

    df = []

    for f_filename, s_filename in tqdm(
            zip(features_filenames, sessions_filenames), 'Reading and processing data...', total=len(features_filenames)
    ):
        assert f_filename == s_filename, f'Filenames {f_filename} and {s_filename} do not match!'

        sessions = pq.read_table(os.path.join(os.path.join(path, 'sessions'), s_filename))
        sessions = pl.from_arrow(sessions)

        if '__index_level_0__' in sessions.columns:
            sessions = sessions.drop('__index_level_0__')

        sessions = filter_invalid_service_area_id(sessions)
        sessions = filter_invalid_locations(sessions)
        sessions = process_time(sessions)
        sessions = sessions.with_columns(pl.col('booking_id').ne(0).cast(pl.Int64).alias('rh'))

        features = pq.read_table(os.path.join(os.path.join(path, 'features'), f_filename))
        features = pl.from_arrow(features)

        if '__index_level_0__' in features.columns:
            features = features.drop('__index_level_0__')

        sub = sessions.join(features, on=['valid_date', 'customer_id'], how='inner')

        for col in ['week_stats', 'hour_stats']:
            sub = dict_stats_to_norm_cols(sub, col=col, prefix=col.split('_')[0])

        sub = process_locations(sub)
        sub = sub.with_columns((pl.col('num_trips') / (pl.col('trx_amt'))).alias('rh_frac'))
        sub = sub.with_columns((pl.col('known_loc_occ') / (pl.col('num_trips'))).alias('known_loc_occ'))
        sub = sub.drop(['num_trips', 'trx_amt'])

        if melt_dicts:
            sub = melt_stats(sub)

        df.append(sub)

    frame = pl.concat(df, how='vertical')

    print('Removing duplicated data...')
    rh_frame = frame.filter(pl.col('booking_id').ne(0))
    sa_frame = frame.filter(pl.col('booking_id').eq(0))

    sa_frame = sa_frame.filter(~pl.col('sessionuuid').is_in(rh_frame['sessionuuid'].to_list())) \
        .sort(by=['sessionuuid', 'ts']) \
        .unique(subset=['sessionuuid'], keep='first')

    rh_frame = rh_frame.sort(by=['sessionuuid', 'is_trip_ended', 'ts'], descending=[False, True, False]) \
        .unique(subset=['sessionuuid'], keep='first')

    frame = pl.concat([rh_frame, sa_frame], how='vertical').sort(by=['ts'])
    frame = frame.drop(['country_name', 'service_area_id'])

    print('Done.')
    return frame.to_pandas()
