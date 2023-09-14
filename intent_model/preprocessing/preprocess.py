import os
import gc
import pandas as pd
from tqdm import tqdm

try:
    from .preprocess_functions import (
        preprocess_locations,
        dict_stats_to_cols,
        minute_cyclical,
        encode_cyclical_time,
        fill_service_area_id,
        melt_stats,
        get_last_ride
    )
    from .filters import filter_invalid_locations, filter_invalid_service_area_id
except ImportError:
    from preprocess_functions import (
        preprocess_locations,
        dict_stats_to_cols,
        minute_cyclical,
        encode_cyclical_time,
        fill_service_area_id,
        melt_stats,
        get_last_ride
    )
    from filters import filter_invalid_locations, filter_invalid_service_area_id


def read_data(
        path: str,
        min_index: int = None,
        max_index: int = None,
        melt_dicts: bool = False,
        add_last_trip: bool = False
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

        features = pd.read_parquet(os.path.join(os.path.join(path, 'features'), f_filename))
        sessions = pd.read_parquet(os.path.join(os.path.join(path, 'sessions'), s_filename))

        sessions = sessions[sessions.customer_id.isin(features.customer_id)]
        sub = sessions.merge(features, on=['customer_id', 'valid_date'], how='left')

        del features, sessions

        sub['rh'] = (sub['booking_id'] != 0).astype(int)
        sub['ts'] = pd.to_datetime(sub['ts'].apply(lambda x: x.split('.')[0]))

        sub = preprocess_locations(sub)

        for col in ['week_stats', 'hour_stats']:
            sub = dict_stats_to_cols(sub, col, col.replace('_stats', ''), include_norm=True)

        sub['weekday'] = sub.ts.dt.weekday
        sub = minute_cyclical(sub)

        sub['rh_frac'] = sub['num_trips'] / sub['trx_amt']
        sub['known_loc_occ'] = sub['known_loc_occ'] / sub['num_trips']
        sub['is_freq'] = sub['is_freq'].fillna(0).astype(int)

        if melt_dicts:
            sub = melt_stats(sub)

        df.append(sub)
        _ = gc.collect()

    frame = pd.concat(df, ignore_index=True)

    print('Removing duplicated data...')
    rh_frame = frame[frame.booking_id != 0].copy()
    sa_frame = frame[frame.booking_id == 0].copy()

    sa_frame = sa_frame[~sa_frame.sessionuuid.isin(rh_frame.sessionuuid)] \
        .sort_values(['sessionuuid', 'ts']) \
        .drop_duplicates(subset=['sessionuuid'], keep='first')

    rh_frame = rh_frame.sort_values(['sessionuuid', 'is_trip_ended', 'ts'], ascending=[True, False, True]) \
        .drop_duplicates(subset=['sessionuuid'], keep='first')

    frame = pd.concat([rh_frame, sa_frame], ignore_index=True).sort_values('ts')
    _ = gc.collect()

    print('Filling missing "service_area_id"...')
    frame = fill_service_area_id(frame)
    frame = filter_invalid_service_area_id(frame)
    frame = filter_invalid_locations(frame)

    if add_last_trip:
        frame = get_last_ride(frame)

    print('Done.')
    return frame.reset_index(drop=True)
