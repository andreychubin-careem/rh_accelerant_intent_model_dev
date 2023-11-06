import os
import polars as pl
import pandas as pd
from typing import Optional, Callable, Dict
from tqdm import tqdm
from pyhive import presto


try:
    from .sql.queries import get_intents, get_rh_features, get_food_features
except ImportError:
    from sql.queries import get_intents, get_rh_features, get_food_features


def clean_sessions(sessions: pl.DataFrame) -> pl.DataFrame:
    service_sessions = sessions.filter(pl.col('booking_id') != 0)
    service_sessions_pos = service_sessions.filter(pl.col('is_trip_ended') == 1)
    service_sessions_neg = service_sessions.filter(pl.col('is_trip_ended') == 0)
    service_sessions_neg = service_sessions_neg.filter(
        ~pl.col('sessionuuid').is_in(service_sessions_pos['sessionuuid'].to_list())
    )
    service_sessions = pl.concat([service_sessions_pos, service_sessions_neg], how='vertical')
    service_sessions = service_sessions.sort(['is_trip_ended', 'ts'], descending=[True, False])\
        .unique(subset=['booking_id'], keep='first')

    sa_sessions = sessions.filter(pl.col('booking_id') == 0)
    sa_sessions = sa_sessions.filter(
        ~pl.col('sessionuuid').is_in(service_sessions['sessionuuid'].to_list())
    )
    sa_sessions = sa_sessions.sort('ts', descending=False)\
        .unique(subset=['customer_id', 'sessionuuid'], keep='first')

    sessions = pl.concat([service_sessions, sa_sessions], how='vertical')
    sessions = sessions.with_columns(pl.col('latitude').cast(pl.Float64)) \
        .with_columns(pl.col('longitude').cast(pl.Float64))
    return sessions.sort('ts', descending=False)


class TargetService(object):
    def __init__(self, RH: Optional[Dict[str, Callable]] = None, FOOD: Optional[Dict[str, Callable]] = None):
        if RH is None:
            self.RH = {'features': get_rh_features, 'intents': get_intents}
        if FOOD is None:
            self.FOOD = {'features': get_food_features, 'intents': get_intents}


class PrestoLoader(object):
    def __init__(
            self,
            up_to_date: str,
            days_back: int,
            service: Optional[dict] = None,
            history_horizon: int = 60,
            percentile: float = 0.8,
            path: str = 'data'
    ):
        if service is None:
            service = TargetService().RH

        self.up_to_date = up_to_date
        self.days_back = days_back
        self.service = service
        self.history_horizon = history_horizon
        self.percentile = percentile
        self.conn = None
        self.path = path
        self.features_path = os.path.join(self.path, 'features')
        self.sessions_path = os.path.join(self.path, 'sessions')

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        if not os.path.exists(self.features_path):
            os.makedirs(self.features_path)

        if not os.path.exists(self.sessions_path):
            os.makedirs(self.sessions_path)

    def _initiate(self):
        self.conn = presto.connect(
            host='presto-python-r-script-cluster.careem-engineering.com',
            username='presto_python_r',
            port=8080
        )

    def _load_chunk(self, query: str) -> pl.DataFrame:
        try:
            return pl.read_database(query=query, connection=self.conn)
        except presto.DatabaseError:
            self._initiate()
            return pl.read_database(query=query, connection=self.conn)

    def terminate(self) -> None:
        self.conn.close()

    def load(self, include_sessions: bool = True, include_features: bool = True) -> None:
        dates = pd.date_range(end=self.up_to_date, periods=self.days_back, freq='D').astype(str).values
        self._initiate()

        for date in tqdm(dates):
            if include_features:
                get_user_features = self.service['features']
                features = self._load_chunk(get_user_features(date, self.history_horizon, self.percentile))
                features.write_parquet(os.path.join(self.features_path, f'{date}.pq'))

            if include_sessions:
                get_intents = self.service['intents']
                sessions = self._load_chunk(get_intents(date, self.history_horizon, self.percentile))
                sessions = clean_sessions(sessions)
                sessions.write_parquet(os.path.join(self.sessions_path, f'{date}.pq'))

        self.terminate()
        print(f'Data written to {self.features_path} and {self.sessions_path}')
