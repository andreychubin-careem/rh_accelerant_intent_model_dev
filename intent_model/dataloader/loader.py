import os
import json
import pandas as pd
from tqdm import tqdm
from pyhive import presto


try:
    from .sql.queries import get_intents, get_user_features
except ImportError:
    from sql.queries import get_intents, get_user_features


class PrestoLoader(object):
    def __init__(
            self,
            up_to_date: str,
            days_back: int,
            history_horizon: int = 60,
            percentile: float = 0.8,
            path: str = 'data'
    ):
        self.up_to_date = up_to_date
        self.days_back = days_back
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

    def _load_chunk(self, query: str) -> pd.DataFrame:
        try:
            return pd.read_sql(sql=query, con=self.conn)
        except presto.DatabaseError:
            self._initiate()
            return pd.read_sql(sql=query, con=self.conn)

    @staticmethod
    def _sessions_clean(sessions: pd.DataFrame) -> pd.DataFrame:
        rh_sessions = sessions[sessions.booking_id != 0].copy()
        rh_sessions_pos = rh_sessions[rh_sessions.is_trip_ended == 1].copy()
        rh_sessions_neg = rh_sessions[rh_sessions.is_trip_ended == 0].copy()
        rh_sessions_neg = rh_sessions_neg[~rh_sessions_neg.sessionuuid.isin(rh_sessions_pos.sessionuuid)]
        rh_sessions = pd.concat([rh_sessions_pos, rh_sessions_neg])
        rh_sessions = rh_sessions.sort_values(['is_trip_ended', 'ts'], ascending=[False, True])\
            .drop_duplicates(subset=['booking_id'], keep='first')

        sa_sessions = sessions[sessions.booking_id == 0].copy()
        sa_sessions = sa_sessions[~sa_sessions.sessionuuid.isin(rh_sessions.sessionuuid)]
        sa_sessions = sa_sessions.sort_values('ts').drop_duplicates(subset=['customer_id', 'sessionuuid'], keep='first')

        sessions = pd.concat([rh_sessions, sa_sessions], ignore_index=True)
        sessions['latitude'] = sessions['latitude'].astype(float)
        sessions['longitude'] = sessions['longitude'].astype(float)
        return sessions.sort_values('ts')

    def terminate(self) -> None:
        self.conn.close()

    def load(self, include_sessions: bool = True, include_features: bool = True) -> None:
        dates = pd.date_range(end=self.up_to_date, periods=self.days_back, freq='D').astype(str).values
        self._initiate()

        for date in tqdm(dates):
            if include_features:
                features = self._load_chunk(get_user_features(date, self.history_horizon, self.percentile))

                for column in ['week_stats', 'week_stats_recom', 'hour_stats', 'hour_stats_recom', 'locations']:
                    if column in features.columns:
                        features[column] = features[column].apply(json.dumps)

                features.to_parquet(os.path.join(self.features_path, f'{date}.pq'), index=False)

            if include_sessions:
                sessions = self._load_chunk(get_intents(date, self.history_horizon, self.percentile))
                sessions = self._sessions_clean(sessions)
                sessions.to_parquet(os.path.join(self.sessions_path, f'{date}.pq'), index=False)

        self.terminate()
        print(f'Data written to {self.features_path} and {self.sessions_path}')
