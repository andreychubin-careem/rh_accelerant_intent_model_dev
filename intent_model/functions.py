import numpy as np
import pandas as pd
import tensorflow_decision_forests as tfdf

from typing import Iterable, Tuple, Union, Any
from tqdm import tqdm

from sklearn.metrics import precision_recall_curve


def get_relevance(
        data: pd.DataFrame,
        X: pd.DataFrame,
        model: Union[tfdf.keras.GradientBoostedTreesModel, Any],
        thresholds: Iterable[float] = (0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
) -> pd.DataFrame:
    if hasattr(model, 'predict_proba'):
        scores = model.predict_proba(X)[:, 1]
    else:
        scores = model.predict(X, verbose=0).flatten()

    sub = data[['ts', 'target', 'is_freq', 'rh']].copy()
    sub['score'] = scores
    n_sessions = len(data)
    n_rh_sessions = len(data[data.rh == 1])
    n_relevant_sessions = len(data[(data.rh == 1) & (data.is_freq == 1)])

    d_dict = {
        'threshold': [],
        'sessions': [],
        'rh_sessions': [],
        'relevant_sessions': []
    }

    for t in tqdm(thresholds):
        d_dict['threshold'].append(t)
        d_dict['sessions'].append(len(sub[sub.score > t]))
        d_dict['rh_sessions'].append(len(sub[(sub.score > t) & (sub.rh == 1)]))
        d_dict['relevant_sessions'].append(len(sub[(sub.score > t) & (sub.is_freq == 1)]))

    res_df = pd.DataFrame(d_dict)
    res_df['relevance (rh total)'] = res_df['rh_sessions'] / res_df['sessions']
    res_df['relevance (freq only)'] = res_df['relevant_sessions'] / res_df['sessions']
    res_df['% of freq detected'] = res_df['relevant_sessions'] / n_relevant_sessions
    res_df['sa_coverage'] = res_df['sessions'] / n_sessions
    res_df['rh_coverage'] = res_df['rh_sessions'] / n_rh_sessions

    return res_df[[
        'threshold',
        'sa_coverage',
        'rh_coverage',
        'relevance (rh total)',
        'relevance (freq only)',
        '% of freq detected'
    ]]


def get_optimal_threshold(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        target_precision: float = 0.90
) -> Tuple[float, float]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_pred)
    max_recall = np.max(recall[np.where(precision >= target_precision)[0]])
    index = np.where(recall == max_recall)[0][0]
    return thresholds[index], recall[index]
