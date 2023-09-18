import numpy as np
import pandas as pd
import catboost as cb

from typing import Iterable, Tuple
from tqdm import tqdm

from sklearn.metrics import precision_recall_curve


def get_conversions(
        data: pd.DataFrame,
        X: pd.DataFrame,
        model: cb.CatBoostClassifier,
        thresholds: Iterable[float] = (0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
) -> pd.DataFrame:
    scores = model.predict_proba(X)[:, 1]

    d_dict = {
        'threshold': [],
        'sessions': [],
        'rh_sessions': [],
        'relevant_sessions': [],
        'sessions_true_pred': [],
        'relevant_sessions_true_pred': [],
        'rh_sessions_true_pred': []
    }

    for t in tqdm(thresholds):
        d_dict['threshold'].append(t)
        sub = data[['ts', 'target', 'is_freq', 'rh']].assign(sessions=1).copy()
        sub['score'] = scores
        sub['correct_preds'] = (sub['target'] == (sub['score'] > t).astype(int)).astype(int)

        d_dict['sessions'].append(len(sub[sub.score > t]))
        d_dict['rh_sessions'].append(len(sub[(sub.score > t) & (sub.rh == 1)]))
        d_dict['relevant_sessions'].append(len(sub[(sub.score > t) & (sub.is_freq == 1)]))
        d_dict['sessions_true_pred'].append(len(sub[(sub.score > t) & (sub.correct_preds == 1)]))
        d_dict['relevant_sessions_true_pred'].append(
            len(sub[(sub.score > t) & (sub.correct_preds == 1) & (sub.is_freq == 1)]))
        d_dict['rh_sessions_true_pred'].append(len(sub[(sub.score > t) & (sub.correct_preds == 1) & (sub.rh == 1)]))

    res_df = pd.DataFrame(d_dict)
    res_df['all_sessions'] = len(data)
    res_df['all_rh_sessions'] = len(data[data.rh == 1])
    res_df['relevance'] = res_df['relevant_sessions_true_pred'] / res_df['sessions']
    res_df['sa_coverage'] = res_df['sessions'] / res_df['all_sessions']
    res_df['rh_coverage'] = res_df['rh_sessions'] / res_df['all_rh_sessions']

    return res_df[['threshold', 'sa_coverage', 'rh_coverage', 'relevance']]


def get_optimal_threshold(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        target_precision: float = 0.90
) -> Tuple[float, float]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_pred)
    max_recall = np.max(recall[np.where(precision >= target_precision)[0]])
    index = np.where(recall == max_recall)[0][0]
    return thresholds[index], recall[index]
