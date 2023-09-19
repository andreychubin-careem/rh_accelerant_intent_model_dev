import polars as pl

try:
    from ._utils import BOUNDS_DICT, VALID_SERVICE_AREA_IDS
except ImportError:
    from _utils import BOUNDS_DICT, VALID_SERVICE_AREA_IDS


def filter_invalid_locations(data: pl.DataFrame) -> pl.DataFrame:
    pl_data = []

    for country in BOUNDS_DICT.keys():
        pl_data.append(
            data.filter(
                (pl.col('country_name') == country) &
                (pl.col('latitude') <= BOUNDS_DICT[country][0][1]) &
                (pl.col('latitude') >= BOUNDS_DICT[country][0][0]) &
                (pl.col('longitude') <= BOUNDS_DICT[country][1][1]) &
                (pl.col('longitude') >= BOUNDS_DICT[country][1][0])
            )
        )

    return pl.concat(pl_data, how='vertical')


def filter_invalid_service_area_id(data: pl.DataFrame) -> pl.DataFrame:
    return data.filter(pl.col('service_area_id').is_in(VALID_SERVICE_AREA_IDS))
