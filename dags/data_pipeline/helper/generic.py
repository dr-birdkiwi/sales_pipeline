from typing import Tuple
from data_pipeline.helper.constants import STORES


def get_store_city_country(id: int) -> Tuple[str, str]:
    """A simple function that returns (city, country) based on input integer"""
    try:
        idx = id % len(STORES)
    except:
        idx = 0

    city, country = STORES[idx] if STORES else (None, None)

    return (city, country)
