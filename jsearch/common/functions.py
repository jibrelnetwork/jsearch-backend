from sqlalchemy.dialects.postgresql import array
from typing import List

from sqlalchemy.sql.functions import GenericFunction


# WTF: This goofy construction with class and a function is here to pass
# function arguments as named parameters and type-check them like so:
#
#   select([get_assets_summaries_f(addresses=['0x...'], assets=['0x...])])
#


class GetAssetsSummaries(GenericFunction):
    name = 'get_assets_summaries'


def get_assets_summaries_f(addresses: List[str], assets: List[str]) -> GetAssetsSummaries:
    return GetAssetsSummaries(array(tuple(addresses)), array(tuple(assets)))
