import pprint
from decimal import Decimal

import six
import typing
from datetime import datetime

T = typing.TypeVar('T')


class Model(object):
    # swaggerTypes: The key is attribute name and the
    # value is attribute type.
    swagger_types = {}

    # attributeMap: The key is attribute name and the
    # value is json key in definition.
    attribute_map = {}

    # integer fields that will be converted to hex for representation
    int_to_hex = set()

    # integer fields that will be converted to string for representation
    int_to_str = set()

    def __init__(self, **fields):
        self._keys = list(fields.keys())
        for k, v in fields.items():
            setattr(self, k, v)

    def to_dict(self):
        """Returns the model properties as a dict

        :rtype: dict
        """
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            nattr = self.attribute_map[attr]
            value = getattr(self, attr, None)
            if isinstance(value, list):
                result[nattr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[nattr] = value.to_dict()
            elif isinstance(value, dict):
                result[nattr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            elif isinstance(value, datetime):
                result[nattr] = value.isoformat()
            elif isinstance(value, Decimal):
                result[nattr] = int(value)
            else:
                result[nattr] = value

            if nattr in self.int_to_hex and isinstance(result[nattr], int):
                result[nattr] = hex(result[nattr])

            if nattr in self.int_to_str and isinstance(result[nattr], int):
                result[nattr] = str(result[nattr])

        return result

    @classmethod
    def select_fields(cls):
        return ','.join(['"{}"'.format(f) for f in cls.attribute_map.keys()])

    def to_str(self):
        """Returns the string representation of the model

        :rtype: str
        """
        return pprint.pformat(self.to_dict())

    def __getitem__(self, item):
        return getattr(self, item, None)

    def __repr__(self):
        """For `print` and `pprint`"""
        params = {key: getattr(self, key, None) for key in self._keys}
        return f"<{self.__class__.__name__}: {params} >"

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
