import pprint
import json

import six
import typing

from jsearch.api import util

T = typing.TypeVar('T')


class Model(object):
    # swaggerTypes: The key is attribute name and the
    # value is attribute type.
    swagger_types = {}

    # attributeMap: The key is attribute name and the
    # value is json key in definition.
    attribute_map = {}

    def __init__(self, **fields):
        for k, v in fields.items():
            assert k in self.attribute_map, 'Invalid field "{}"'.format(k)
            assert k in self.swagger_types, 'Invalid field "{}"'.format(k)
            setattr(self, k, v)

    @classmethod
    def from_dict(cls: typing.Type[T], dikt) -> T:
        """Returns the dict as a model"""
        return util.deserialize_model(dikt, cls)

    @classmethod
    def from_row(cls: typing.Type[T], row) -> T:
        """Returns the dict as a model"""
        row = dict(row)
        dikt = {}
        fields = json.loads(row.pop('fields'))
        dikt.update(fields)
        for k, v in row.items():
            amap = cls().attribute_map
            if k in amap:
                dikt[amap[k]] = v
        return cls.from_dict(dikt)

    def to_dict(self):
        """Returns the model properties as a dict

        :rtype: dict
        """
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            nattr = self.attribute_map[attr]
            value = getattr(self, attr)
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
            else:
                result[nattr] = value

        return result

    @classmethod
    def select_fields(cls):
        return ','.join(['"{}"'.format(f) for f in cls.attribute_map.keys()])

    def to_str(self):
        """Returns the string representation of the model

        :rtype: str
        """
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
