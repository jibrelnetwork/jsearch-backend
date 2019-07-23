import pytest
from marshmallow import ValidationError
from pytest import fail

from jsearch.api.helpers import Tag


@pytest.mark.parametrize(
    "init_tags, value, expected, exception",
    [
        ({Tag.LATEST}, Tag.LATEST, Tag.LATEST, None),
        ({Tag.LATEST}, 'Latest', Tag.LATEST, None),
        ({Tag.LATEST}, 'LATEST', Tag.LATEST, None),
        ({Tag.LATEST}, 'aaa', Tag.LATEST, ValidationError('Not a valid number or tag.')),
        ([Tag.LATEST], 'aaa', Tag.LATEST, ValueError("Tags should be a set instead of ['latest']")),
    ]
)
async def test_positive_or_integer_field(init_tags, value, expected, exception: ValidationError):
    from jsearch.api.serializers.fields import PositiveIntOrTagField

    try:
        result = PositiveIntOrTagField(init_tags).deserialize(value)
    except ValidationError as e:
        assert isinstance(exception, e.__class__)
        assert exception.messages == e.messages
    except ValueError as e:
        assert isinstance(exception, e.__class__)
        assert getattr(exception, 'message', "") == getattr(e, 'message', "")
    else:
        if exception:
            fail(f'Missed exception {exception}')
        assert result == expected
