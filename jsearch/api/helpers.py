import json

from aiohttp import web


DEFAULT_LIMIT = 20
MAX_LIMIT = 20
DEFAULT_OFFSET = 0
DEFAULT_ORDER = 'desc'


class Tag:
    """
    Block tag, can be block number, block hash or 'latest' lable
    """
    LATEST = 'latest'
    NUMBER = 'number'
    HASH = 'hash'

    __types = [LATEST, NUMBER, HASH]

    def __init__(self, type_, value):
        assert type_ in self.__types, 'Invalid tag type: {}'.format(type_)
        self.type = type_
        self.value = value

    def is_number(self):
        return self.type == self.NUMBER

    def is_hash(self):
        return self.type == self.HASH

    def is_latest(self):
        return self.type == self.LATEST


def get_tag(request):
    tag_value = request.match_info.get('tag') or request.query.get('tag', Tag.LATEST)
    if tag_value.isdigit():
        value = int(tag_value)
        type_ = Tag.NUMBER
    elif tag_value == Tag.LATEST:
        value = tag_value
        type_ = Tag.LATEST
    else:
        value = tag_value
        type_ = Tag.HASH
    return Tag(type_, value)


def validate_params(request):
    params = {}
    errors = {}

    limit = request.query.get('limit')
    if limit and limit.isdigit():
        params['limit'] = min(int(limit), MAX_LIMIT)

    elif limit and not limit.isdigit():
        errors['limit'] = 'Limit value should be valid integer, got "{}"'.format(limit)
    else:
        params['limit'] = DEFAULT_LIMIT

    offset = request.query.get('offset')
    if offset and offset.isdigit():
        params['offset'] = int(offset)
    elif offset and not offset.isdigit():
        errors['offset'] = 'Offset value should be valid integer, got "{}"'.format(offset)
    else:
        params['offset'] = DEFAULT_OFFSET

    order = request.query.get('order', '').lower()
    if order and order in ['asc', 'desc']:
        params['order'] = order
    elif order:
        errors['order'] = 'Order value should be one of "asc", "desc", got "{}"'.format(order)
    else:
        params['order'] = DEFAULT_ORDER

    if errors:
        raise web.HTTPBadRequest(text=json.dumps(errors), content_type='application/json')
    return params
