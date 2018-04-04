
import re
import os


FILE_TPL = """
from typing import List, Dict, Any

from jsearch.api.models.base_model_ import Model


{classes}
"""


CLASS_TPL = """


class {name}(Model):

    swagger_types = {{
{swagger_types}
    }}

    attribute_map = {{
{attribute_map}
    }}

"""


"""
'Reward': {'properties': {'address': {'example': '0x47071214d1ef76eeb26e9ac3ec6cc965ab8eb75b',
   'format': 'address',
   'type': 'string'},
  'amount': {'example': 500000000000000, 'type': 'integer'}}},

"""

def generate(swagger_scheme):
    """
    Generate models from swagger scheme
    """
    types = swagger_scheme['definitions']
    weights = {}

    def get_type(attr_params):
        type_ = attr_params.get('type')
        if type_ == 'string':
            return 'str'
        elif type_ == 'integer':
            return 'int'
        elif type_ == 'array':
            if 'items' in attr_params:
                item_type = get_type(attr_params['items'])
            else:
                item_type = 'Any'
            return 'List[{}]'.format(item_type)
        elif type_ is None:
            ref = attr_params.get('$ref')
            if ref:
                name = ref.split('/')[-1]
                weights[name] = 1
                return name
            else:
                return 'Any'
        else:
            return 'Any'

    classes = {}

    for name, params in types.items():
        swagger_types = {}
        attribute_map = {}
        for attr_name, attr_params in params['properties'].items():
            attribute_map[cc_to_underscore(attr_name)] = attr_name
            swagger_types[cc_to_underscore(attr_name)] = get_type(attr_params)

        types_str = '\n'.join(["        '{}': {},".format(k, v) for k, v in swagger_types.items()])
        attrs_str = '\n'.join(["        '{}': '{}',".format(k, v) for k, v in attribute_map.items()])

        cls = CLASS_TPL.format(name=name, swagger_types=types_str, attribute_map=attrs_str)
        classes[name] = cls
    classes_defs = [c[1] for c in sorted(classes.items(), key=lambda n: weights.get(n[0], 0), reverse=True)]
    print(weights, classes)
    return FILE_TPL.format(classes=''.join(classes_defs))


def cc_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


if __name__ == '__main__':
    import yaml
    import sys
    swagger_file = open(sys.argv[1], 'r')
    print('Processing Swagger file', swagger_file)

    code = generate(yaml.load(swagger_file))
    dir_ = os.path.dirname(__file__)
    with open(os.path.join(dir_, 'all.py'), 'w') as f:
        f.write(code)
    print(code)
    print('Done')