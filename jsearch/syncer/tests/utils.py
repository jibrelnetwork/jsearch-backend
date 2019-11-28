import json
from decimal import Decimal

from sqlalchemy.engine import ResultProxy
from typing import List, Any, Dict, Optional


def normalize_data(records: List[ResultProxy], nullify_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    data = []
    for record in records:
        row = {}
        for key, value in dict(record).items():
            if isinstance(value, dict):
                value = json.dumps(value)

            if isinstance(value, Decimal):
                value = int(value)

            if key == 'created_at':
                value = None

            if nullify_columns and key in nullify_columns:
                value = None

            row[key] = value
        data.append(row)
    return data
