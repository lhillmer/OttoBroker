from json import JSONEncoder
from decimal import Decimal
from datetime import datetime

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)