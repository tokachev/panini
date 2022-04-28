import inspect
import json

import jsonschema
from jsonschema import validate
from .exceptions import ValidationError
from .utils.logger import Logger

_logger = Logger

class Validator:

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        Validator.set_logger()

    @staticmethod
    def set_logger():
        global _logger
        if _logger is None:
            from .app import _app
            _logger = _app.logger

    @classmethod
    def validated_message(cls, message, schema):
        if not type(message) in [dict, list]:
            error = (
                f"Unexpected message. Accepted dict or list but got {type(message)}."
            )
            _logger.error(error)
            raise ValidationError(error)
        if type(message) is list:
            if not cls.__many:
                error = (
                    f"Unexpected message, expected dict got {type(message)}."
                    "You can set many=True in validator if you need to handle list of dicts"
                )
                _logger.error(error)
                raise ValidationError(error)
            result = []
            for m in message:
                result.append(cls._validate_message(cls, m, schema))
            return result
        message = cls._validate_message(message, schema)
        print(f'Validation status: {message}')
        return message

    @classmethod
    def _validate_message(cls, message, schema):
        try:
            validate(instance=message, schema=schema)
        except jsonschema.exceptions.ValidationError as se:
            raise ValidationError(se)
        return message

    # @classmethod
    # def get_schema_from_kv(cls, app, key):
    #     print(app)
    #     print(key)
    #     kv_bucket = json.loads(app.nats.js_client.key_value("json_schemas").get(key).value.decode('utf-8'))
    #     entry = kv_bucket.get(key)
    #     entry_decoded = json.loads(entry.value.decode('utf-8'))
    #     return entry_decoded
