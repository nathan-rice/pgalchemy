from typing import Sequence
from datetime import date, time, datetime, timedelta

Array = type('Array', Sequence.__bases__, dict(Sequence.__dict__))

mappings = {
    bool: 'boolean',
    int: 'int',
    float: 'numeric',
    bytes: 'bytea',
    bytearray: 'bytea',
    str: 'text',
    date: 'date',
    time: 'time without time zone',
    datetime: 'timestamp without time zone',
    timedelta: 'interval'
}
