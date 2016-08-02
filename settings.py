LISTENING_PORT = 8080
DEBUG = False
ASYNC_ES_MAX_CLIENT = 100
ES_TIMEOUT = 10  # in seconds
STATUS_OK = 0
STATUS_ERROR = 1

ES_RESULT_SIZE = 10000

VERSION = 3.1

OOO_QUERYPARAMS = [
    'type',
    'running',
    'search',
    'lookup',
    'fingerprint',
    'country',
    'family',
    'as',
    'flag',
    'first_seen_days',
    'last_seen_days',
    'contact'
]

OOO_QUERYPARAMS_EXTRAS = [
    'fields',
    'order',
    'offset',
    'limit'
]

DB_NAME = "onionoo-ng"
COLL_RELAYS = "relay"
COLL_BRIDGES = "bridge"