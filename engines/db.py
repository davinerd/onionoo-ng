import settings as ts
from tornado import gen
from pymongo.errors import BulkWriteError
import motor

client = motor.MotorClient('mongodb://localhost:27017')
db = client[ts.DB_NAME]


@gen.coroutine
def insert_bridges(bridges):
    coll_bridges = db[ts.COLL_BRIDGES]
    bulk = coll_bridges.initialize_unordered_bulk_op()

    for bridge in bridges:
        bulk.insert(bridge)

    try:
        yield bulk.execute()
    except BulkWriteError as err:
        print err.details


@gen.coroutine
def insert_relays(relays):
    coll_relays = db[ts.COLL_RELAYS]
    bulk = coll_relays.initialize_unordered_bulk_op()

    for relay in relays:
        bulk.insert(relay)

    try:
        yield bulk.execute()
    except BulkWriteError as err:
        print err.details