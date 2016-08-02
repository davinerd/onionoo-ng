from os import listdir
from os.path import isfile, join
import sys
from engines import db
from tornado import escape
from tornado.ioloop import IOLoop
from tornado import gen


@gen.coroutine
def insert_relays(r):
    print "insert rel"
    yield db.insert_relays(r)


@gen.coroutine
def insert_bridges(b):
    print "insert br"
    yield db.insert_bridges(b)


@gen.coroutine
def main():
    onlyfiles = [join(sys.argv[1], f) for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]

    for f in onlyfiles:
        with open(f, 'r') as j:
            json_op = escape.json_decode(j.read())

        if 'fingerprint' in json_op:
            yield insert_relays([json_op])
        else:
            yield insert_bridges([json_op])


IOLoop.current().run_sync(main)
