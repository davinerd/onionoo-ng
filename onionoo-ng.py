from tornado.web import RequestHandler, Application, HTTPError
from tornado.ioloop import IOLoop
import settings as ts
from engines.es import ES
from tornado import gen
#from guppy import hpy
#from timer import Timer
#from pympler import tracker

# with Timer() as t:
#   blablabal
# print "=> elapsed time: %s s" % t.secs

#t = tracker.SummaryTracker()

#h = hpy()
#print h.heap()

class BaseHandler(RequestHandler):
    #def write_error(self, status_code, **kwargs):
    #    self.set_header('Content-Type', 'text/json')
    #    self.finish({'code': ts.STATUS_ERROR, 'message': self._reason, 'version': ts.VERSION})
    pass


class APIError(HTTPError):
    pass


class APIBaseHandler(BaseHandler):
    #def write_error(self, status_code, **kwargs):
    #    self.set_header('Content-Type', 'text/json')
    #    self.finish({'code': ts.STATUS_ERROR, 'message': self._reason, 'version': ts.VERSION})
    pass


class IndexHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.write("Hello my friend.")


class APIDetailsHandler(APIBaseHandler):
    @gen.coroutine
    def get(self):
        query_params = dict()
        query_extra = dict()
        es_index = None
        msg = {
            'version': ts.VERSION,
            'relays': [],
            'bridges': [],
            'relays_published': None,
            'bridges_published': None
        }

        if len(self.request.query) > 0:
            # making everithing lowercase will help us later on
            query_split = self.request.query.lower().split('&')
            for p in query_split:
                p_split = p.split('=')
                if len(p_split) == 1 or (p_split[0] not in ts.OOO_QUERYPARAMS and p_split[0] not in ts.OOO_QUERYPARAMS_EXTRAS):
                    # raise APIError(reason="invalid query parameters")
                    raise HTTPError(status_code=400)

                if p_split[0] in ts.OOO_QUERYPARAMS_EXTRAS:
                    query_extra[p_split[0]] = p_split[1]
                elif p_split[0] == 'type':
                    es_index = p_split[1].lower()
                else:
                    query_params[p_split[0]] = p_split[1]

        if es_index:
            if es_index != "relay" and es_index != "bridge":
                raise HTTPError(status_code=400)
            # little trick to be consistent with the old protocol
            msg[es_index+"s"] = yield self.application.es_instance.search(es_index, query_params, query_extra)
        else:
            msg['relays'] = yield self.application.es_instance.search("relay", query_params, query_extra)
            msg['bridges'] = yield self.application.es_instance.search("bridge", query_params, query_extra)

        self.write(msg)
        self.finish()


class ErrorHandler(BaseHandler):
    pass


class App(Application):
    def __init__(self):
        handlers = [
            (r"/", IndexHandler),
            (r"/details", APIDetailsHandler),
            (r"/(.*)", ErrorHandler)
        ]
        settings = {
            "debug": ts.DEBUG,
            "decompress_request": True,
            "compress_response": True
        }
        super(App, self).__init__(handlers, **settings)

        self.es_instance = ES()

        try:
            IOLoop.current().run_sync(self.es_instance.check_connection)
        except Exception:
            print "ERROR: ElasticSearch not running...quitting"
            IOLoop.current().stop()
            exit()


if __name__ == '__main__':
    try:
        app = App()
        app.listen(ts.LISTENING_PORT)
        print "Onionoo-ng listening on {0}".format(ts.LISTENING_PORT)
        IOLoop.current().start()
    except KeyboardInterrupt:
        IOLoop.current().stop()
