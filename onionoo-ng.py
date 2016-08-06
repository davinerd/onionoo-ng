from tornado.web import RequestHandler, Application, HTTPError
from tornado.ioloop import IOLoop
import settings as ts
from engines.es import ES
from tornado import gen


# from guppy import hpy
# from timer import Timer
# from pympler import tracker

# with Timer() as t:
#   blablabal
# print "=> elapsed time: %s s" % t.secs

# t = tracker.SummaryTracker()

# h = hpy()
# print h.heap()

class BaseHandler(RequestHandler):
    # def write_error(self, status_code, **kwargs):
    #    self.set_header('Content-Type', 'text/json')
    #    self.finish({'code': ts.STATUS_ERROR, 'message': self._reason, 'version': ts.PROTOCOL_VERSION})
    pass


class APIError(HTTPError):
    pass


class APIBaseHandler(BaseHandler):
    # def write_error(self, status_code, **kwargs):
    #    self.set_header('Content-Type', 'text/json')
    #    self.finish({'code': ts.STATUS_ERROR, 'message': self._reason, 'version': ts.PROTOCOL_VERSION})

    msg = {
        'version': ts.PROTOCOL_VERSION,
        'relays': [],
        'bridges': [],
        'relays_published': None,
        'bridges_published': None
    }

    @staticmethod
    def parse_query(q):
        query_params = dict()
        query_extra = dict()
        index = None
        # making everything lowercase will help us later on
        query_split = q.lower().split('&')
        for p in query_split:
            p_split = p.split('=')
            if len(p_split) == 1 or (
                            p_split[0] not in ts.OOO_QUERYPARAMS and p_split[0] not in ts.OOO_QUERYPARAMS_EXTRAS):
                # raise APIError(reason="invalid query parameters")
                raise HTTPError(status_code=400)

            if p_split[0] in ts.OOO_QUERYPARAMS_EXTRAS:
                query_extra[p_split[0]] = p_split[1]
            elif p_split[0] == 'type':
                index = p_split[1].lower()
            else:
                query_params[p_split[0]] = p_split[1]

        return {'es_index': index, 'params': query_params, 'extra': query_extra}


class IndexHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.write("Hello my friend.")


class APIHandler(APIBaseHandler):
    @gen.coroutine
    def get(self, doc_type):
        parsed_query = {'es_index': None, 'params': dict(), 'extra': dict()}

        if len(self.request.query) > 0:
            parsed_query = self.parse_query(self.request.query)

        if 'fields' in parsed_query['extra'] and doc_type == "summary":
            # in 'summary' we need to ignore the 'fields' parameter.
            # we can ignore user's input in this case.
            # hardcoding the fields helps up in speed up the query
            parsed_query['extra']['fields'] = "nickname,flags,fingerprint,hashed_fingerprint,or_addresses,dir_address"

        es_index = parsed_query['es_index']

        # by default, assume doc_type == "details"
        if es_index:
            if es_index != "relay" and es_index != "bridge":
                raise HTTPError(status_code=400)
            # little trick to be consistent with the old protocol
            self.msg[es_index + "s"] = yield self.application.es_instance.search(es_index, parsed_query['params'],
                                                                                 parsed_query['extra'])
        else:
            self.msg['relays'] = yield self.application.es_instance.search("relay", parsed_query['params'],
                                                                           parsed_query['extra'])
            self.msg['bridges'] = yield self.application.es_instance.search("bridge", parsed_query['params'],
                                                                            parsed_query['extra'])

        if doc_type == "summary":
            if es_index:
                self.msg[es_index + "s"] = self.make_summary(self.msg[es_index + "s"], es_index)
            else:
                self.msg['relays'] = self.make_summary(self.msg['relays'], 'relay')
                self.msg['bridges'] = self.make_summary(self.msg['bridges'], 'bridge')

        self.msg['relays_published'] = yield self.application.es_instance.get_last_node("relay")
        self.msg['bridges_published'] = yield self.application.es_instance.get_last_node("bridge")

        self.write(self.msg)
        self.finish()

    @staticmethod
    def make_summary(nodes, index):
        ret_d = list()
        for node in nodes:
            entry = dict()
            if 'nickname' in node and node['nickname'] != "Unnamed" and len(node['nickname']) <= 19:
                entry['n'] = node['nickname']

            if index == "relay":
                entry['f'] = node['fingerprint']
            else:
                entry['h'] = node['hashed_fingerprint']

            if index == "relay":
                # this is the legacy mode
                # entry['a'] = [ip.rsplit(':', 1)[0] for ip in node['or_addresses']]

                # this is the proposed method
                entry['a'] = [ip.rsplit(':', 1)[0].strip("[]") for ip in node['or_addresses']]

            if 'flags' in node and 'Running' in node['flags']:
                entry['r'] = True
            else:
                entry['r'] = False

            ret_d.append(entry)

        return ret_d


class ErrorHandler(BaseHandler):
    pass


class App(Application):
    def __init__(self):
        handlers = [
            (r"/", IndexHandler),
            (r"/(details|summary)", APIHandler),
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
        except EnvironmentError:
            print "ERROR: ElasticSearch not running...quitting"
            IOLoop.current().stop()
            exit()


if __name__ == '__main__':
    try:
        app = App()
        app.listen(ts.LISTENING_PORT)
        print "Onionoo-ng (v{0}) listening on {1}".format(ts.VERSION, ts.LISTENING_PORT)
        IOLoop.current().start()
    except KeyboardInterrupt:
        IOLoop.current().stop()
