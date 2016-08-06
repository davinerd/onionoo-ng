import settings
from tornadoes import ESConnection
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, escape
from tornado.web import HTTPError
from datetime import date, timedelta, datetime
from IPy import IP
import urllib
import base64

AsyncHTTPClient.configure(None, max_clients=settings.ASYNC_ES_MAX_CLIENT)


class ES:
    url = None
    index = None
    es = None
    type_mapping = None

    def __init__(self, index=settings.DB_NAME):
        self.es = ESConnection()
        self.es.httprequest_kwargs['headers'] = {'Connection': 'keep-alive'}
        self.es.httprequest_kwargs['request_timeout'] = settings.ES_TIMEOUT
        self.index = index

    @gen.coroutine
    def check_connection(self):
        response = yield self.es.get_by_path("/")

        if response.code != 200 or not response.body:
            raise EnvironmentError

    @gen.coroutine
    def search(self, mapping, query, extras):
        self.type_mapping = mapping
        return_data = list()
        query_string = {"match_all": {}}
        fs = None
        # we need a copy to avoid changes to the original query
        qq = query.copy()

        extra_parameters = self.__parse_extra(extras)

        if extra_parameters['limit'] == 0:
            raise gen.Return(return_data)

        if qq:
            qq = self.__parse_query(qq)

            query_string = {
                "filtered": {
                    "query": {}
                }
            }

            if 'first_seen_days' in qq:
                query_string['filtered'] = {
                    'filter': {
                        'range': qq.pop('first_seen_days')
                    }
                }

            if 'last_seen_days' in qq:
                query_string['filtered'] = {
                    'filter': {
                        'range': qq.pop('last_seen_days')
                    }
                }

            if 'free_search' in qq:
                fs = qq.pop('free_search')

            # 'query' can be empty now due to the pop()
            if not qq and not fs:
                qq = "*"
            else:
                qq = " AND ".join("%s:%s" % (k, v) for (k, v) in qq.items())

            if fs:
                qq += " AND ".join(fs)

            query_string['filtered']['query'] = {
                "query_string": {
                    "query": qq
                }
            }

        body_query = {
            "from": extra_parameters['offset'],
            "_source": extra_parameters['fields'],
            "query": query_string,
            "sort": {extra_parameters['sort']['field']: {"order": extra_parameters['sort']['order'],
                                                         "unmapped_type": extra_parameters['sort']['type']}}
        }

        if settings.DEBUG:
            print "index: {0}".format(self.index)
            print "mapping: {0}".format(self.type_mapping)
            print "body query: {0}".format(body_query)
            print "extra parameters: {0}".format(extra_parameters)

        result = yield self.es.search(index=self.index, type=self.type_mapping, source=body_query,
                                      size=extra_parameters['limit'])

        if result.code != 200 or not result.body:
            raise HTTPError(status_code=result.code)

        result = escape.json_decode(result.body)

        if 'hits' in result and 'hits' in result['hits']:
            return_data = [h['_source'] for h in result['hits']['hits'] if h['_source']]
            if settings.DEBUG:
                print "Got {0} results".format(len(return_data))
        else:
            # generic error related to ES - shouldn't happen...
            raise Exception(result.error)

        raise gen.Return(return_data)

    @gen.coroutine
    def get_last_node(self, mapping):

        body_query = {
            "sort": {
                "last_seen": {
                    "unmapped_type": "string", "order": "desc"
                }
            },
            "query": {
                "match_all": {}
            }, "from": 0, "_source": ["last_seen"], "size": 1
        }

        result = yield self.es.search(index=self.index, type=mapping, source=body_query)

        if result.code != 200 or not result.body:
            raise HTTPError(status_code=result.code)

        result = escape.json_decode(result.body)

        if 'hits' in result and 'hits' in result['hits']:
            last_seen = result['hits']['hits'][0]['_source']['last_seen']
        else:
            # generic error related to ES - shouldn't happen...
            raise Exception(result.error)

        raise gen.Return(last_seen)

    def __parse_query(self, q):
        if 'as' in q:
            q['as_number'] = q.pop('as').split(',')[0]
            if q['as_number'][0] != 'a' and q['as_number'][1] != 's':
                q['as_number'] = "AS{0}".format(q['as_number'])

        if 'flag' in q:
            # remove extra flags
            q['flags'] = q.pop('flag').title().split(',')[0]

        if 'fingerprint' in q:
            if len(q['fingerprint']) != 40:
                raise HTTPError(status_code=400)

            if self.type_mapping == "bridge":
                q['hashed_fingerprint'] = q.pop('fingerprint')

        if 'running' in q and (q['running'].lower() != "true" and q['running'].lower() != "false"):
            raise HTTPError(status_code=400)

        if 'family' in q:
            q['effective_family'] = q.pop('family')

        if 'contact' in q:
            q['contact'] = "*{0}*".format(q['contact'])

        if 'first_seen_days' in q:
            q['first_seen_days'] = self.__extract_range(q['first_seen_days'], 'first_seen')

        if 'last_seen_days' in q:
            q['last_seen_days'] = self.__extract_range(q['last_seen_days'], 'last_seen')

        if 'search' in q:
            # not allowed in the 'search' term as a value of a key
            forbidden_keys = ['search', 'fingerprint', 'order', 'limit', 'offset', 'fields']

            full_search = urllib.unquote(q.pop('search')).decode('utf8').split()
            new_se = list()
            for s in full_search:
                tokenz = s.split(':')
                if len(tokenz) == 1:
                    if self.___is_ip(tokenz[0]):
                        new_se.append("(or_address:{0} OR dir_address:{0})".format(tokenz[0]))
                    elif self.__is_fingerprint(tokenz[0]):
                        fingerprint = self.__transform_fingerprint(tokenz[0])
                        if fingerprint[0] == "$":
                            new_se.append("(fingerprint:{0}* OR hashed_fingerprint:{0}*)".format(fingerprint[1:]))
                        else:
                            new_se.append("(fingerprint:*{0}* OR hashed_fingerprint:*{0}*)".format(fingerprint))
                    else:
                        new_se.append("nickname:*{0}*".format(tokenz[0]))
                else:
                    if tokenz[0] in forbidden_keys:
                        raise HTTPError(status_code=400)

                    new_se.append("{0}:{1}".format(tokenz[0], tokenz[1]))

            q['free_search'] = new_se

        return q

    @staticmethod
    def __parse_extra(extra):
        return_data = {'limit': settings.ES_RESULT_SIZE, 'offset': 0, 'fields': ["*"],
                       'sort': {'field': None, 'order': 'asc', 'type': 'string'}}

        if 'limit' in extra:
            # 'limit' cannot go beyond the settings.ES_RESULT_SIZE value
            if 0 < int(extra['limit']) < settings.ES_RESULT_SIZE:
                return_data['limit'] = int(extra['limit'])
            elif int(extra['limit']) <= 0:
                return_data['limit'] = 0

        if 'offset' in extra and int(extra['offset']) > 0:
            return_data['offset'] = int(extra['offset'])

        # offset + limit must be <= settings.ES_RESULT_SIZE
        if (return_data['offset'] + return_data['limit']) > settings.ES_RESULT_SIZE:
            return_data['limit'] -= return_data['offset']

        if 'fields' in extra:
            return_data['fields'] = extra['fields'].split(',')

        if 'order' in extra:
            field = extra['order']
            if extra['order'][0] == "-":
                return_data['sort']['order'] = "desc"
                field = extra['order'][1:]

            return_data['sort']['field'] = field

        return return_data

    @staticmethod
    def __extract_range(val, field):
        drange = val.split('-')
        # malformed range
        if len(drange) > 2:
            raise HTTPError(status_code=400)

        start_days = 0
        end_days = 0

        try:
            if len(drange) == 1:
                # print "X"
                start_days = int(drange[0])
                end_days = start_days
            elif len(drange) == 2:
                if drange[0] == '':
                    # print "-X"
                    end_days = int(drange[1])
                elif drange[1] == '':
                    # print "X-"
                    start_days = int(drange[0])
                    end_days = datetime(1970, 1, 1).days
                else:
                    # print "X-Y"
                    start_days = int(drange[0])
                    end_days = int(drange[1])
        except ValueError:
            raise HTTPError(status_code=400)

        if start_days > end_days:
            raise HTTPError(status_code=400)

        d1 = date.today() - timedelta(days=start_days)
        d2 = date.today() - timedelta(days=end_days)

        query_range = {
            field: {
                'gte': d2.strftime("%Y-%m-%d") + " 00:00:00",
                'lte': d1.strftime("%Y-%m-%d") + " 23:59:59"
            }
        }

        return query_range

    @staticmethod
    def ___is_ip(ip):
        try:
            IP(ip)
            return True
        except ValueError:
            return False

    @staticmethod
    def __is_fingerprint(s):
        try:
            # if it's base64 then it's an encoded fingerprint
            base64.b64decode(s + "==")

            # or it can be an hex string
            int(s, 16)

            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def __transform_fingerprint(fingerprint):

        # if it's already hex we don't need to transform it
        try:
            int(fingerprint, 16)
            return fingerprint
        except ValueError:
            pass

        # we assume that 'fingerprint' is a valid base64 string
        return base64.b64decode(fingerprint + "==")
