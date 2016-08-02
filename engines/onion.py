from onion_py.manager import Manager
from onion_py.caching import OnionSimpleCache


class OnionGetter:

    type = "details"
    bridges = list()
    relays = list()
    manager = None
    nodes = {'relays': list(), 'bridges': list()}

    def __init__(self):
        self.manager = Manager(OnionSimpleCache())

    def __retrieveNodes(self):
        if self.manager is None:
            raise Exception("Please initialize the class")

        the_noodes = self.manager.query(self.type)
        for relay in the_noodes.relays:
            self.nodes['relays'].append(relay.__dict__)

        for bridge in the_noodes.bridges:
            self.nodes['bridges'].append(bridge.__dict__)

    def get_nodes(self):
        self.__retrieveNodes()

        return self.nodes

    def get_relays(self):
        self.__retrieveNodes()

        return self.nodes['relays']

    def get_bridges(self):
        self.__retrieveNodes()

        return self.nodes['bridges']