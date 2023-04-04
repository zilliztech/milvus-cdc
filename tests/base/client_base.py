from pymilvus import connections


class TestBase:
    def teardown_method(self, method):
        if len(connections.list_connections()) > 0:
            for conn in connections.list_connections():
                connections.disconnect(conn[0])
                