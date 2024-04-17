from pymilvus import connections
from api.milvus_cdc import MilvusCdcClient

# client = MilvusCdcClient('http://localhost:8444')


class TestBase:

    def setup_method(self, method):
        pass
        # res, result = client.list_tasks()
        # # delete the tasks
        # for task in res["tasks"]:
        #     task_id = task["task_id"]
        #     rsp, result = client.delete_task(task_id)
        #     assert result
        #     assert rsp == {}

    def teardown_method(self, method):
        if len(connections.list_connections()) > 0:
            for conn in connections.list_connections():
                connections.disconnect(conn[0])
