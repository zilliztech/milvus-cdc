import time
from datetime import datetime
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient, DEFAULT_TOKEN
from pymilvus import (
    connections,
    Collection
)
from base.checker import (
    InsertEntitiesCollectionChecker,
)
from base.client_base import TestBase

prefix = "cdc_resume_task_"
client = MilvusCdcClient('http://localhost:8444')


class TestCdcResume(TestBase):
    """ Test Milvus CDC delete """

    def test_cdc_resume_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        collection_name1 = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '_1'
        collection_name2 = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + "_2"
        task_id1 = self.create_cdc_task(upstream_host, upstream_port, downstream_host, downstream_port, collection_name1)
        task_id2 = self.create_cdc_task(upstream_host, upstream_port, downstream_host, downstream_port, collection_name2)
        self.resume_task_with_collection_name(upstream_host, upstream_port, downstream_host, downstream_port, collection_name1, task_id1)
        self.resume_task_with_collection_name(upstream_host, upstream_port, downstream_host, downstream_port, collection_name2, task_id2)

    def create_cdc_task(self, upstream_host, upstream_port, downstream_host, downstream_port, collection_name):
        # create cdc task
        request_data = {
            "milvus_connect_param": {
                "host": downstream_host,
                "port": int(downstream_port),
                "username": "root",
                "password": "Milvus",
                "enable_tls": False,
                "ignore_partition": False,
                "connect_timeout": 10
            },
            "collection_infos": [
                {
                    "name": collection_name
                }
            ]
        }
        # create a cdc task
        rsp, result = client.create_task(request_data)
        assert result
        log.info(f"create task response: {rsp}")
        task_id = rsp['task_id']
        return task_id

    def resume_task_with_collection_name(self, upstream_host, upstream_port, downstream_host, downstream_port, collection_name, task_id):
        """
        target: test cdc delete task
        method: create task, delete task
        expected: create successfully, delete successfully
        """
        # create collection and insert entities into it in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        checker = InsertEntitiesCollectionChecker(host=upstream_host, port=upstream_port, c_name=collection_name)
        checker.run()
        time.sleep(20)
        # pause the insert task
        log.info("start to pause the insert task")
        checker.pause()
        log.info("pause the insert task successfully")
        # check the collection in upstream
        num_entities_upstream = checker.get_num_entities()
        log.info(f"collection name: {collection_name}, num_entities_upstream: {num_entities_upstream}")
        count_by_query_upstream = checker.get_count_by_query()
        log.info(f"collection name: {collection_name}, count_by_query_upstream: {count_by_query_upstream}")

        # check the collection in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port, token=DEFAULT_TOKEN)
        collection = Collection(name=collection_name)
        collection.create_index(field_name="float_vector",
                                index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        collection.load()
        # wait for the collection to be synced
        timeout = 60
        count_by_query_downstream = len(
            collection.query(expr=checker.query_expr, output_fields=checker.output_fields))
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(
                collection.query(expr=checker.query_expr, output_fields=checker.output_fields))
            if count_by_query_downstream == count_by_query_upstream:
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"count_by_query_downstream: {count_by_query_downstream}")
        assert count_by_query_upstream == count_by_query_downstream
        # wait for the collection to be flushed
        time.sleep(20)
        collection.flush()
        num_entities_downstream = collection.num_entities
        log.info(f"num_entities_downstream: {num_entities_downstream}")
        assert num_entities_upstream == num_entities_downstream,\
            f"num_entities_upstream {num_entities_upstream} != num_entities_downstream {num_entities_downstream}"

        # pause cdc task
        rsp, result = client.pause_task(task_id)
        assert result
        log.info(f"pause task response: {rsp}")

        # check task id is deleted
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task response: {rsp}")
        rsp, result = client.list_tasks()
        assert result
        log.info(f"list tasks response: {rsp}")
        task_ids = [t["task_id"] for t in rsp['tasks']]
        assert task_id in task_ids

        # connect to upstream
        connections.disconnect("default")
        log.info(f"start to connect to upstream {upstream_host} {upstream_port}")
        connections.connect(host=upstream_host, port=upstream_port)
        # insert entities into the collection
        checker.resume()
        time.sleep(20)
        checker.pause()
        # check the collection in upstream
        count_by_query_upstream_second = checker.get_count_by_query()
        log.info(f"count_by_query_upstream_second: {count_by_query_upstream_second}")
        assert count_by_query_upstream_second > count_by_query_upstream
        num_entities_upstream_second = checker.get_num_entities()
        log.info(f"num_entities_upstream_second: {num_entities_upstream_second}")
        assert num_entities_upstream_second > num_entities_upstream
        timeout = 30
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_upstream_second = checker.get_count_by_query()
            num_entities_upstream_second = checker.get_num_entities()
            if count_by_query_upstream_second == num_entities_upstream_second:
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"upstream query: {count_by_query_upstream_second}, upstream num entity: {num_entities_upstream_second}")
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"after pause insert task, upstream query: {count_by_query_upstream_second}, upstream num entity: {num_entities_upstream_second}")

        # connect to downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port, token=DEFAULT_TOKEN)
        # check the collection in downstream has not been synced
        timeout = 30
        collection = Collection(name=collection_name)
        count_by_query_downstream_second = len(
            collection.query(expr=checker.query_expr, output_fields=checker.output_fields, consistency_level="Eventually"))
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream_second = len(
                collection.query(expr=checker.query_expr, output_fields=checker.output_fields, consistency_level="Eventually"))
            if count_by_query_downstream_second == count_by_query_upstream_second:
                assert False
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"downstream: {count_by_query_downstream_second}")
        log.info(f"after pause cdc task, downstream: {count_by_query_downstream_second}, upstream: {count_by_query_upstream_second}")
        assert count_by_query_downstream_second == count_by_query_downstream

        # resume cdc task
        rsp, result = client.resume_task(task_id)
        assert result
        log.info(f"resume task response: {rsp}")
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task response: {rsp}")
        assert rsp["state"] == "Running"
        timeout = 60
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream_second = len(
                collection.query(expr=checker.query_expr, output_fields=checker.output_fields))
            if count_by_query_downstream_second == count_by_query_upstream_second:
                break
            else:
                log.info(f"downstream: {count_by_query_downstream_second}, upstream: {count_by_query_upstream_second}")
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"downstream: {count_by_query_downstream_second}, upstream: {count_by_query_upstream_second}")
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"after resume cdc task, downstream: {count_by_query_downstream_second}, upstream: {count_by_query_upstream_second}")
        assert count_by_query_downstream_second == count_by_query_upstream_second
