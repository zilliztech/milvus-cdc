import pytest
import time
from datetime import datetime
from pymilvus import connections
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient
from pymilvus import (
    connections, list_collections, has_partition,
    FieldSchema, CollectionSchema, DataType,
    Collection, Partition
)
from base.checker import (
    CreateCollectionChecker,
    DropCollectionChecker,
    CreatePartitionChecker,
    DropPartitionChecker,
    InsertEntitiesCollectionChecker,
    InsertEntitiesPartitionChecker,
    DeleteEntitiesCollectionChecker,
    DeleteEntitiesPartitionChecker
)


prefix = "cdc_e2e_"
client = MilvusCdcClient('http://localhost:8444')


class TestE2E(object):
    """ Test Milvus CDC end to end """
    def test_cdc_collection(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: create task with default params
        expected: create successfully
        """
        collection_name = prefix + datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        request_data = {
            "milvus_connect_param": {
                "host": downstream_host,
                "port": int(downstream_port),
                "username": "",
                "password": "",
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
        # get the cdc task
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task {task_id} response: {rsp}")
        # check create collection and  insert entities to collection
        connections.connect(host=upstream_host, port=upstream_port)
        checker = InsertEntitiesCollectionChecker(host=upstream_host, port=upstream_port, c_name=collection_name)
        checker.run()
        time.sleep(120)
        all_collections = list_collections()
        # pause the insert task
        log.info(f"start to pause the insert task")
        checker.pause()
        log.info(f"pause the insert task successfully")
        # check the collection in upstream
        num_entities_upstream =  checker.get_num_entities()
        log.info(f"num_entities_upstream: {num_entities_upstream}")
        count_by_query_upstream = checker.get_count_by_query()
        log.info(f"count_by_query_upstream: {count_by_query_upstream}")
        # check the collection in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port)
        all_collections = list_collections()
        collection = Collection(name=collection_name)
        collection.create_index(field_name="float_vector", index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        collection.load()
        # wait for the collection to be synced
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(collection.query(expr=checker.query_expr, output_fields=checker.output_fields))
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
        assert num_entities_upstream == num_entities_downstream, f"num_entities_upstream {num_entities_upstream} != num_entities_downstream {num_entities_downstream}"

        # delete the entities in upstream
        connections.disconnect("default")
        log.info(f"start to connect to upstream {upstream_host} {upstream_port}")
        connections.connect(host=upstream_host, port=upstream_port)
        log.info(f"start to delete the entities in upstream")
        delete_expr = f"int64 in {[i for i in range(0, 3000)]}"
        checker.collection.delete(delete_expr)
        while True and time.time() - t0 < timeout:
            res = checker.collection.query(expr=delete_expr, output_fields=checker.output_fields)
            if len(res) == 0:
                break
            else:
                log.info(f"res: {len(res)}")
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for delete entities in upstream")
        log.info(f"res: {res}")
        count_by_query_upstream = len(res)
        assert count_by_query_upstream == 0
        log.info(f"delete the entities in upstream successfully")
        # check the collection in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port)
        collection = Collection(name=collection_name)
        collection.load()
        # wait for the collection to be synced
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(collection.query(expr=delete_expr, output_fields=checker.output_fields))
            if count_by_query_downstream == count_by_query_upstream:
                log.info(f"cost time: {time.time() - t0} to sync the delete entities")
                break
            else:
                log.info(f"count_by_query_downstream: {count_by_query_downstream}")
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"count_by_query_downstream: {count_by_query_downstream}")
        assert count_by_query_upstream == count_by_query_downstream

        # drop the collection in upstream
        connections.disconnect("default")
        log.info(f"start to connect to upstream {upstream_host} {upstream_port}")
        connections.connect(host=upstream_host, port=upstream_port)
        log.info(f"start to drop the collection in upstream")
        checker.collection.drop()
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if collection_name not in list_collections():
                break
            time.sleep(1)
            log.info(f"collection: {collection_name} still exists")
            if time.time() - t0 > timeout:
                log.error(f"Timeout waiting for collection {collection_name} to be dropped")
        log.info(f"drop the collection in upstream successfully")
        # check the collection in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port)
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            log.info(f"all collections in downstream: {list_collections()}")
            if collection_name not in list_collections():
                log.info(f"cost time: {time.time() - t0} to drop the collection")
                break
            time.sleep(1)
            log.info(f"collection: {collection_name} still exists")
            if time.time() - t0 > timeout:
                log.error(f"Timeout waiting for collection {collection_name} to be dropped")
        assert collection_name not in list_collections()


    def test_cdc_partition(self, upstream_host, upstream_port, downstream_host, downstream_port):
        collection_name = prefix + datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        request_data = {
            "milvus_connect_param": {
                "host": downstream_host,
                "port": int(downstream_port),
                "username": "",
                "password": "",
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
        # get the cdc task
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task {task_id} response: {rsp}")            
        # check create partition and insert entities to partition
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        p_name = "p1"
        checker = InsertEntitiesPartitionChecker(host=upstream_host, port=upstream_port, c_name=collection_name, p_name=p_name)
        checker.run()
        time.sleep(120)
        # pause the insert task
        log.info(f"start to pause the insert task")
        checker.pause()
        log.info(f"pause the insert task successfully")
        # check the collection in upstream
        count_by_query_upstream = checker.get_count_by_query(p_name=p_name)
        log.info(f"count_by_query_upstream: {count_by_query_upstream}")
        num_entities_upstream =  checker.get_num_entities(p_name=p_name)
        log.info(f"num_entities_upstream: {num_entities_upstream}")

        # check the collection in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        collection = Collection(name=collection_name)
        # create index and load collection
        collection.create_index(field_name="float_vector", index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 1024}})
        collection.load()
        # wait for the collection to be synced
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(collection.query(expr=checker.query_expr, output_fields=checker.output_fields, partition_names=[p_name]))
            if count_by_query_downstream == count_by_query_upstream:
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"count_by_query_downstream: {count_by_query_downstream}")
        assert count_by_query_upstream == count_by_query_downstream
        # wait for the collection to be flushed
        p = Partition(collection, p_name)
        p.flush()
        num_entities_downstream = p.num_entities
        assert num_entities_upstream == num_entities_downstream, f"num_entities_upstream {num_entities_upstream} != num_entities_downstream {num_entities_downstream}"

        # delete the entities of partition in upstream
        connections.disconnect("default")
        log.info(f"start to connect to upstream {upstream_host} {upstream_port}")
        connections.connect(host=upstream_host, port=upstream_port)
        log.info(f"start to delete the entities in upstream")
        delete_expr = f"int64 in {[i for i in range(0, 3000)]}"
        checker.collection.delete(delete_expr, partition_name=p_name)
        while True and time.time() - t0 < timeout:
            res = checker.collection.query(expr=delete_expr, output_fields=checker.output_fields, partition_names=[p_name])
            if len(res) == 0:
                break
            else:
                log.info(f"res: {len(res)}")
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for delete entities in upstream")
        log.info(f"res: {res}")
        count_by_query_upstream = len(res)
        assert count_by_query_upstream == 0
        log.info(f"delete the entities in upstream successfully")
        # check the collection in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port)
        collection = Collection(name=collection_name)
        collection.load()
        # wait for the collection to be synced
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(collection.query(expr=delete_expr, output_fields=checker.output_fields, partition_names=[p_name]))
            if count_by_query_downstream == count_by_query_upstream:
                log.info(f"cost time: {time.time() - t0} to sync the delete entities")
                break
            else:
                log.info(f"count_by_query_downstream: {count_by_query_downstream}")
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for collection {collection_name} to be synced")
        log.info(f"count_by_query_downstream: {count_by_query_downstream}")
        assert count_by_query_upstream == count_by_query_downstream

        # drop the partition in upstream
        connections.disconnect("default")
        log.info(f"start to connect to upstream {upstream_host} {upstream_port}")
        connections.connect(host=upstream_host, port=upstream_port)
        checker.collection.release()
        log.info(f"start to drop the partition in upstream")
        p = Partition(checker.collection, p_name)
        p.drop()
        # check the partition is dropped in upstream
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            p_list = [p.name for p in checker.collection.partitions]
            if p_name not in p_list:
                break
            time.sleep(1)
            log.info(f"partition: {p_name} still exists")
            if time.time() - t0 > timeout:
                log.error(f"Timeout waiting for partition {p_name} to be dropped")
        log.info(f"drop the partition in upstream successfully")
        # check the partition in downstream
        connections.disconnect("default")
        log.info(f"start to connect to downstream {downstream_host} {downstream_port}")
        connections.connect(host=downstream_host, port=downstream_port)
        collection = Collection(name=collection_name)
        collection.release()
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            p_list = [p.name for p in collection.partitions]
            if p_name not in p_list:
                log.info(f"cost time: {time.time() - t0} to drop the collection")
                break
            time.sleep(1)
            log.info(f"partition: {p_name} still exists")
            if time.time() - t0 > timeout:
                log.error(f"Timeout waiting for partition {p_name} to be dropped")
        assert p_name not in p_list

