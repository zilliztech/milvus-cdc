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
from base.checker import default_schema
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


prefix = "cdc_get_"
client = MilvusCdcClient('http://localhost:8444')


class TestCDCCreate(object):
    """ Test Milvus CDC end to end """
    
    def test_cdc_get_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: create task with default params
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        col_list = []
        for i in range(10):
            time.sleep(0.1)
            collection_name = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
        log.info(f"col_list be created: {col_list}")
        c_infos = [
            {"name": col_name} for col_name in col_list
        ]
        request_data = {
            "milvus_connect_param": {
                "host": downstream_host,
                "port": int(downstream_port),
                "username": "",
                "password": "",
                "enable_tls": False,
                "ignore_partition": True,
                "connect_timeout": 10
            },
            "collection_infos": c_infos
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
        # create collections in upstream
        for col_name in col_list:
            col = Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")


        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 120
        t0= time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time()-t0 < timeout:
            # get the union of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{len(intersection_set)/len(col_list)*100:.2f}%")
            # collections in subset of downstream
            if set(col_list).issubset(set(list_collections())):
                log.info(f"all collections has been synced in downstream, col_list: {col_list}, list_collections: {list_collections()}")
                break
            time.sleep(1)
            if time.time()-t0 > timeout:
                log.error(f"wait all collections to be synced in downstream timeout, col_list: {col_list}, list_collections: {list_collections()}")
        assert set(col_list).issubset(set(list_collections()))

    def test_cdc_for_collections_create_before_cdc_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        """
        connections.connect(host=upstream_host, port=upstream_port)
        checker = CreateCollectionChecker(host=upstream_host, port=upstream_port)
        checker.run()
        time.sleep(10)
        checker.pause()
        col_list = checker.collection_name_list
        log.info(f"col_list be created: {col_list}")
        c_infos = [
            {"name": col_name} for col_name in col_list
        ]
        request_data = {
            "milvus_connect_param": {
                "host": downstream_host,
                "port": int(downstream_port),
                "username": "",
                "password": "",
                "enable_tls": False,
                "ignore_partition": True,
                "connect_timeout": 10
            },
            "collection_infos": c_infos
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

        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
        t0= time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time()-t0 < timeout:
            # get the intersection of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{len(intersection_set)/len(col_list)*100:.2f}%")
            # collections in subset of downstream
            if set(col_list).issubset(set(list_collections())):
                log.info(f"all collections has been synced in downstream, col_list: {col_list}, list_collections: {list_collections()}")
                break
            time.sleep(1)
            if time.time()-t0 > timeout:
                log.error(f"wait all collections to be synced in downstream timeout, col_list: {col_list}, list_collections: {list_collections()}")
        assert set(col_list).issubset(set(list_collections()))

        


