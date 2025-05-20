import time
from datetime import datetime
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient, DEFAULT_TOKEN
from pymilvus import (
    connections, list_collections,
    Collection, Partition
)
from base.checker import default_schema, list_partitions
from base.checker import (
    InsertEntitiesCollectionChecker
)
from base.client_base import TestBase

prefix = "cdc_create_task_"
client = MilvusCdcClient('http://localhost:8444')


class TestCDCCreate(TestBase):
    """ Test Milvus CDC end to end """

    def test_cdc_for_collections_create_after_cdc_task(self, upstream_host, upstream_port, downstream_host,
                                                       downstream_port):
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
                "username": "root",
                "password": "Milvus",
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
            Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")

        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))

        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port, token=DEFAULT_TOKEN)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            # get the union of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{len(intersection_set) / len(col_list) * 100:.2f}%")
            # collections in subset of downstream
            if set(col_list).issubset(set(list_collections())):
                log.info(
                    f"all collections has been synced in downstream, col_list: {col_list}, "
                    f"list_collections: {list_collections()}")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.error(
                    f"wait all collections to be synced in downstream timeout, col_list: {col_list},"
                    f"list_collections: {list_collections()}")
        assert set(col_list).issubset(set(list_collections()))

    def test_cdc_for_partitions_create_after_cdc_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        connections.connect(host=upstream_host, port=upstream_port)
        c_name = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c_infos = [
            {"name": c_name}
        ]
        # create a cdc task, not ignore partition
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
            "collection_infos": c_infos
        }
        rsp, result = client.create_task(request_data)
        assert result
        log.info(f"create task response: {rsp}")
        task_id = rsp['task_id']
        # get the cdc task
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task {task_id} response: {rsp}")
        # create collection in upstream
        col = Collection(name=c_name, schema=default_schema)
        log.info(f"create collection {c_name} in upstream")
        # create partitions in upstream
        p_name_list = []
        for i in range(10):
            p_name = "partition_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            Partition(collection=col, name=p_name)
            p_name_list.append(p_name)
            log.info(f"create partition {p_name} in upstream")
        assert c_name in list_collections()
        assert set(p_name_list).issubset(set(list_partitions(col)))
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port, token=DEFAULT_TOKEN)
        log.info(f"all collections in downstream {list_collections()}")
        t0 = time.time()
        timeout = 60
        while True and time.time() - t0 < timeout:
            if c_name in list_collections():
                log.info(f"collection {c_name} has been synced in downstream")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.error(
                    f"wait collection {c_name} to be synced in downstream timeout, list_collections: {list_collections()}")
        assert c_name in list_collections()
        timeout = 120
        t0 = time.time()
        p_list_in_downstream = list_partitions(col)
        while True and time.time() - t0 < timeout:
            p_list_in_downstream = list_partitions(col)
            intersection_set = set(p_name_list).intersection(set(p_list_in_downstream))
            log.info(f"sync progress:{len(intersection_set) / len(p_name_list) * 100:.2f}%")
            if set(p_name_list).issubset(set(p_list_in_downstream)):
                log.info(
                    f"all partition has been synced in downstream,"
                    f"p_name_list: {p_name_list}, p_list_in_downstream: {p_list_in_downstream}")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.error(
                    f"wait all partition to be synced in downstream timeout,"
                    f"p_name_list: {p_name_list}, p_list_in_downstream: {p_list_in_downstream}")
        assert set(p_name_list).issubset(set(p_list_in_downstream))

    def test_cdc_for_collection_insert_after_cdc_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        connections.connect(host=upstream_host, port=upstream_port)
        c_name = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c_infos = [
            {"name": c_name}
        ]
        # create a cdc task, not ignore partition
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
            "collection_infos": c_infos
        }
        rsp, result = client.create_task(request_data)
        assert result
        log.info(f"create task response: {rsp}")
        task_id = rsp['task_id']
        # get the cdc task
        rsp, result = client.get_task(task_id)
        assert result
        log.info(f"get task {task_id} response: {rsp}")
        # create collection in upstream
        checker = InsertEntitiesCollectionChecker(host=upstream_host, port=upstream_port, c_name=c_name)
        checker.run()
        time.sleep(60)
        checker.pause()
        # check entities in upstream
        num_entities_upstream = checker.get_num_entities()
        log.info(f"num_entities in upstream: {num_entities_upstream}")
        count_by_query_upstream = checker.get_count_by_query()
        log.info(f"count_by_query in upstream: {count_by_query_upstream}")
        count_by_query_json_path_upstream = checker.get_count_by_query(expr="json['number']>=0", output_fields=["json"])
        log.info(f"count_by_query_json_path in upstream: {count_by_query_json_path_upstream}")

        # check entities in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port, token=DEFAULT_TOKEN)
        col = Collection(name=c_name)
        col.create_index(field_name="float_vector",
                         index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        # add json path index for json field
        json_path_index_params_double = {"index_type": "INVERTED", "params": {"json_cast_type": "double",
                                                                              "json_path": "json['number']"}}
        col.create_index(field_name="json", index_params=json_path_index_params_double)
        json_path_index_params_varchar = {"index_type": "INVERTED", "params": {"json_cast_type": "VARCHAR",
                                                                               "json_path": "json['varchar']"}}
        col.create_index(field_name="json", index_params=json_path_index_params_varchar)
        json_path_index_params_bool = {"index_type": "INVERTED", "params": {"json_cast_type": "Bool",
                                                                            "json_path": "json['bool']"}}
        col.create_index(field_name="json", index_params=json_path_index_params_bool)
        json_path_index_params_not_exist = {"index_type": "INVERTED", "params": {"json_cast_type": "Double",
                                                                                 "json_path": "json['not_exist']"}}
        col.create_index(field_name="json", index_params=json_path_index_params_not_exist)
        col.load()
        # wait for the collection to be synced
        timeout = 120
        count_by_query_downstream = len(col.query(expr="int64 >= 0", output_fields=["int64"]))
        count_by_query_json_path_downstream = len(col.query(expr="json['number']>=0", output_fields=["json"]))
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            count_by_query_downstream = len(col.query(expr="int64 >= 0", output_fields=["int64"]))
            count_by_query_json_path_downstream = len(col.query(expr="json['number']>=0", output_fields=["json"]))
            # log.info(f"count_by_query_downstream: {len(count_by_query_downstream)}")
            log.info(
                f"count_by_query_downstream {count_by_query_downstream},"
                f"count_by_query_upstream {count_by_query_upstream}")
            log.info(
                f"count_by_query_json_path_downstream {count_by_query_json_path_downstream},"
                f"count_by_query_json_path_upstream {count_by_query_json_path_upstream}")
            if (count_by_query_downstream == count_by_query_upstream) and \
                    (count_by_query_json_path_downstream == count_by_query_json_path_upstream):
                log.info(f"collection {c_name} has been synced")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                raise Exception(f"Timeout waiting for collection {c_name} to be synced")
        log.info(f"count_by_query in downstream: {count_by_query_downstream}")
        log.info(f"count_by_query_json_path in downstream: {count_by_query_json_path_downstream}")
        assert count_by_query_upstream == count_by_query_downstream
        assert count_by_query_json_path_upstream == count_by_query_json_path_downstream
        # flush collection in downstream
        col.flush()
        num_entities_downstream = col.num_entities
        log.info(f"num_entities in downstream: {num_entities_downstream}")
        assert num_entities_upstream == num_entities_downstream
