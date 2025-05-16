import random

import pytest
import time
import numpy as np
from datetime import datetime
from utils.util_log import test_log as log
from utils.utils_import import MinioSyncer
from pymilvus import (
    connections, list_collections,
    Collection, Partition, db,
    utility,
)
import pandas as pd
from pymilvus.client.types import LoadState
from pymilvus.orm.role import Role
from pymilvus.bulk_writer import RemoteBulkWriter, BulkFileType
from base.checker import default_schema, list_partitions
from base.client_base import TestBase

prefix = "cdc_create_task_"


class TestCDCSyncRequest(TestBase):
    """ Test Milvus CDC end to end """

    def connect_downstream(self, host, port, token="root:Milvus"):
        connections.connect(host=host, port=port, token=token)

    def test_cdc_sync_create_collection_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: create task with default params
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        col_list = []
        for i in range(10):
            time.sleep(0.1)
            collection_name = prefix + "create_col_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
        # create collections in upstream
        for col_name in col_list:
            Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")
        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            # get the union of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{len(intersection_set) / len(col_list) * 100:.2f}%")
            # collections in subset of downstream
            if set(col_list).issubset(set(list_collections())):
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert set(col_list).issubset(set(list_collections()))

    def test_cdc_sync_drop_collection_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: drop collection in upstream
        expected: collection in downstream is dropped
        """
        connections.connect(host=upstream_host, port=upstream_port)
        col_list = []
        for i in range(10):
            time.sleep(0.1)
            collection_name = prefix + "drop_col_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
        # create collections in upstream
        for col_name in col_list:
            Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")
        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            # get the union of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{len(intersection_set) / len(col_list) * 100:.2f}%")
            # collections in subset of downstream
            if set(col_list).issubset(set(list_collections())):
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert set(col_list).issubset(set(list_collections()))
        # drop collection in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        for col_name in col_list:
            Collection(name=col_name).drop()
            log.info(f"drop collection {col_name} in upstream")
        assert set(col_list).isdisjoint(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            # get the union of col_list and list_collections
            intersection_set = set(col_list).intersection(set(list_collections()))
            log.info(f"sync progress:{(len(col_list) - len(intersection_set)) / len(col_list) * 100:.2f}%")
            # collections in subset of downstream
            if set(col_list).isdisjoint(set(list_collections())):
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert set(col_list).isdisjoint(set(list_collections())), f"collections in downstream {list_collections()}"

    def test_cdc_sync_insert_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: insert entities in upstream
        expected: entities in downstream is inserted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "insert_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 10
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
                break
            # get the number of entities in downstream
            if c_downstream.num_entities != nb:
                log.info(f"sync progress:{c_downstream.num_entities / (nb*epoch) * 100:.2f}%")
            # collections in subset of downstream
            if c_downstream.num_entities == nb*epoch:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(10)
            try:
                c_downstream.flush(timeout=5)
            except Exception as e:
                log.info(f"flush err: {str(e)}")
        assert c_downstream.num_entities == nb*epoch

    def test_cdc_sync_upsert_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is upserted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "upsert_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        # add json path index for json field
        json_path_index_params_double = {"index_type": "INVERTED", "params": {"json_cast_type": "double",
                                                                              "json_path": "json['number']"}}
        c.create_index(field_name="json", index_params=json_path_index_params_double)
        json_path_index_params_varchar = {"index_type": "INVERTED", "params": {"json_cast_type": "VARCHAR",
                                                                               "json_path": "json['varchar']"}}
        c.create_index(field_name="json", index_params=json_path_index_params_varchar)
        json_path_index_params_bool = {"index_type": "INVERTED", "params": {"json_cast_type": "Bool",
                                                                            "json_path": "json['bool']"}}
        c.create_index(field_name="json", index_params=json_path_index_params_bool)
        json_path_index_params_not_exist = {"index_type": "INVERTED", "params": {"json_cast_type": "Double",
                                                                                 "json_path": "json['not_exist']"}}
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        # upsert data in upstream
        data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                ["hello" for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb, 2*nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
        c.upsert(data)
        # check data has been upserted in upstream
        time.sleep(5)
        res = c.query('varchar == "hello"', timeout=10)
        assert len(res) == nb
        res = c.query(f"json['number'] >= {nb}", timeout=10)
        assert len(res) == nb
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
                break
            time.sleep(3)
            c_state = utility.load_state(collection_name)
            if c_state != LoadState.Loaded:
                log.info(f"collection state in downstream: {str(c_state)}")
                continue
            res = c_downstream.query('varchar == "hello"', timeout=10)
            # get the number of entities in downstream
            if len(res) != nb:
                log.info(f"sync progress:{len(res) / nb * 100:.2f}%")
            # collections in subset of downstream
            if len(res) == nb:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            res = c_downstream.query(f"json['number'] >= {nb}", timeout=10)
            # get the number of entities in downstream
            if len(res) != nb:
                log.info(f"sync progress:{len(res) / nb * 100:.2f}%")
            # collections in subset of downstream
            if len(res) == nb:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
        assert c_state == LoadState.Loaded
        assert len(res) == nb

    def test_cdc_sync_delete_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: insert entities in upstream
        expected: entities in downstream is inserted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "delete_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        # delete data in upstream
        c.delete(f"int64 in {[i for i in range(100)]}")
        time.sleep(5)
        # query data in upstream
        res = c.query(f"int64 in {[i for i in range(100)]}", timeout=10)
        assert len(res) == 0
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
                break
            time.sleep(3)
            c_state = utility.load_state(collection_name)
            if c_state != LoadState.Loaded:
                log.info(f"collection state in downstream: {str(c_state)}")
                continue
            # get the number of entities in downstream
            res = c_downstream.query(f"int64 in {[i for i in range(100)]}", timeout=10)
            if len(res) != 100:
                log.info(f"sync progress:{(100-len(res)) / 100 * 100:.2f}%")
            # collections in subset of downstream
            if len(res) == 0:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
        c_state = utility.load_state(collection_name)
        assert c_state == LoadState.Loaded
        assert len(res) == 0

    def test_cdc_sync_create_partition_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "create_par_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # create partition in upstream
        for i in range(10):
            Partition(collection=c, name=f"partition_{i}")
        # check partitions in upstream
        assert set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c)))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            if set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c_downstream))):
                log.info(f"partition synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"partition synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c_downstream)))

    def test_cdc_sync_drop_partition_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "drop_par_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # create partition in upstream
        for i in range(10):
            Partition(collection=c, name=f"partition_{i}")
        # check partitions in upstream
        assert set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c)))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            #
            if set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c_downstream))):
                log.info(f"partition synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"partition synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert set([f"partition_{i}" for i in range(10)]).issubset(set(list_partitions(c_downstream)))

        # drop partition in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        for i in range(10):
            Partition(collection=c, name=f"partition_{i}").drop()
        assert set([f"partition_{i}" for i in range(10)]).isdisjoint(set(list_partitions(c)))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            if set([f"partition_{i}" for i in range(10)]).isdisjoint(set(list_partitions(c_downstream))):
                log.info(f"partition synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            c_downstream = Collection(name=collection_name)
            if time.time() - t0 > timeout:
                log.info(f"partition synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"partitions in downstream {list_partitions(c_downstream)}")
        assert set([f"partition_{i}" for i in range(10)]).isdisjoint(set(list_partitions(c_downstream)))

    def test_cdc_sync_create_index_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "create_idx_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        assert len(c.indexes) == 1
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            if len(c_downstream.indexes) == 1:
                log.info(f"index synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"index synced in downstream failed with timeout: {time.time() - t0:.2f}s")

    def test_cdc_sync_drop_index_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """

        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "drop_idx_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        assert len(c.indexes) == 1
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            if len(c_downstream.indexes) == 1:
                log.info(f"index synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"index synced in downstream failed with timeout: {time.time() - t0:.2f}s")

        # drop index in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        c.drop_index()
        assert len(c.indexes) == 0
        # check index in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            if len(c_downstream.indexes) == 0:
                log.info(f"index synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"index synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert len(c_downstream.indexes) == 0

    def test_cdc_sync_load_and_release_collection_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "load_release_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        assert len(c.indexes) == 1
        c.load()
        res = c.get_replicas()
        assert len(res.groups) == 1
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            try:
                res = c_downstream.get_replicas()
            except Exception:
                log.error("get replicas failed")
                continue
            if len(res.groups) == 1:
                log.info(f"replicas num in downstream: {len(res.groups)}")
                log.info(f"replicas synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"replicas synced in downstream failed with timeout: {time.time() - t0:.2f}s")

        # release replicas in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        c = Collection(name=collection_name, schema=default_schema)
        c.release()
        c_state = utility.load_state(collection_name)
        assert c_state == LoadState.NotLoad, f"upstream collection state is {c_state}"
        log.info("replicas released in upstream successfully")
        # check replicas in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            c_state = utility.load_state(collection_name)
            if c_state == LoadState.NotLoad:
                log.info("replicas released in downstream successfully")
                log.info(f"replicas synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"replicas synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        assert c_state == LoadState.NotLoad, f"downstream collection state is {c_state}"

    def test_cdc_sync_flush_collection_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "flush_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        epoch = 1
        for e in range(epoch):
            data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        assert c.num_entities == 0
        # get number of entities in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        log.info(f"number of entities in downstream: {c_downstream.num_entities}")
        assert c_downstream.num_entities == 0

        # flush collection in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        c.flush()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        assert c.num_entities == nb
        # get number of entities in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        log.info(f"number of entities in downstream: {c_downstream.num_entities}")
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # get the number of entities in downstream
            if c_downstream.num_entities != nb:
                log.info(f"sync progress:{c_downstream.num_entities / nb * 100:.2f}%")
            # collections in subset of downstream
            if c_downstream.num_entities == nb:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        c_downstream.flush(timeout=10)
        log.info(f"number of entities in downstream: {c_downstream.num_entities}")

    def test_cdc_sync_create_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        # create database in upstream
        connections.connect(host=upstream_host, port=upstream_port)
        db_name = "db_create_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        db.create_database(db_name)
        log.info(f"database in upstream {db.list_database()}")
        assert db_name in db.list_database()
        # check database in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if db_name in db.list_database():
                log.info(f"database synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"database synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"database in downstream {db.list_database()}")
        assert db_name in db.list_database()

    def test_cdc_sync_drop_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        # create database in upstream
        connections.connect(host=upstream_host, port=upstream_port)
        db_name = "db_drop_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        db.create_database(db_name)
        log.info(f"database in upstream {db.list_database()}")
        assert db_name in db.list_database()
        # check database in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if db_name in db.list_database():
                log.info(f"database synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"database synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"database in downstream {db.list_database()}")
        assert db_name in db.list_database()

        # drop database in upstream
        connections.disconnect("default")
        connections.connect(host=upstream_host, port=upstream_port)
        db.drop_database(db_name)
        assert db_name not in db.list_database()
        # check database in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        while time.time() - t0 < timeout:
            if db_name not in db.list_database():
                log.info(f"database synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"database synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"database in downstream {db.list_database()}")
        assert db_name not in db.list_database()

    def test_cdc_sync_rbac_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc rbac replicate
        """

        username = "foo"
        old_password = "foo123456"
        new_password = "foo123456789"
        role_name = "birder"
        privilege = "CreateDatabase"

        # upstream operation
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        utility.create_user(username, old_password)
        utility.update_password(username, old_password, new_password)
        role = Role(role_name)
        role.create()
        role.add_user(username)
        role.grant("Global", "*", privilege)
        connections.disconnect("default")

        # downstream check
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        while time.time() - t0 < timeout:
            userinfo = utility.list_users(False)
            user_list = [user for user in userinfo.groups if user.username == username]
            if len(user_list) != 1:
                time.sleep(2)
                continue
            roleinfo = utility.list_roles(True)
            role_list = [role for role in roleinfo.groups if role.role_name == role_name]
            if len(role_list) != 1:
                time.sleep(2)
                continue
            role = Role(role_name)
            grantinfo = role.list_grant("Global", "*")
            grant_list = [grant for grant in grantinfo.groups if grant.privilege == privilege]
            if len(grant_list) != 1:
                time.sleep(2)
                continue
        assert len(user_list) == 1, user_list
        assert len(role_list) == 1, role_list
        assert username in role_list[0].users, role_list[0].users
        assert len(grant_list) == 1, grant_list
        connections.disconnect("default")

        # downstream new user connect
        timeout = 120
        t0 = time.time()
        success_update = False
        while time.time() - t0 < timeout:
            try:
                self.connect_downstream(downstream_host, downstream_port, f"{username}:{new_password}")
                success_update = True
            except Exception:
                connections.disconnect("default")
                time.sleep(2)
                continue
        assert success_update
        connections.disconnect("default")

        # upstream operation
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        role = Role(role_name)
        role.revoke("Global", "*", privilege)
        role.remove_user(username)
        role.drop()
        utility.delete_user(username)
        connections.disconnect("default")

        # downstream check
        self.connect_downstream(downstream_host, downstream_port)
        timeout = 120
        t0 = time.time()
        while time.time() - t0 < timeout:
            userinfo = utility.list_users(False)
            user_list = [user for user in userinfo.groups if user.username == username]
            if len(user_list) != 0:
                time.sleep(1)
                continue
            roleinfo = utility.list_roles(True)
            role_list = [role for role in roleinfo.groups if role.role_name == role_name]
            if len(role_list) != 0:
                time.sleep(1)
                continue
        assert len(user_list) == 0, user_list
        assert len(role_list) == 0, role_list
        connections.disconnect("default")

    @pytest.mark.skip(reason="TODO: support import in milvus2.5")
    @pytest.mark.parametrize("file_type", ["json", "parquet"])
    def test_cdc_sync_import_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port, file_type,
        upstream_minio_endpoint, downstream_minio_endpoint, upstream_minio_bucket_name, downstream_minio_bucket_name):
        """
        target: test cdc default
        method: import entities in upstream
        expected: entities in downstream is inserted
        """
        connections.connect(host=upstream_host, port=upstream_port)
        collection_name = prefix + "import_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        c = Collection(name=collection_name, schema=default_schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        data = pd.DataFrame(
            {
                "int64": [i for i in range(nb)],
                "float": [np.float32(i) for i in range(nb)],
                "varchar": [str(i) for i in range(nb)],
                "json": [{"number": i, "varchar": str(i), "bool": bool(i)} for i in range(nb)],
                "float_vector": [[random.random() for _ in range(128)] for _ in range(nb)]
            }
        )
        with RemoteBulkWriter(
            schema=default_schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=upstream_minio_bucket_name,
                endpoint=upstream_minio_endpoint,
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=BulkFileType.JSON if file_type == "json" else BulkFileType.PARQUET,
        ) as remote_writer:
            for _, row in data.iterrows():
                remote_writer.append_row(row.to_dict())
            remote_writer.commit()
            files = remote_writer.batch_files
            print(files)
        # sync data from upstream to downstream
        syncer = MinioSyncer(
            src_endpoint=upstream_minio_endpoint,
            src_access_key="minioadmin",
            src_secret_key="minioadmin",
            dst_endpoint=downstream_minio_endpoint,
            dst_access_key="minioadmin",
            dst_secret_key="minioadmin",
            secure=False
        )
        # Sync only the specific files generated by RemoteBulkWriter
        all_files = [file for batch in files for file in batch]  # Flatten the list of files
        success, errors, skipped = syncer.sync_files(
            src_bucket=upstream_minio_bucket_name,
            dst_bucket=downstream_minio_bucket_name,
            files=all_files
        )

        print(f"Sync completed: {success} files synced successfully, {skipped} files skipped, {errors} errors encountered")

        # import data using bulk insert
        for f in files:
            t0 = time.time()
            task_id = utility.do_bulk_insert(collection_name, files=f)
            log.info(f"bulk insert task ids: {task_id}")
            states = utility.get_bulk_insert_state(task_id=task_id)
            while states.state != utility.BulkInsertState.ImportCompleted:
                time.sleep(5)
                states = utility.get_bulk_insert_state(task_id=task_id)
            tt = time.time() - t0
            log.info(f"bulk insert state: {states} in {tt} with states: {states}")
            assert states.state == utility.BulkInsertState.ImportCompleted

        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 360
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while True and time.time() - t0 < timeout:
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
                break
            # get the number of entities in downstream
            if c_downstream.num_entities != nb:
                log.info(f"sync progress:{c_downstream.num_entities / (nb) * 100:.2f}%")
            # collections in subset of downstream
            if c_downstream.num_entities == nb:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            try:
                c_downstream.flush(timeout=5)
            except Exception as e:
                log.info(f"flush err: {str(e)}")
        assert c_downstream.num_entities == nb
