import random

import pytest
import time
import numpy as np
from datetime import datetime
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient
from pymilvus import (
    connections, list_collections,
    Collection, Partition, db,
    utility,
)
from pymilvus.client.types import LoadState
from base.checker import default_schema, list_partitions
from base.checker import (
    InsertEntitiesPartitionChecker,
    InsertEntitiesCollectionChecker
)
from base.client_base import TestBase

prefix = "cdc_create_task_"
# client = MilvusCdcClient('http://localhost:8444')


class TestCDCSyncRequest(TestBase):
    """ Test Milvus CDC end to end """

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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
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
        assert set(col_list).isdisjoint(set(list_collections()))

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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
            time.sleep(1)
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
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        # upsert data in upstream
        data = [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                ["hello" for i in range(nb)],
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
        c.upsert(data)
        # check data has been upserted in upstream
        time.sleep(5)
        res = c.query('varchar == "hello"', timeout=10)
        assert len(res) == nb
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if time.time() - t0 > timeout:
                log.info(f"collection synced in downstream failed with timeout: {time.time() - t0:.2f}s")
                break
            time.sleep(3)
            c_state = utility.load_state(collection_name)
            if c_state != LoadState.Loaded:
                log.info(f"collection state in downstream: {str(c_state)}")
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        assert len(c.indexes) == 1
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        c.flush()
        index_params = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        c.create_index("float_vector", index_params)
        assert len(c.indexes) == 1
        # check collections in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
        replicas = None
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            try:
                res = c_downstream.get_replicas()
            except Exception as e:
                replicas = 0
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
        log.info(f"replicas released in upstream successfully")
        # check replicas in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        timeout = 60
        replicas = None
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            # collections in subset of downstream
            c_state = utility.load_state(collection_name)
            if c_state == LoadState.NotLoad:
                log.info(f"replicas released in downstream successfully")
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
                [[random.random() for _ in range(128)] for _ in range(nb)]
            ]
            c.insert(data)
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        assert c.num_entities == 0
        # get number of entities in downstream
        connections.disconnect("default")
        connections.connect(host=downstream_host, port=downstream_port)
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
        connections.connect(host=downstream_host, port=downstream_port)
        c_downstream = Collection(name=collection_name)
        log.info(f"number of entities in downstream: {c_downstream.num_entities}")
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
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
        connections.connect(host=downstream_host, port=downstream_port)
        timeout = 60
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            if db_name not in db.list_database():
                log.info(f"database synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"database synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"database in downstream {db.list_database()}")
        assert db_name not in db.list_database()
