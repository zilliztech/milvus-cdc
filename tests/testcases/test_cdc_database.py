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
from pymilvus.orm.role import Role
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

    def connect_downstream(self, host, port, token="root:Milvus"):
        connections.connect(host=host, port=port, token=token)

    def test_cdc_sync_default_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc without not match database
        method: create collection/insert/load/flush in upstream
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        col_list = []
        for i in range(5):
            time.sleep(0.1)
            collection_name = prefix + "not_match_database_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
        # create collections in upstream
        for col_name in col_list:
            c = Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")
            # insert data to upstream
            nb = 300
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
        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        idx = 0
        log.info(f"all collections in downstream {list_collections()}")
        while idx < 10:
            downstream_col_list = list_collections()
            if len(downstream_col_list) != 0:
                log.info(f"all collections in downstream {downstream_col_list}")
            time.sleep(3)
            idx += 1
        assert len(list_collections()) == 0, f"collections in downstream {list_collections()}"

    def test_cdc_sync_not_match_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc without not match database
        method: create collection/insert/load/flush in upstream
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        db.create_database("hoo")
        db.using_database(db_name="hoo")
        col_list = []
        for i in range(5):
            time.sleep(0.1)
            collection_name = prefix + "not_match_database_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
        # create collections in upstream
        for col_name in col_list:
            c = Collection(name=col_name, schema=default_schema)
            log.info(f"create collection {col_name} in upstream")
            # insert data to upstream
            nb = 300
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
        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)
        idx = 0
        db_list = db.list_database()
        log.info(f"all collections in downstream {db_list}")
        while idx < 10:
            db_list = db.list_database()
            if len(db_list) != 1:
                log.info(f"all collections in downstream {db_list}")
            time.sleep(3)
            idx += 1
        assert len(db_list) == 1, f"collections in downstream {db.list_database()}"

    def test_cdc_sync_match_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc with match database
        method: create collection/insert/load/flush in upstream
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        db.create_database("foo")
        db.using_database(db_name="foo")
        col_list = []
        col_name = prefix + "match_database_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        # create collections in upstream
        c = Collection(name=col_name, schema=default_schema)
        log.info(f"create collection {col_name} in upstream")
        # insert data to upstream
        nb = 300
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
        # check collections in upstream
        assert set(col_list).issubset(set(list_collections()))
        # check collections in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port)

        db_list = db.list_database()
        log.info(f"all collections in downstream {db_list}")
        timeout = 20
        t0 = time.time()
        db_name = "foo"

        while time.time() - t0 < timeout:
            if db_name in db.list_database():
                log.info(f"database synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(2)
            if time.time() - t0 > timeout:
                log.info(f"database synced in downstream failed with timeout: {time.time() - t0:.2f}s")
        log.info(f"database in downstream {db.list_database()}")
        assert db_name in db.list_database()

        db.using_database(db_name=db_name)
        c_downstream = Collection(name=col_name)
        timeout = 60
        t0 = time.time()
        log.info(f"all collections in downstream {list_collections()}")
        while time.time() - t0 < timeout:
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
