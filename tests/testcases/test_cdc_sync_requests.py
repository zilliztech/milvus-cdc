import random
import string
import faker
import pytest
import time
import numpy as np
from datetime import datetime
from utils.util_log import test_log as log
from utils.utils_import import MinioSyncer
from pymilvus import (
    connections, list_collections,
    Collection, Partition, db, CollectionSchema, FieldSchema, DataType, Function, FunctionType,
    utility,
)
import pandas as pd
from pymilvus.client.types import LoadState
from pymilvus import MilvusClient
from pymilvus.bulk_writer import RemoteBulkWriter, BulkFileType
from base.checker import default_schema, list_partitions
from base.client_base import TestBase

fake = faker.Faker()
prefix = "cdc_create_task_"


def clean_rbac(client, users=None, roles=None):
    """
    Clean up RBAC configuration

    Args:
        client: MilvusClient instance
        users: List of users to clean up
        roles: List of roles to clean up
        privilege_groups: List of privilege groups to clean up
    """
    if roles:
        for role in roles:
            try:
                privileges = client.describe_role(role_name=role)["privileges"]
                for privilege in privileges:
                    client.revoke_privilege_v2(
                        role_name=role,
                        privilege=privilege["privilege"],
                        collection_name='*',
                        db_name='*'
                    )
                client.drop_role(role)
            except Exception as e:
                log.error(f"Failed to clean up role {role}: {str(e)}")

    if users:
        for user in users:
            try:
                client.drop_user(user_name=user)
            except Exception as e:
                log.error(f"Failed to clean up user {user}: {str(e)}")


def gen_unique_str(prefix="", length=8):
    """
    Generate a unique string containing random characters and numbers

    Args:
        prefix: String prefix
        length: Length of the random part, default is 8

    Returns:
        A unique string containing random characters and numbers
    """
    import random

    # Available character set: uppercase and lowercase letters and digits
    chars = string.ascii_letters + string.digits

    # Generate a random string of specified length
    random_str = ''.join(random.choice(chars) for _ in range(length))

    return f"{prefix}_{random_str}"



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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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
                c_downstream.flush(timeout=60)
            except Exception as e:
                log.info(f"flush err: {str(e)}")
        assert c_downstream.num_entities == nb*epoch

    def test_cdc_sync_upsert_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is upserted
        """
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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

        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        c.flush(timeout=60)
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        c.flush(timeout=60)
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
        c_downstream.flush(timeout=60)
        log.info(f"number of entities in downstream: {c_downstream.num_entities}")

    def test_cdc_sync_create_database_request(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: upsert entities in upstream
        expected: entities in downstream is deleted
        """
        # create database in upstream
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        db_name = "db_create_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        db.create_database(db_name)
        log.info(f"database in upstream {db.list_database()}")
        assert db_name in db.list_database()
        # check database in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        db.drop_database(db_name)
        assert db_name not in db.list_database()
        # check database in downstream
        connections.disconnect("default")
        self.connect_downstream(downstream_host, downstream_port, token="root:Milvus")
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

        # Create upstream MilvusClient
        upstream_client = MilvusClient(uri=f"http://{upstream_host}:{upstream_port}", token="root:Milvus")

        # Create downstream MilvusClient
        downstream_client = MilvusClient(uri=f"http://{downstream_host}:{downstream_port}", token="root:Milvus")

        # Generate unique username, role name and privilege group name
        user_name = gen_unique_str("user_")
        password = "P@ssw0rd"
        role_name = gen_unique_str("role_")

        # Upstream operation - Create RBAC environment
        log.info(f"Creating upstream RBAC environment: user={user_name}, role={role_name}")
        upstream_client.create_user(user_name=user_name, password=password)
        upstream_client.create_role(role_name=role_name)
        upstream_client.grant_privilege_v2(
            role_name=role_name,
            privilege="ClusterReadOnly",
            collection_name='*',
            db_name='*',
        )
        upstream_client.grant_role(user_name=user_name, role_name=role_name)

        # Check if upstream RBAC was created successfully
        upstream_users = upstream_client.list_users()
        upstream_roles = upstream_client.list_roles()

        assert user_name in upstream_users, f"Failed to create upstream user {user_name}"
        assert role_name in upstream_roles, f"Failed to create upstream role {role_name}"
        upstream_role_res = upstream_client.describe_role(role_name=role_name)
        log.info(f"Role info in upstream: {upstream_role_res}")
        # Downstream check - Wait for RBAC synchronization
        log.info("Waiting for RBAC configuration to sync to downstream")
        timeout = 120
        t0 = time.time()
        sync_completed = False

        while time.time() - t0 < timeout and not sync_completed:
            # Check if users, roles and privilege groups are synchronized
            downstream_users = downstream_client.list_users()
            downstream_roles = downstream_client.list_roles()

            if user_name in downstream_users and role_name in downstream_roles:
                downstream_role_res = downstream_client.describe_role(role_name=role_name)
                log.info(f"Role info in downstream: {downstream_role_res}")
                # Check role privileges
                role_privileges = downstream_role_res["privileges"]
                log.info(f"Role privileges: {role_privileges}")
                has_privilege = False
                for priv in role_privileges:
                    if priv["privilege"] == "ClusterReadOnly":
                        has_privilege = True
                        break

                if has_privilege and downstream_role_res == upstream_role_res:
                    sync_completed = True
                    break

            log.info(f"RBAC sync progress: user={user_name in downstream_users}, role={role_name in downstream_roles}")
            time.sleep(2)

        assert sync_completed, "RBAC configuration failed to sync to downstream within timeout"
        log.info("RBAC configuration successfully synced to downstream")

        # Test downstream user login
        log.info(f"Testing downstream user {user_name} login")
        try:
            MilvusClient(
                uri=f"http://{downstream_host}:{downstream_port}",
                token=f"{user_name}:{password}"
            )
            user_login_success = True
        except Exception as e:
            log.error(f"Downstream user login failed: {str(e)}")
            user_login_success = False

        assert user_login_success, "Synchronized downstream user cannot login"
        log.info("Downstream user login successful")

        # Clean up upstream RBAC configuration
        log.info("Cleaning up upstream RBAC configuration")
        clean_rbac(upstream_client, [user_name], [role_name])

        # Check if downstream RBAC is also cleaned up
        log.info("Waiting for downstream RBAC configuration cleanup synchronization")
        timeout = 120
        t0 = time.time()
        cleanup_completed = False

        while time.time() - t0 < timeout and not cleanup_completed:
            # Check if users, roles and privilege groups are cleaned up
            downstream_users = downstream_client.list_users()
            downstream_roles = downstream_client.list_roles()

            if user_name not in downstream_users and role_name not in downstream_roles:
                cleanup_completed = True
                break

            log.info(f"RBAC cleanup sync progress: user={user_name not in downstream_users}, role={role_name not in downstream_roles}")
            time.sleep(2)

        assert cleanup_completed, "RBAC cleanup configuration failed to sync to downstream within timeout"
        log.info("RBAC cleanup configuration successfully synced to downstream")


    @pytest.mark.skip(reason="TODO: support import in milvus2.5")
    @pytest.mark.parametrize("file_type", ["json", "parquet"])
    def test_cdc_sync_import_entities_request(self, upstream_host, upstream_port, downstream_host, downstream_port, file_type,
        upstream_minio_endpoint, downstream_minio_endpoint, upstream_minio_bucket_name, downstream_minio_bucket_name):
        """
        target: test cdc default
        method: import entities in upstream
        expected: entities in downstream is inserted
        """
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
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

        c.flush(timeout=60)
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
                c_downstream.flush(timeout=60)
            except Exception as e:
                log.info(f"flush err: {str(e)}")
        assert c_downstream.num_entities == nb

    
    @pytest.mark.parametrize("nullable", [True])
    def test_cdc_sync_insert_with_full_datatype_request(self, upstream_host, upstream_port, downstream_host, downstream_port, nullable):
        """
        target: test cdc sync insert with full datatype
        method: insert entities in upstream
        expected: entities in downstream is inserted
        """
        connections.connect(host=upstream_host, port=upstream_port, token="root:Milvus")
        collection_name = prefix + "insert_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True, description="id"),
            FieldSchema(name="int64", dtype=DataType.INT64, nullable=nullable, description="int64"),
            FieldSchema(name="float", dtype=DataType.FLOAT, nullable=nullable, description="float"),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=1024, nullable=nullable, enable_match=True, enable_analyzer=True, description="varchar"),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=1024, enable_match=True, enable_analyzer=True, description="text"),
            FieldSchema(name="json", dtype=DataType.JSON, nullable=nullable, description="json"),
            FieldSchema(name="bool", dtype=DataType.BOOL, nullable=nullable, description="bool"),
            FieldSchema(name="array", dtype=DataType.ARRAY, element_type=DataType.INT64, nullable=nullable, max_capacity=10, description="array"),
            FieldSchema(name="float_embedding", dtype=DataType.FLOAT_VECTOR, dim=128, description="float_embedding"),
            FieldSchema(name="binary_embedding", dtype=DataType.BINARY_VECTOR, dim=128, description="binary_embedding"),
            FieldSchema(name="sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR, description="sparse_vector"),
            FieldSchema(name="bm25_sparse", dtype=DataType.SPARSE_FLOAT_VECTOR, description="bm25_sparse"),
        ]
        bm25_func = Function(
            name="bm25", 
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["bm25_sparse"],
            params={})
        schema = CollectionSchema(fields, "test collection")
        schema.add_function(bm25_func)
        c = Collection(name=collection_name, schema=schema)
        log.info(f"create collection {collection_name} in upstream")
        # insert data to upstream
        nb = 3000
        data = [
            {
                "int64": i,
                "float": np.float32(i),
                "varchar": fake.sentence(),
                "text": fake.text(),
                "json": {"number": i, "varchar": str(i), "bool": bool(i)},
                "bool": bool(i),
                "array": [i for _ in range(10)],
                "float_embedding": [random.random() for _ in range(128)],
                "binary_embedding": [random.randint(0, 255) for _ in range(128//8)],
                "sparse_vector": {1: 0.5, 100: 0.3, 500: 0.8, 1024: 0.2, 5000: 0.6}
            }
            for i in range(nb)
        ]
        c.insert(data)
        # add null data
        data = [
            {
                "int64": None,
                "float": None,
                "varchar": None,
                "text": fake.text(),
                "json": None,
                "bool": None,
                "array": [],
                "float_embedding": [random.random() for _ in range(128)],
                "binary_embedding": [random.randint(0, 255) for _ in range(128//8)],
                "sparse_vector": {1: 0.5, 100: 0.3, 500: 0.8, 1024: 0.2, 5000: 0.6}
            }
            for i in range(nb)
        ]
        c.insert(data)
        c.flush(timeout=60)
        float_emb_index_params = {"index_type": "HNSW", "params": {"M": 128, "efConstruction": 128}, "metric_type": "L2"}
        binary_emb_index_params = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "HAMMING"}
        sparse_index_params = {"index_type": "SPARSE_INVERTED_INDEX", "params": {}, "metric_type": "IP"}
        bm25_index_params = {"index_type": "SPARSE_INVERTED_INDEX", "params": {"k1": 1.2, "b": 0.75}, "metric_type": "BM25"}
        c.create_index("float_embedding", float_emb_index_params)
        c.create_index("binary_embedding", binary_emb_index_params)
        c.create_index("sparse_vector", sparse_index_params)
        c.create_index("bm25_sparse", bm25_index_params)
        c.load()
        # get number of entities in upstream
        log.info(f"number of entities in upstream: {c.num_entities}")
        upstream_entities = c.num_entities
        upstream_index = [index.to_dict() for index in c.indexes]
        upstream_count = c.query(
            expr="",
            output_fields=["count(*)"]
        )[0]["count(*)"]
        assert upstream_count == upstream_entities
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
            if c_downstream.num_entities != upstream_entities:
                log.info(f"sync progress:{c_downstream.num_entities / upstream_entities * 100:.2f}%")
            # collections in subset of downstream
            if c_downstream.num_entities == upstream_entities:
                log.info(f"collection synced in downstream successfully cost time: {time.time() - t0:.2f}s")
                break
            time.sleep(10)
            try:
                c_downstream.flush(timeout=60)
            except Exception as e:
                log.info(f"flush err: {str(e)}")
        assert c_downstream.num_entities == upstream_entities

        # check index in downstream
        downstream_index = [index.to_dict() for index in c_downstream.indexes]
        assert sorted(upstream_index) == sorted(downstream_index)
        # check count in downstream
        downstream_count = c_downstream.query(
            expr="",
            output_fields=["count(*)"]
        )["count(*)"][0]
        assert downstream_count == upstream_count