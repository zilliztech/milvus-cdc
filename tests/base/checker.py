from enum import Enum
import threading
import random
import numpy as np
import time
from time import sleep
from datetime import datetime
import functools
from utils.util_log import test_log as log
from pymilvus import (
    connections, list_collections, has_partition,
    FieldSchema, CollectionSchema, DataType,
    Collection, Partition
)


dim = 128
nb = 3000
default_fields = [
    FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="float", dtype=DataType.FLOAT),
    FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
]
default_schema = CollectionSchema(fields=default_fields, description="test collection")

def get_collection_name_by_prefix(prefix="cdc_test"):
    all_collections = list_collections()
    return [c for c in all_collections if c.startswith(prefix)]

def list_partitions(col):
    return [p.name for p in col.partitions]

def exception_handler():
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            try:
                res, result = func(self, *args, **kwargs)
                return res, result
            except Exception as e:
                log_row_length = 300
                e_str = str(e)
                log_e = e_str[0:log_row_length] + \
                    '......' if len(e_str) > log_row_length else e_str
                log.error(log_e)
                return e, False
        return inner_wrapper
    return wrapper



class Op(Enum):
    create_collection = "create_collection"
    drop_collection = "drop_collection"
    create_partition = "create_partition"
    drop_partition = "drop_partition"
    insert_collection = "insert_collection"
    delete_collection = "delete_collection"
    insert_partition = "insert_partition"
    delete_partition = "delete_partition"


class Checker:

    def __init__(self, host, port, name_prefix="cdc_test", c_name=None):
        self.host = host
        self.port = port
        self.name_prefix = name_prefix
        self.c_name = c_name
        self._succ = 0
        self._fail = 0
        self._total = 0
        self._is_running = False
        self._keep_running = True
        self._paused = False
        self.connections = connections
        self.query_expr = "int64 >= 0"
        self.output_fields = ["int64"]
        
    
    def total(self):
        self._total = self._succ + self._fail
        return self._total
    
    def run(self):
        self._is_running = True
        self._keep_running = True
        self._paused = False
        name = self.__class__.__name__
        t = threading.Thread(target=self.run_task, args=(), name=name, daemon=True)
        t.start()
        return t

    def terminate(self):
        self._keep_running = False
        self.wait_running_stop()
        self.reset()
    
    def pause(self):
        log.info("start to pause checker")
        self._paused = True
        self.wait_running_stop()
        log.info("checker pause finished")


    def resume(self):
        log.info("start to resume checker")
        self._paused = False
        self._is_running = True
        log.info("checker resume finished")

    def wait_running_stop(self, timeout=60):
        log.info("start to wait running stop")
        t0 = time.time()
        while self._is_running and time.time() - t0 < timeout:
            log.debug(f"is running: {self._is_running}")
            sleep(1)
            if time.time() - t0 > timeout:
                log.info(f"current status: is running: {self._is_running}, keep running: {self._keep_running}, paused: {self._paused}")
                break
        if self._is_running:
            log.error("wait running stop timeout")
        else:
            log.info("wait running stop finished")
    
    def reset(self):
        self._succ = 0
        self._fail = 0
        self._total = 0


    def get_num_entities(self, p_name=None):
        if p_name:
            self.p = Partition(collection=self.collection, name=p_name)
            self.p.flush()
            return self.p.num_entities
        self.collection.flush()
        return self.collection.num_entities
    
    def get_count_by_query(self, expr="int64 >= 0", output_fields=["int64"], p_name=None):
        self.query_expr = expr
        self.output_fields = output_fields
        if len(self.collection.indexes) == 0:
            self.collection.create_index(field_name="float_vector", index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        self.collection.load()
        if p_name:
            res = self.collection.query(expr=expr, output_fields=self.output_fields, partition_names=[p_name])
            return len(res)
        res = self.collection.query(expr=expr, output_fields=self.output_fields)
        return len(res)

    def get_num_entities_of_partition(self):
        self.p = Partition(name=self.p_name, collection_name=self.c_name)
        self.p.flush()
        return self.p.num_entities
    
    def get_count_by_query_of_partition(self, expr="int64 >= 0", output_fields=["int64"]):
        self.query_expr = expr
        self.output_fields = output_fields
        self.collection.create_index(field_name="float_vector", index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        self.collection.load()
        res = self.collection.query(expr=expr, output_fields=self.output_fields, partition_names=[self.p_name])
        return len(res)


class CreateCollectionChecker(Checker):
    def __init__(self, host, port, name_prefix="cdc_test"):
        super().__init__(host, port, name_prefix)
        self.collection_name_list = []
        self.collection_name = name_prefix + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')

    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    Collection(name=self.collection_name, schema=default_schema)
                    collections = list_collections()
                    if self.collection_name in collections:
                        self._succ += 1
                        log.info(f"create collection {self.collection_name} successfully")
                        self.collection_name_list.append(self.collection_name)
                        self.collection_name = f"{self.name_prefix}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
                    else:
                        self._fail += 1
                        log.error(f"create collection {self.collection_name} failed")
                    sleep(1)
                except Exception as e:
                    log.error(f"create collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    log.info(f"checker paused or terminated: {self._paused}, keep running: {self._keep_running}")
                    self._is_running = False
            sleep(1)

class DropCollectionChecker(Checker):
    def __init__(self, host, port, name_prefix="cdc_test"):
        super().__init__(host, port, name_prefix=name_prefix)
        self.deleted_collections = []
        self.all_collections = get_collection_name_by_prefix(name_prefix)
        self.collection_name = name_prefix + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')

    def run_task(self):
        while self._keep_running:
            while not self._paused and self.all_collections:
                try:
                    c_name = self.all_collections.pop()
                    self.collection_name = c_name
                    Collection(name=c_name).drop()
                    collections = list_collections()
                    if self.collection_name not in collections:
                        self._succ += 1
                        log.info(f"drop collection {self.collection_name} successfully")
                        self.deleted_collections.append(self.collection_name)
                    else:
                        self._fail += 1
                        log.error(f"create collection {self.collection_name} failed")
                    sleep(1)
                except Exception as e:
                    log.error(f"create collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False
            sleep(1)
        log.info(f"deleted collections: {self.deleted_collections}")

class CreatePartitionChecker(Checker):
    def __init__(self, host, port, c_name="cdc_create_partition_test"):
        super().__init__(host, port, c_name=c_name)
        self.collection_name = c_name
        self.partition_name_list = []
        self.partition_name = f"{self.name_prefix}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
        self.collection = Collection(name=self.collection_name, schema=default_schema)
    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    p = Partition(collection=self.collection, name=self.partition_name)
                    if has_partition(self.collection.name, p.name) is True:
                        self._succ += 1
                        log.info(f"create partition {self.partition_name} for collection {self.collection_name} successfully")
                        self.partition_name_list.append(self.collection_name)
                        self.partition_name = f"{self.name_prefix}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
                    else:
                        self._fail += 1
                        log.error(f"create partition {self.partition_name} for collection {self.collection_name} failed")
                    sleep(1)
                except Exception as e:
                    log.error(f"create partition {self.partition_name} for collection {self.collection_name} may failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False
            sleep(1)

class DropPartitionChecker(Checker):
    def __init__(self, host, port, name_prefix="", c_name="cdc_create_partition_test"):
        super().__init__(host, port, name_prefix=name_prefix, c_name=c_name)
        self.collection_name = c_name
        self.deleted_partition_list = []
        self.partition_name = f"{self.name_prefix}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
        self.collection = Collection(name=self.collection_name, schema=default_schema)
        self.partition_name_list = [p.name for p in self.collection.partitions if p.name.startswith(self.name_prefix)]
    
    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    p_name = self.partition_name_list.pop()
                    p = Partition(collection=self.collection, name=p_name)
                    p.drop()
                    if has_partition(self.collection.name, p.name) is False:
                        self._succ += 1
                        log.info(f"drop partition {self.partition_name} for collection {self.collection_name} successfully")
                        self.deleted_partition_list.append(self.collection_name)
                    else:
                        self._fail += 1
                        log.error(f"drop partition {self.partition_name} for collection {self.collection_name} failed")
                    sleep(1)
                except Exception as e:
                    log.error(f"drop partition {self.partition_name} for collection {self.collection_name} may failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False            
            sleep(1)

class InsertEntitiesCollectionChecker(Checker):
    def __init__(self, host, port, name_prefix="", c_name="cdc_insert_collection_test"):
        super().__init__(host, port, name_prefix=name_prefix, c_name=c_name)
        self.collection_name = c_name
        self.collection = Collection(name=self.collection_name, schema=default_schema)
        self.query_expr = "int64 >= 0"
        self.output_fields = ["int64"]


    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    default_entities = [
                        [i for i in range(self.total()*nb, self.total()*nb+nb)],
                        [np.float32(i) for i in range(nb)],
                        [str(i) for i in range(nb)],
                        [[random.random() for _ in range(dim)] for _ in range(nb)]                      
                    ]
                    t0 = time.time()
                    self.collection.insert(default_entities)
                    t1 = time.time()
                    self._succ += 1
                    log.info(f"insert {nb} entities into collection {self.collection_name} successfully, cost {t1-t0}s")
                    sleep(1)
                except Exception as e:
                    log.error(f"insert {nb} entities into collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                log.info(f"total: {self.total()}, succ: {self._succ}, fail: {self._fail}")

                if self._paused or not self._keep_running:
                    log.info("stop insert entities into collection")
                    self._is_running = False
                log.info(f"is running: {self._is_running}")
                sleep(1)
            sleep(1)

class InsertEntitiesPartitionChecker(Checker):
    def __init__(self, host, port, name_prefix="cdc_insert_partition_test", c_name="cdc_test", p_name="cdc_insert_partition_test"):
        super().__init__(host, port, name_prefix=name_prefix, c_name=c_name)
        self.collection_name = c_name
        self.collection = Collection(name=self.collection_name, schema=default_schema)
        self.partition_name = p_name
        self.p = Partition(collection=self.collection, name=self.partition_name)

    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:

                    default_entities = [
                        [i for i in range(self.total()*nb, self.total()*nb+nb)],
                        [np.float32(i) for i in range(nb)],
                        [str(i) for i in range(nb)],
                        [[random.random() for _ in range(dim)] for _ in range(nb)]                      
                    ]

                    self.p.insert(default_entities)
                    self._succ += 1
                    log.info(f"insert {nb} entities into partition {self.p.name} of collection {self.collection_name} successfully")
                    sleep(1)
                except Exception as e:
                    log.error(f"insert {nb} entities into partition {self.p.name} of collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False
            sleep(1)

class DeleteEntitiesCollectionChecker(Checker):
    def __init__(self, host, port, name_prefix="", c_name="cdc_insert_collection_test"):
        super().__init__(host, port, name_prefix=name_prefix, c_name=c_name)
        self.collection_name = c_name
        self.delete_expr = f"int64 in {[i for i in range(self.total()*10, self.total()*10+10)]}"
        self.delete_expr_list = []
        self.collection = Collection(name=self.collection_name, schema=default_schema)

    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    self.collection.delete(self.delete_expr)
                    self._succ += 1
                    self.delete_expr_list.append(self.delete_expr)
                    log.info(f"delete entities with expr {self.delete_expr} for collection {self.collection_name} successfully")
                    self.delete_expr = f"int64 in {[i for i in range(self.total()*10, self.total()*10+10)]}"
                    sleep(1)
                except Exception as e:
                    log.error(f"delete entities with expr {self.delete_expr} for collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False
            sleep(1)


class DeleteEntitiesPartitionChecker(Checker):
    def __init__(self, host, port, name_prefix="", c_name="cdc_test", p_name="cdc_delete_partition_test"):
        super().__init__(host, port, name_prefix=name_prefix, c_name=c_name)
        self.collection_name = c_name
        self.delete_expr = f"int64 in {[i for i in range(self.total()*10, self.total()*10+10)]}"
        self.delete_expr_list = []
        self.collection = Collection(name=self.collection_name, schema=default_schema)
        self.p = Partition(collection=self.collection, name=p_name)

    def run_task(self):
        while self._keep_running:
            while self._keep_running and not self._paused:
                try:
                    self.collection.delete(self.delete_expr, partition_name=self.p.name)
                    self._succ += 1
                    self.delete_expr_list.append(self.delete_expr)
                    log.info(f"delete entities in {self.p.name}  with expr {self.delete_expr} for collection {self.collection_name} successfully")
                    self.delete_expr = f"int64 in {[i for i in range(self.total()*10, self.total()*10+10)]}"
                    sleep(1)
                except Exception as e:
                    log.error(f"delete entities in {self.p.name} with expr {self.delete_expr} for collection {self.collection_name} failed, {e}")
                    self._fail += 1
                    sleep(1)
                if self._paused or not self._keep_running:
                    self._is_running = False
            sleep(1)           