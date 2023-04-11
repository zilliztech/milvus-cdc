import time
import math
from datetime import datetime
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient
from pymilvus import (
    connections, list_collections,
    Collection
)
from base.checker import (
    InsertEntitiesCollectionChecker,
    DeleteEntitiesCollectionChecker,
)
from base.client_base import TestBase

prefix = "cdc_create_task_"
client = MilvusCdcClient('http://localhost:8444')


def divide_time(total_time, num_segments=10):
    segment_times = []
    for i in range(1, num_segments + 1):
        segment_time = math.exp(i - 1) / sum([math.exp(j - 1) for j in range(1, num_segments + 1)]) * total_time
        segment_times.append(segment_time)
    return segment_times


class TestCdcStability(TestBase):
    """ Test Milvus CDC end to end """

    def test_cdc_for_collection_insert_delete_concurrent_after_cdc_task(self, upstream_host, upstream_port, downstream_host, downstream_port, duration_time, task_num):
        connections.connect(host=upstream_host, port=upstream_port)
        c_name_list = [prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') for i in range(task_num)]
        insert_checker_list = []
        delete_checker_list = []
        check_times = 10
        base_sleep_time = 60
        sleep_time_list = divide_time(duration_time, num_segments=check_times)
        sleep_time_list = [base_sleep_time + i for i in sleep_time_list]
        log.info(f"sleep_time_list: {sleep_time_list}")
        for i in range(task_num):
            c_name = c_name_list[i]
            c_infos = [
                {"name": c_name}
            ]
            # create a cdc task, not ignore partition
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
            # insert entities in upstream
            insert_checker = InsertEntitiesCollectionChecker(host=upstream_host, port=upstream_port, c_name=c_name)
            insert_checker.run()
            insert_checker_list.append(insert_checker)

            # wait for the collection to be created
            # delete entities in upstream
            delete_checker = DeleteEntitiesCollectionChecker(host=upstream_host, port=upstream_port, c_name=c_name)
            delete_checker.run()
            delete_checker_list.append(delete_checker)
        checker_list = list(zip(insert_checker_list, delete_checker_list))
        for i in range(check_times):
            log.info(f"start to check entities in upstream and downstream, {i}th time, sleep time: {sleep_time_list[i]}")

            time.sleep(sleep_time_list[i])
            log.info("start to pause all checkers")
            for i, checker in enumerate(checker_list):
                insert_checker, delete_checker = checker
                insert_checker.pause()
                delete_checker.pause()                
            log.info("start to check entities in upstream and downstream")
            for i, checker in enumerate(checker_list):
                c_name = c_name_list[i]
                insert_checker, delete_checker = checker
                insert_checker.pause()
                delete_checker.pause()
                # check entities in upstream
                num_entities_upstream = insert_checker.get_num_entities()
                log.info(f"num_entities in upstream: {num_entities_upstream}")
                count_by_query_upstream = insert_checker.get_count_by_query()
                log.info(f"count_by_query in upstream: {count_by_query_upstream}")

                log.info("start to connect to downstream")
                # check entities in downstream
                connections.disconnect("default")
                log.info("disconnect default")
                connections.connect(host=downstream_host, port=downstream_port)
                log.info("connect to downstream")
                timeout = 60
                t0 = time.time()
                while True and time.time() - t0 < timeout:
                    if c_name in list_collections():
                        break
                    time.sleep(1)
                    if time.time() - t0 > timeout:
                        raise Exception(f"Timeout waiting for collection {c_name} to be created")
                col = Collection(name=c_name)
                index_infos = [index.to_dict() for index in col.indexes]
                if len(index_infos) == 0:
                    col.create_index(field_name="float_vector",
                                    index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
                col.load()
                # wait for the collection to be synced
                timeout = 120
                count_by_query_downstream = len(col.query(expr="int64 >= 0", output_fields=["int64"]))
                t0 = time.time()
                while True and time.time() - t0 < timeout:
                    count_by_query_downstream = len(col.query(expr="int64 >= 0", output_fields=["int64"]))
                    # log.info(f"count_by_query_downstream: {len(count_by_query_downstream)}")
                    log.info(
                        f"count_by_query_downstream {count_by_query_downstream},"
                        f"count_by_query_upstream {count_by_query_upstream}")
                    if count_by_query_downstream == count_by_query_upstream:
                        log.info(f"collection {c_name} has been synced")
                        break
                    time.sleep(1)
                    if time.time() - t0 > timeout:
                        raise Exception(f"Timeout waiting for collection {c_name} to be synced")
                log.info(f"count_by_query in downstream: {count_by_query_downstream}")
                assert count_by_query_upstream == count_by_query_downstream
                # flush collection in downstream
                col.flush()
                num_entities_downstream = col.num_entities
                log.info(f"num_entities in downstream: {num_entities_downstream}")
                assert num_entities_upstream == num_entities_downstream

            connections.disconnect("default")
            connections.connect(host=upstream_host, port=upstream_port)
            for i, checker in enumerate(checker_list):
                insert_checker, delete_checker = checker
                insert_checker.resume()
                delete_checker.resume()     


