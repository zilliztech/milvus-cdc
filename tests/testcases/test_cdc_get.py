import time
from datetime import datetime
from utils.util_log import test_log as log
from api.milvus_cdc import MilvusCdcClient
from pymilvus import (
    connections
)
from base.client_base import TestBase

prefix = "cdc_get_"
client = MilvusCdcClient('http://localhost:8444')


class TestCdcGet(TestBase):
    """ Test Milvus CDC end to end """

    def test_cdc_get_task(self, upstream_host, upstream_port, downstream_host, downstream_port):
        """
        target: test cdc default
        method: create task with default params
        expected: create successfully
        """
        connections.connect(host=upstream_host, port=upstream_port)
        col_list = []
        task_id_list = []
        for i in range(10):
            time.sleep(0.1)
            collection_name = prefix + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
            col_list.append(collection_name)
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
                "collection_infos": [
                    {"name": collection_name}
                ]
            }
            # create a cdc task
            rsp, result = client.create_task(request_data)
            assert result
            log.info(f"create task response: {rsp}")
            task_id = rsp['task_id']
            task_id_list.append(task_id)
        # get task 
        for i, task_id in enumerate(task_id_list):
            rsp, result = client.get_task(task_id)
            assert result
            log.info(f"get task response: {rsp}")
            assert rsp['state'] == 'Running'
            assert rsp['task_id'] == task_id
            assert rsp['collection_infos'][0]['name'] == col_list[i]

        # pause task
        for i, task_id in enumerate(task_id_list):
            rsp, result = client.pause_task(task_id)
            assert result
            rsp, result = client.get_task(task_id)
            log.info(f"get pause task response: {rsp}")
            assert rsp['state'] == 'Paused'
            assert rsp['task_id'] == task_id
            assert rsp['collection_infos'][0]['name'] == col_list[i]
        # resume task
        for i, task_id in enumerate(task_id_list):
            rsp, result = client.resume_task(task_id)
            assert result
            rsp, result = client.get_task(task_id)
            log.info(f"get resume task response: {rsp}")
            assert rsp['state'] == 'Running'
            assert rsp['task_id'] == task_id
            assert rsp['collection_infos'][0]['name'] == col_list[i]
        # delete task
        for i, task_id in enumerate(task_id_list):
            rsp, result = client.delete_task(task_id)
            assert result
            rsp, result = client.get_task(task_id)
            log.info(f"get delete task response: {rsp}")
            assert not result
