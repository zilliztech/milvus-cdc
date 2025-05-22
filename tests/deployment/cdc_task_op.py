from loguru import logger
import requests
import json
import time

def create_task(host, downstream_uri, downstream_token="root:Milvus", channel_num=16):
    url = f"http://{host}:8444/cdc"
    payload = {
    "request_type": "create",
    "request_data": {
        "milvus_connect_param": {
            "uri": downstream_uri,
            "token": downstream_token,
            "connect_timeout": 120,
            "channel_num": channel_num
        },
        "extra_info": {
            "enable_user_role": True
        },
        "db_collections": {
            "*": [
                {
                    "name": "*"
                }
            ]
        }
        }
    }
    payload = json.dumps(payload)
    rsp = requests.post(url, data=payload)
    res= rsp.json()
    logger.info(f"create task payload: {payload}, response: {res}")
    return res

def list_task(host):
    url = f"http://{host}:8444/cdc"
    payload = {
    "request_type": "list",
    }
    payload = json.dumps(payload)
    rsp = requests.post(url, data=payload)
    res= rsp.json()
    logger.info(f"list task payload: {payload}, response: {res}")
    all_tasks = res["data"]["tasks"]
    return all_tasks


def resume_task(host,task_id):
    url = f"http://{host}:8444/cdc"
    payload = {
    "request_type": "resume",
    "request_data": {
        "task_id": task_id,
    }
    }
    payload = json.dumps(payload)
    rsp = requests.post(url, data=payload)
    res= rsp.json()
    logger.info(f"resume task payload: {payload}, response: {res}")
    return res

def pause_task(host, task_id):
    url = f"http://{host}:8444/cdc"
    payload = {
    "request_type": "pause",
    "request_data": {
        "task_id": task_id,
        }
    }
    payload = json.dumps(payload)
    rsp = requests.post(url, data=payload)
    res= rsp.json()
    logger.info(f"pause task payload: {payload}, response: {res}")
    return res


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for cdc test')
    parser.add_argument('--cdc_host', type=str, default="127.0.0.1", help='host of cdc')
    parser.add_argument('--downstream_uri', type=str, default="http://127.0.0.1:19500", help='downstream uri')
    parser.add_argument('--downstream_token', type=str, default="root:Milvus", help='downstream token')
    parser.add_argument('--channel_num', type=int, default=16, help='channel num of downstream')
    parser.add_argument('--op_name', type=str, default="pause", help='pause all task')
    args = parser.parse_args()
    host = args.cdc_host
    downstream_uri = args.downstream_uri
    downstream_token = args.downstream_token
    channel_num = args.channel_num
    op_name = args.op_name
    if op_name == "create":
        create_task(host, downstream_uri, downstream_token, channel_num)
    if op_name == "list":
        list_task(host)
    if op_name == "resume":
        tasks = list_task(host)
        for t in tasks:
            task_id = t["task_id"]
            resume_task(host, task_id)
    if op_name == "pause":
        tasks = list_task(host)
        for t in tasks:
            task_id = t["task_id"]
            pause_task(host, task_id)
    time.sleep(20)
    list_task(host)
