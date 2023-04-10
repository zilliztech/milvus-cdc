import json
import requests


class MilvusCdcClient:

    def __init__(self, url) -> None:
        self.url = url
        self.headers = {'Content-Type': 'application/json'}
        
    def create_task(self, request_data):
        url = self.url + '/cdc'
        body = {
            "request_type": "create",
            "request_data": request_data
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False
    
    def list_tasks(self):
        url = self.url + '/cdc'
        body = {
            "request_type": "list"
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False
    
    def get_task(self, task_id):
        url = self.url + '/cdc'
        body = {
            "request_type": "get",
            "request_data": {
                "task_id": task_id
            }
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False
    
    def pause_task(self, task_id):
        url = self.url + '/cdc'
        body = {
            "request_type": "pause",
            "request_data": {
                "task_id": task_id
            }
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False
    
    def resume_task(self, task_id):
        url = self.url + '/cdc'
        body = {
            "request_type": "resume",
            "request_data": {
                "task_id": task_id
            }
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False
    
    def delete_task(self, task_id):
        url = self.url + '/cdc'
        body = {
            "request_type": "delete",
            "request_data": {
                "task_id": task_id
            }
        }
        payload = json.dumps(body)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            return response.json(), True
        else:
            return response.text, False

if __name__ == '__main__':
    client = MilvusCdcClient('http://localhost:8444')
    # create task
    create_request_data = {
        "milvus_connect_param": {
            "host": "10.101.128.34",
            "port": 19530,
            "username": "",
            "password": "",
            "enable_tls": False,
            "ignore_partition": True,
            "connect_timeout": 10
        },
        "collection_infos": [
            {
                "name": "hello_milvus_v4"
            }
        ]
    }
    rsp = client.create_task(create_request_data)
    print(rsp)
    # list tasks
    rsp, _ = client.list_tasks()
    task_ids = []
    for task in rsp['tasks']:
        print(task)
        task_ids.append(task['task_id'])
    
    print(task_ids)
    # get task
    rsp, _ = client.get_task(task_ids[0])
    print(rsp)
    # pause task
    rsp, _ = client.pause_task(task_ids[0])
    print(rsp)
    # get task
    rsp, _ = client.get_task(task_ids[0])
    print(rsp)
    # resume task
    rsp, _ = client.resume_task(task_ids[0])
    print(rsp)
    # get task
    rsp, _ = client.get_task(task_ids[0])
    print(rsp)
    # delete task
    rsp, _ = client.delete_task(task_ids[0])
    print(rsp)
    # get task
    rsp, _ = client.get_task(task_ids[0])
    print(rsp)
