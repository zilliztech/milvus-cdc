# Milvus-CDC Usage

## Limitation

1. Only the cluster dimension can be synchronized, that is, the collection name can only be specified as `*` when creating a task. The collection dimension will also be supported in the future.
2. Supported operations
- Create/Drop Collection
- Insert/Delete
- Create/Drop Partition
- Create/Drop Index
- Load/Release/Flush
- Create/Drop Database

    **Anything not mentioned is not supported;**

3. Milvus cdc only supports synchronized data. If you need active and backup disaster recovery functions, please contact us;

## Configuration

The configuration consists of two main parts: the Milvus configuration for Milvus-Target and the CDC startup configuration.

Furthermore, please ensure that both the source and target Milvus versions are **2.3.2 or higher**. Any other versions are not supported.

1. Milvus-Target Config

    Set the value of `common.ttMsgEnabled` in the milvus.yaml file in the milvus-target cluster to `false`.

2. CDC Startup Config

    The following is the cdc startup configuration file template. You can see this file in the `server/configs` directory.

```yaml
# cdc server address
address: 0.0.0.0:8444
# max task num
maxTaskNum: 100
# max task name length
maxNameLength: 256

# cdc meta data config
metaStoreConfig:
  # the metastore type, available value: etcd, mysql
  storeType: etcd
  # etcd address
  etcdEndpoints:
    - localhost:2379
  # mysql connection address
  mysqlSourceUrl: root:root@tcp(127.0.0.1:3306)/milvus-cdc?charset=utf8
  # meta data prefix, if multiple cdc services use the same store service, you can set different rootPaths to achieve multi-tenancy
  rootPath: cdc

# milvus-source config, these settings are basically the same as the corresponding configuration of milvus.yaml in milvus source.
sourceConfig:
  # etcd config
  etcdAddress:
    - localhost:2379
  etcdRootPath: by-dev
  etcdMetaSubPath: meta
  # default partition name
  defaultPartitionName: _default
  # read buffer length, mainly used for buffering if writing data to milvus-target is slow.
  readChanLen: 10
  # milvus-target mq config, which is pulsar or kafka
  pulsar:
    address: pulsar://localhost:6650
    webAddress: localhost:80
    maxMessageSize: 5242880
    tenant: public
    namespace: default
#    authPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken
#    authParams: token:xxx
#  kafka:
#    address: 127.0.0.1:9092
```

After completing these two steps of configuration, do not rush to start CDC. Need to ensure the following points:

1. Whether the `common.ttMsgEnabled` configuration value of milvus-target is `false``;
2. Confirm that the `mq` type configured by cdc is the same as the type configured by milvus-source;
3. Ensure that the network environment where the cdc service is located can correctly connect to the mq and etcd addresses in the milvus-source in the configuration;

## Usage

All http APIs need to comply with the following rules:

- Request method: POST
- Request path:/cdc
- Request body:

```json
{
     "request_type": "",
     "request_data": {}
}
```

Different requests are distinguished by `request_type`, which currently includes: create, delete, pause, resume, get and list. If the request fails, a non-200 http status code will be returned.

### create request

- milvus_connect_param, the connection params of the milvus-target server;
- collection_infos, the collection information that needs to be synchronized, which currently only supports `*`;
- rpc_channel_info, the corresponding name value is composed of the two values ​​of `common.chanNamePrefix.cluster` and `common.chanNamePrefix.replicateMsg` in **milvus-source**, connected by the symbol `-`

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"create",
  "request_data":{
    "milvus_connect_param":{
      "host":"localhost",
      "port":19530,
      "username":"root",
      "password":"Milvus",
      "enable_tls":true,
      "connect_timeout":10
    },
    "collection_infos":[
      {
        "name":"*"
      }
    ],
    "rpc_channel_info": {
      "name": "by-dev-replicate-msg"
    }
  }
}
```

After success, the task_id will be returned, such as:

```json
{"task_id":"6623ae52d35842a5a2c9d89b16ed7aa1"}
```

If there is an exception, an http error will appear.

### delete request

delete a cdc task.

**request**

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"delete",
  "request_data": {
    "task_id": "f84605ae48fb4170990ab80addcbd71e"
  }
}
```

**response**

```json
{}
```

### pause request

pause a cdc task.

**request**

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"pause",
  "request_data": {
    "task_id": "4d458a58b0f74e85b842b1244dc69546"
  }
}
```

**response**

```json
{}
```

### resume request

resume a cdc task.

**request**

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"resume",
  "request_data": {
    "task_id": "4d458a58b0f74e85b842b1244dc69546"
  }
}
```

**response**

```json
{}
```

### get request

get a cdc task info

**request**

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"get",
  "request_data": {
    "task_id": "4d458a58b0f74e85b842b1244dc69546"
  }
}
```

**response**

```json
{
    "task_id":"4d458a58b0f74e85b842b1244dc69546",
    "Milvus_connect_param":{
        "host":"localhost",
        "port":19530,
        "connect_timeout":10
    },
    "collection_infos":[
        {
            "name":"*"
        }
    ],
    "state":"Running"
}
```

### list request

list the info of all cdc tasks.

**request**

```http
POST localhost:8444/cdc
Content-Type: application/json

body:
{
  "request_type":"list"
}
```

**response**

```json
{
    "tasks": [
        {
            "task_id": "4d458a58b0f74e85b842b1244dc69546",
            "Milvus_connect_param": {
                "host": "localhost",
                "port": 19530,
                "connect_timeout": 10
            },
            "collection_infos": [
                {
                    "name": "*"
                }
            ],
            "state": "Running"
        }
    ]
}
```