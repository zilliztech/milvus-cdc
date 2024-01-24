# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.


import random
import numpy as np
import time
import argparse
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)
TIMEOUT = 120


def hello_milvus(host="127.0.0.1"):
    import time
    # create connection
    connections.connect(host=host, port="19530")

    print(f"\nList collections...")
    print(list_collections())

    # create collection
    dim = 128
    default_fields = [
        FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="float", dtype=DataType.FLOAT),
        FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")

    print(f"\nCreate collection...")
    collection = Collection(name="delete_demo", schema=default_schema)

    print(f"\nList collections...")
    print(list_collections())
    time.sleep(10)
    while True:
        start_insert = input("start insert? y/n")
        if start_insert == "y":
            break
        else:
            time.sleep(10)

    #  insert data
    for i in range(1):
        nb = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
        t0 = time.time()
        collection.insert(
            [
                [i for i in range(nb)],
                [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)],
                vectors
            ]
        )
        t1 = time.time()
        print(f"\nInsert {nb} vectors cost {t1 - t0:.4f} seconds")
        time.sleep(20)

    # delete data
    while True:
        start_delete = input("start delete? y/n")
        if start_delete == "y":
            break
        else:
            time.sleep(10)
    
    for i in range(100):
        batch = 10
        expr = f"int64 in {[x for x in range(batch*i, batch*(i+1))]}"
        # delete half of data
        del_res = collection.delete(expr)
        print(del_res)
        print(f"delete data with expr {expr}")
        time.sleep(10)


parser = argparse.ArgumentParser(description='host ip')
parser.add_argument('--host', type=str, default='127.0.0.1', help='host ip')
args = parser.parse_args()
# add time stamp
print(f"\nStart time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
hello_milvus(args.host)
