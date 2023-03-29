import h5py
import numpy as np
import time
import copy
from pathlib import Path
from loguru import logger
import pymilvus
from pymilvus import connections
from pymilvus import Collection, utility
import constant
pymilvus_version = pymilvus.__version__

def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors

dim = 128
TIMEOUT = 200


def delete_entities(host="127.0.0.1", port=19530, expr=""):
    connections.connect(host=host, port=port)
    name = f"sift_128_euclidean_HNSW"
    collection = Collection(name=name)

    # delete
    logger.info(f"delete entities...")
    t0 = time.time()
    res = collection.delete(expr)
    t1 = time.time()
    logger.info(f"delete entities cost {t1 - t0:.4f} seconds: {res}")


def query(host='127.0.0.1', port=19530, expr=""):
    connections.connect(host=host, port=port)
    name = f"sift_128_euclidean_HNSW"
    collection = Collection(name=name)
    t0 = time.time()
    logger.info(f"Get collection entities...")
    if pymilvus_version >= "2.2.0":
        collection.flush()
    else:
        collection.num_entities
    logger.info(collection.num_entities)
    t1 = time.time()
    logger.info(f"Get collection entities cost {t1 - t0:.4f} seconds")
    # load collection
    replica_number = 1
    logger.info(f"load collection...")
    t0 = time.time()
    collection.load(replica_number=replica_number)
    t1 = time.time()
    logger.info(f"load collection cost {t1 - t0:.4f} seconds")
    res = utility.get_query_segment_info(name)
    cnt = 0
    logger.info(f"segments info: {res}")
    for segment in res:
        cnt += segment.num_rows
    assert cnt == collection.num_entities
    logger.info(f"wait for loading complete...")
    time.sleep(30)
    res = utility.get_query_segment_info(name)
    logger.info(f"segments info: {res}")
    # query
    output_fields = ["int64", "float"]
    res = collection.query(expr, output_fields, timeout=TIMEOUT)
    sorted_res = sorted(res, key=lambda k: k['int64'])
    logger.info(f"query result: {len(sorted_res)}")
    connections.disconnect(alias="default")
    return len(res)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--source_host', type=str, default="127.0.0.1", help='milvus server ip')
    parser.add_argument('--source_port', type=int,
                        default=19530, help='milvus server port')
    parser.add_argument('--target_host', type=str, default="127.0.0.1", help='milvus server ip')
    parser.add_argument('--target_port', type=int,
                        default=19500, help='milvus server port')    
    args = parser.parse_args()
    source_host = args.source_host
    source_port = args.source_port
    target_host = args.target_host
    target_port = args.target_port
    query_expr = f"int64 in {[i for i in range(constant.ALL_ENTITIES_NUM)]}"
    source_data = query(host=source_host, port=source_port, expr=query_expr)
    target_data = query(host=target_host, port=target_port, expr=query_expr)
    assert source_data == target_data
    logger.info(f"query data from source and target are equal")
    delete_expr = f"int64 in {[i for i in range(constant.DELETED_ENTITIES_NUM)]}"
    delete_entities(host=source_host, port=source_port, expr=delete_expr)
    time.sleep(30) # wait for delete sink to target
    # check source has deleted data
    source_data = query(host=source_host, port=source_port, expr=query_expr)
    assert source_data == constant.ALL_ENTITIES_NUM - constant.DELETED_ENTITIES_NUM
    deleted_data = query(host=source_host, port=source_port, expr=delete_expr)
    assert deleted_data == 0
    # check target has deleted data
    target_data = query(host=target_host, port=target_port, expr=query_expr)
    assert target_data == constant.ALL_ENTITIES_NUM - constant.DELETED_ENTITIES_NUM
    deleted_data = query(host=target_host, port=target_port, expr=delete_expr)
    assert deleted_data == 0


