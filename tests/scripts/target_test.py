import h5py
import numpy as np
import time
import copy
from pathlib import Path
from loguru import logger
import pymilvus
from pymilvus import connections, Collection, utility
import constant
pymilvus_version = pymilvus.__version__
all_index_types = ["IVF_FLAT", "IVF_SQ8", "HNSW"]
default_index_params = [{"nlist": 128}, {"nlist": 128}, {"M": 48, "efConstruction": 200}]
index_params_map = dict(zip(all_index_types, default_index_params))

def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors

def gen_index_params(index_type, metric_type="L2"):
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": metric_type}
    index = copy.deepcopy(default_index)
    index["index_type"] = index_type
    index["params"] = index_params_map[index_type]
    if index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        index["metric_type"] = "HAMMING"
    return index

def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]:
        for nprobe in [10]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [10]:
            bin_search_params = {"metric_type": "HAMMING", "params": {"nprobe": nprobe}}
            search_params.append(bin_search_params)
    elif index_type in ["HNSW"]:
        for ef in [150]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    else:
        logger.info("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params[0]


dim = 128
TIMEOUT = 200


def search_test(host="127.0.0.1", port=19530, index_type="HNSW"):
    logger.info(f"recall test for index type {index_type}")
    file_path = f"{str(Path(__file__).absolute().parent.parent)}/assets/ann_hdf5/sift-128-euclidean.hdf5"
    logger.info(f"read data from {file_path}...")
    train, test, neighbors = read_benchmark_hdf5(file_path)
    connections.connect(host=host, port=port)
    name = f"sift_128_euclidean_{index_type}"
    collection = Collection(name=f"sift_128_euclidean_{index_type}")
    t0 = time.time()
    logger.info(f"Get collection entities...")
    if pymilvus_version >= "2.2.0":
        collection.flush()
    else:
        collection.num_entities
    logger.info(collection.num_entities)
    t1 = time.time()
    logger.info(f"Get collection entities cost {t1 - t0:.4f} seconds")
    assert collection.num_entities == constant.ALL_ENTITIES_NUM

    # create index
    default_index = gen_index_params(index_type)
    logger.info(f"Create index...")
    t0 = time.time()
    collection.create_index(field_name="float_vector",
                            index_params=default_index)
    t1 = time.time()
    logger.info(f"Create index cost {t1 - t0:.4f} seconds")

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
    assert cnt == constant.ALL_ENTITIES_NUM
    logger.info(f"wait for loading complete...")
    time.sleep(30)
    res = utility.get_query_segment_info(name)
    logger.info(f"segments info: {res}")

    # search
    topK = 100
    nq = 10000
    current_search_params = gen_search_param(index_type)

    # define output_fields of search result
    for i in range(3):
        t0 = time.time()
        logger.info(f"Search...")
        res = collection.search(
            test[:nq], "float_vector", current_search_params, topK, output_fields=["int64"], timeout=TIMEOUT
        )
        t1 = time.time()
        logger.info(f"search cost  {t1 - t0:.4f} seconds")
        result_ids = []
        for hits in res:
            result_id = []
            for hit in hits:
                result_id.append(hit.entity.get("int64"))
            result_ids.append(result_id)
        logger.info(f"search result: {len(result_ids)}")
    # query
    expr = "int64 in [2,4,6,8]"
    output_fields = ["int64", "float"]
    res = collection.query(expr, output_fields, timeout=TIMEOUT)
    sorted_res = sorted(res, key=lambda k: k['int64'])
    for r in sorted_res:
        logger.info(r)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--host', type=str, default="127.0.0.1", help='milvus server ip')
    parser.add_argument('--port', type=int,
                        default=19530, help='milvus server port')
    args = parser.parse_args()
    host = args.host
    port = args.port
    tasks = []
    for index_type in ["HNSW"]:
        search_test(host, port, index_type)