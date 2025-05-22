import pytest


def pytest_addoption(parser):
    parser.addoption("--upstream_host", action="store", default="localhost", help="upstream host")
    parser.addoption("--upstream_port", action="store", default=19530, help="upstream port")
    parser.addoption("--upstream_mq_type", action="store", default="pulsar", help="upstream mq type")
    parser.addoption("--upstream_username", action="store", default="root", help="upstream username")
    parser.addoption("--upstream_password", action="store", default="Milvus", help="upstream password")
    parser.addoption("--downstream_host", action="store", default="localhost", help="downstream host")
    parser.addoption("--downstream_port", action="store", default=19530, help="downstream port")
    parser.addoption("--downstream_username", action="store", default="root", help="downstream username")
    parser.addoption("--downstream_password", action="store", default="Milvus", help="downstream password")
    parser.addoption("--duration_time", action="store", default=7200, help="duration_time")
    parser.addoption("--task_num", action="store", default=10, help="task_num")
    parser.addoption("--upstream_minio_endpoint", action="store", default="127.0.0.1:9000", help="upstream minio endpoint")
    parser.addoption("--upstream_minio_bucket_name", action="store", default="a-bucket", help="upstream minio bucket name")
    parser.addoption("--downstream_minio_endpoint", action="store", default="127.0.0.1:9010", help="downstream minio endpoint")
    parser.addoption("--downstream_minio_bucket_name", action="store", default="milvus-bucket", help="downstream minio bucket name")

@pytest.fixture
def upstream_host(request):
    return request.config.getoption("--upstream_host")

@pytest.fixture
def upstream_port(request):
    return request.config.getoption("--upstream_port")

@pytest.fixture
def upstream_mq_type(request):
    return request.config.getoption("--upstream_mq_type")

@pytest.fixture
def upstream_username(request):
    return request.config.getoption("--upstream_username")

@pytest.fixture
def upstream_password(request):
    return request.config.getoption("--upstream_password")

@pytest.fixture
def downstream_host(request):
    return request.config.getoption("--downstream_host")

@pytest.fixture
def downstream_port(request):
    return request.config.getoption("--downstream_port")

@pytest.fixture
def downstream_username(request):
    return request.config.getoption("--downstream_username")

@pytest.fixture
def downstream_password(request):
    return request.config.getoption("--downstream_password")

@pytest.fixture
def duration_time(request):
    return request.config.getoption("--duration_time")

@pytest.fixture
def task_num(request):
    return request.config.getoption("--task_num")

@pytest.fixture
def upstream_minio_endpoint(request):
    return request.config.getoption("--upstream_minio_endpoint")

@pytest.fixture
def upstream_minio_bucket_name(request):
    return request.config.getoption("--upstream_minio_bucket_name")

@pytest.fixture
def downstream_minio_endpoint(request):
    return request.config.getoption("--downstream_minio_endpoint")

@pytest.fixture
def downstream_minio_bucket_name(request):
    return request.config.getoption("--downstream_minio_bucket_name")
