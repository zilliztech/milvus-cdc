import pytest


def pytest_addoption(parser):
    parser.addoption("--upstream_host", action="store", default="localhost", help="upstream host")
    parser.addoption("--upstream_port", action="store", default=19530, help="upstream port")
    parser.addoption("--upstream_mq_type", action="store", default="pulsar", help="upstream mq type")
    parser.addoption("--upstream_username", action="store", default="", help="upstream username")
    parser.addoption("--upstream_password", action="store", default="", help="upstream password")
    parser.addoption("--downstream_host", action="store", default="localhost", help="downstream host")
    parser.addoption("--downstream_port", action="store", default=19530, help="downstream port")
    parser.addoption("--downstream_username", action="store", default="", help="downstream username")
    parser.addoption("--downstream_password", action="store", default="", help="downstream password")

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