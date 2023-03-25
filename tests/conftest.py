import pytest

import clickhouse_arrow as ch


@pytest.fixture(scope="session")
def clickhouse(docker_ip, docker_services):
    port = docker_services.port_for("clickhouse", 8123)
    url = f"http://{docker_ip}:{port}/"

    def is_responsive():
        try:
            client = create_client(url)
            result = client.read_table("select 1")
            return len(result) == 1
        except:
            return False

    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=1.0,
        check=lambda: is_responsive(),
    )
    return url


@pytest.fixture
def client(clickhouse):
    return create_client(clickhouse)


def create_client(url):
    return ch.Client(url, user="default", password="test")
