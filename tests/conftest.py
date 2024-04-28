# Copyright 2023 Martin Galpin <galpin@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


def create_client(url, **kwargs):
    return ch.Client(url, user="default", password="test", **kwargs)
