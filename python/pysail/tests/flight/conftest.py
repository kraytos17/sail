"""Configuration for Arrow Flight SQL tests."""

import pytest

from pysail.flight import FlightSqlServer


@pytest.fixture(scope="module")
def flight_catalog_uri(tmp_path_factory):
    """Start a Flight SQL server using Sail's default Memory catalog.

    The server loads its application config (including the default Memory catalog with a
    ``default`` database) at construction time. An Iceberg warehouse is created under a
    temporary directory so tests can create tables with ``file://`` locations.

    :yields: A tuple of the ``grpc://`` URI and the warehouse directory.
    """
    warehouse = tmp_path_factory.mktemp("warehouse")
    server = FlightSqlServer(ip="127.0.0.1", port=0)
    server.start(background=True)
    address = server.listening_address
    if address is None:
        server.stop()
        raise RuntimeError("Flight SQL server failed to start")
    host, port = address
    uri = f"grpc://{host}:{port}"
    try:
        yield uri, warehouse
    finally:
        server.stop()


def pytest_configure(config):
    # Suppress ADBC autocommit warnings - Flight SQL doesn't support disabling autocommit
    config.addinivalue_line(
        "filterwarnings",
        "ignore:Cannot disable autocommit:Warning",
    )
