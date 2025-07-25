import pytest
from unittest.mock import MagicMock, AsyncMock
from mimir.api.client import Client, GeneratorStream
import pyarrow as pa
import httpx
import io


@pytest.fixture
def mock_httpx_client(mocker):
    return mocker.patch("httpx.AsyncClient", autospec=True)


@pytest.fixture
def mock_httpx(mocker):
    return mocker.patch("httpx.get", autospec=True)


@pytest.fixture
def mock_httpx_post(mocker):
    return mocker.patch("httpx.post", autospec=True)


@pytest.fixture
def mock_requests_post(mocker):
    return mocker.patch("requests.post", autospec=True)


def test_client_get_schema_success(mock_httpx):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"schema": "test"}
    mock_httpx.return_value = mock_response

    client = Client(uri="http://test.com")
    schema = client.get_schema()

    assert schema == {"schema": "test"}
    mock_httpx.assert_called_once_with("http://test.com/schema")


def test_client_get_schema_http_error(mock_httpx):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    mock_httpx.return_value = mock_response

    client = Client(uri="http://test.com")
    with pytest.raises(httpx.HTTPStatusError):
        client.get_schema()


def test_client_get_schema_wrong_content_type(mock_httpx):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "text/html"}
    mock_httpx.return_value = mock_response

    client = Client(uri="http://test.com")
    with pytest.raises(ValueError):
        client.get_schema()


def test_client_query_success(mock_httpx_post):
    table = pa.table({"a": [1, 2, 3]})
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    buf = sink.getvalue()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
    mock_response.content = buf
    mock_httpx_post.return_value = mock_response

    client = Client(uri="http://test.com")
    result = client.query(metrics=["a"], dimensions=[])
    assert result.equals(table)


@pytest.mark.asyncio
async def test_client_aquery_success(mock_httpx_client):
    table = pa.table({"a": [1, 2, 3]})
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    buf = sink.getvalue()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
    mock_response.content = buf

    mock_async_client = mock_httpx_client.return_value
    mock_async_client.post = AsyncMock(return_value=mock_response)

    client = Client(uri="http://test.com")
    result = await client.aquery(metrics=["a"], dimensions=[])
    assert result.equals(table)


def test_generator_stream():
    def gen():
        yield b"hello"
        yield b"world"

    stream = GeneratorStream(gen())
    assert stream.readable()

    # Reading into a buffer larger than the first chunk
    b = bytearray(10)
    n = stream.readinto(b)
    assert n == 5
    assert b[:n] == b"hello"

    # Reading the rest
    n = stream.readinto(b)
    assert n == 5
    assert b[:n] == b"world"

    # End of stream
    n = stream.readinto(b)
    assert n == 0
