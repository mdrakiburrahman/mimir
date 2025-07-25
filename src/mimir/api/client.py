import io
import pyarrow as pa
import httpx
import requests

from mimir.api.models import InquiryRequest

import logging
import typing as t

logger = logging.getLogger(__name__)


class GeneratorStream(io.RawIOBase):
    """A file-like object that reads from a generator of bytes."""

    def __init__(self, generator: t.Iterator[bytes]):
        super().__init__()
        self.generator = generator
        self.buffer = io.BytesIO()
        self.buffer_pos = 0

    def readable(self) -> bool:
        """Returns True if the stream is readable."""
        return True

    def readinto(self, b: t.Any) -> int:
        """Reads bytes from the generator into a pre-allocated bytearray."""
        bytes_read = self.buffer.readinto(b)
        if bytes_read > 0:
            return bytes_read

        try:
            chunk = next(self.generator)
        except StopIteration:
            return 0

        self.buffer = io.BytesIO(chunk)
        return self.buffer.readinto(b)


class Client:
    """A client for interacting with the Mimir API."""

    def __init__(self, uri: str) -> None:
        """Initializes the client.

        Args:
            uri: The base URI of the Mimir API.
        """
        self.uri = uri
        self._async_client = httpx.AsyncClient(base_url=self.uri)

    def get_schema(self) -> t.Dict[str, t.Any]:
        """Retrieves the schema of the Mimir API.

        Returns:
            A dictionary representing the schema.
        """
        response = httpx.get(f"{self.uri}/schema")
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        if content_type != "application/json":
            raise ValueError(
                f"Unexpected content type: {content_type}. Expected application/json"
            )

        return dict(response.json())

    def query(
        self, *, inquiry: t.Optional[InquiryRequest] = None, **kwargs
    ) -> pa.Table:
        """
        Performs a synchronous inquiry and returns the complete Arrow Table.
        """
        inquiry = inquiry or InquiryRequest(**kwargs)
        payload = inquiry.model_dump()
        logger.info(f"Client sending payload: {payload}")
        response = httpx.post(f"{self.uri}/inquiry", json=payload)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        if content_type != "application/vnd.apache.arrow.stream":
            raise ValueError(
                f"Unexpected content type: {content_type}. Expected application/vnd.apache.arrow.stream"
            )

        with pa.ipc.open_stream(response.content) as reader:
            return reader.read_all()

    def query_stream(
        self, inquiry: t.Optional[InquiryRequest] = None, **kwargs
    ) -> t.Generator[bytes, None, None]:
        """
        Performs a synchronous inquiry and streams the response.
        Returns a generator of bytes.
        """
        inquiry = inquiry or InquiryRequest(**kwargs)
        payload = inquiry.model_dump()
        logger.info(f"Client sending payload (stream): {payload}")
        response = requests.post(f"{self.uri}/inquiry", json=payload, stream=True)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        if content_type != "application/vnd.apache.arrow.stream":
            raise ValueError(
                f"Unexpected content type: {content_type}. Expected application/vnd.apache.arrow.stream"
            )

        with pa.ipc.open_stream(GeneratorStream(response.iter_content(None))) as reader:
            yield from reader

    async def aquery(
        self, inquiry: t.Optional[InquiryRequest] = None, **kwargs
    ) -> pa.Table:
        """
        Performs an asynchronous inquiry and returns the complete Arrow Table.
        """
        inquiry = inquiry or InquiryRequest(**kwargs)
        payload = inquiry.model_dump()
        logger.info(f"Client sending payload (async): {payload}")
        response = await self._async_client.post("/inquiry", json=payload)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        if content_type != "application/vnd.apache.arrow.stream":
            raise ValueError(
                f"Unexpected content type: {content_type}. Expected application/vnd.apache.arrow.stream"
            )

        with pa.ipc.open_stream(response.content) as reader:
            return reader.read_all()
