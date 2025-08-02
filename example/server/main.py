from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import pyarrow as pa

import os
import io

from mimir.api import MimirEngine, Inquiry
from mimir.api.loaders import FileConfigLoader
from mimir.api.models import InquiryRequest

import typing as t

import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

base_path = os.environ.get("CONFIGS_PATH")
secret_base_path = os.environ.get("SECRETS_PATH")

if not base_path:
    raise RuntimeError("CONFIGS_PATH environment variable is not set or empty")
if not secret_base_path:
    raise RuntimeError("SECRETS_PATH environment variable is not set or empty")

logger.info(f"CONFIGS_PATH: {base_path}")
logger.info(f"SECRETS_PATH: {secret_base_path}")

app = FastAPI()


def _get_engine():
    return MimirEngine(
        config_loader=FileConfigLoader(
            base_path=base_path,
            secret_base_path=secret_base_path,
        )
    )


def arrow_stream_generator(table):
    sink = io.BytesIO()
    writer = pa.ipc.new_stream(sink, table.schema)

    for batch in table.to_batches(max_chunksize=8192):
        writer.write_batch(batch)
        yield sink.getvalue()
        sink.seek(0)
        sink.truncate()

    writer.close()
    yield sink.getvalue()


@app.post("/inquiry")
async def inquiry(inquiry_request: InquiryRequest) -> StreamingResponse:
    """Endpoint function for inquries"""

    table = Inquiry(
        mimir_engine=_get_engine(), **inquiry_request.model_dump()
    ).dispatch()
    return StreamingResponse(
        arrow_stream_generator(table), media_type="application/vnd.apache.arrow.stream"
    )


@app.get("/schema")
def get_schema() -> t.Dict[str, t.Any]:
    """Endpoint function for virtual table schemas"""
    return _get_engine().get_schema()
