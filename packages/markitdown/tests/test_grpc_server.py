from __future__ import annotations

from concurrent import futures
from pathlib import Path

import grpc
import pytest

from markitdown.grpc.server import MarkItDownServiceServicer
from markitdown.grpc.v1 import markitdown_pb2, markitdown_pb2_grpc


@pytest.fixture
def grpc_client():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    markitdown_pb2_grpc.add_MarkItDownServiceServicer_to_server(
        MarkItDownServiceServicer(), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = markitdown_pb2_grpc.MarkItDownServiceStub(channel)
    try:
        yield stub
    finally:
        channel.close()
        server.stop(grace=None)


def test_convert_local_file(grpc_client, tmp_path: Path):
    sample_path = tmp_path / "sample.txt"
    sample_path.write_text("hello\ngrpc\n", encoding="utf-8")

    response = grpc_client.Convert(
        markitdown_pb2.ConvertRequest(
            source=markitdown_pb2.Source(
                local_path=str(sample_path),
                stream_info=markitdown_pb2.StreamInfo(
                    extension=".txt",
                    mimetype="text/plain",
                    charset="utf-8",
                ),
            ),
            conversion_options=markitdown_pb2.ConversionOptions(keep_data_uris=False),
            service_options=markitdown_pb2.ServiceOptions(
                enable_builtins=True, enable_plugins=False
            ),
        )
    )

    assert "hello" in response.result.markdown
    assert "grpc" in response.result.markdown


def test_convert_stream_returns_chunk_sequence(grpc_client):
    request = markitdown_pb2.ConvertRequest(
        source=markitdown_pb2.Source(
            content=b"one\ntwo\nthree\n",
            stream_info=markitdown_pb2.StreamInfo(
                extension=".txt", mimetype="text/plain", charset="utf-8"
            ),
        ),
        conversion_options=markitdown_pb2.ConversionOptions(keep_data_uris=False),
        service_options=markitdown_pb2.ServiceOptions(enable_builtins=True),
        streaming_options=markitdown_pb2.StreamingOptions(markdown_chunk_size_bytes=4),
    )

    stream = list(grpc_client.ConvertStream(request))

    assert stream[0].HasField("started")
    markdown_events = [event.markdown_chunk for event in stream if event.HasField("markdown_chunk")]
    assert len(markdown_events) > 0
    assert stream[-1].HasField("completed")
    assert stream[-1].completed.total_chunks == len(markdown_events)
    assert "".join(event.markdown for event in markdown_events).startswith("one")
    assert markdown_events[-1].is_last


def test_convert_requires_source_oneof(grpc_client):
    with pytest.raises(grpc.RpcError) as exc_info:
        grpc_client.Convert(markitdown_pb2.ConvertRequest())

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
