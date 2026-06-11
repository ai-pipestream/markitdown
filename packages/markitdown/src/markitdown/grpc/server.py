from __future__ import annotations

import argparse
import io
from concurrent import futures
from typing import Iterable, Iterator

import grpc

from markitdown import MarkItDown
from markitdown._base_converter import DocumentConverterResult
from markitdown._stream_info import StreamInfo
from markitdown.converters import ContentUnderstandingFileType

from .v1 import markitdown_pb2, markitdown_pb2_grpc


_DEFAULT_MARKDOWN_CHUNK_SIZE_BYTES = 4096

_CU_FILE_TYPE_MAP: dict[int, ContentUnderstandingFileType] = {
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_PDF: ContentUnderstandingFileType.PDF,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_DOCX: ContentUnderstandingFileType.DOCX,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_PPTX: ContentUnderstandingFileType.PPTX,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_XLSX: ContentUnderstandingFileType.XLSX,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_HTML: ContentUnderstandingFileType.HTML,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_TXT: ContentUnderstandingFileType.TXT,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MD: ContentUnderstandingFileType.MD,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_RTF: ContentUnderstandingFileType.RTF,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_XML: ContentUnderstandingFileType.XML,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_EML: ContentUnderstandingFileType.EML,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MSG: ContentUnderstandingFileType.MSG,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_JPEG: ContentUnderstandingFileType.JPEG,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_PNG: ContentUnderstandingFileType.PNG,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_BMP: ContentUnderstandingFileType.BMP,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_TIFF: ContentUnderstandingFileType.TIFF,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_HEIF: ContentUnderstandingFileType.HEIF,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MP4: ContentUnderstandingFileType.MP4,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_M4V: ContentUnderstandingFileType.M4V,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MOV: ContentUnderstandingFileType.MOV,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_AVI: ContentUnderstandingFileType.AVI,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MKV: ContentUnderstandingFileType.MKV,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_WEBM: ContentUnderstandingFileType.WEBM,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_FLV: ContentUnderstandingFileType.FLV,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_WMV: ContentUnderstandingFileType.WMV,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_WAV: ContentUnderstandingFileType.WAV,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_MP3: ContentUnderstandingFileType.MP3,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_M4A: ContentUnderstandingFileType.M4A,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_FLAC: ContentUnderstandingFileType.FLAC,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_OGG: ContentUnderstandingFileType.OGG,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_AAC: ContentUnderstandingFileType.AAC,
    markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_WMA: ContentUnderstandingFileType.WMA,
}


class MarkItDownServiceServicer(markitdown_pb2_grpc.MarkItDownServiceServicer):
    def Convert(
        self, request: markitdown_pb2.ConvertRequest, context: grpc.ServicerContext
    ) -> markitdown_pb2.ConvertResponse:
        conversion_result = self._convert_request(request, context)
        return markitdown_pb2.ConvertResponse(
            result=self._to_proto_result(conversion_result)
        )

    def ConvertStream(
        self,
        request: markitdown_pb2.ConvertStreamRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[markitdown_pb2.ConvertStreamResponse]:
        conversion_result = self._convert_request(request, context)
        source_kind = request.source.WhichOneof("input") or ""

        yield markitdown_pb2.ConvertStreamResponse(
            started=markitdown_pb2.ConversionStarted(source_kind=source_kind)
        )

        chunk_size = _DEFAULT_MARKDOWN_CHUNK_SIZE_BYTES
        if request.HasField("streaming_options") and request.streaming_options.HasField(
            "markdown_chunk_size_bytes"
        ):
            chunk_size = request.streaming_options.markdown_chunk_size_bytes
        if chunk_size == 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "streaming_options.markdown_chunk_size_bytes must be greater than zero.",
            )

        chunks = list(_chunk_markdown(conversion_result.markdown, int(chunk_size)))
        if not chunks:
            chunks = [""]

        for chunk_index, markdown_chunk in enumerate(chunks):
            yield markitdown_pb2.ConvertStreamResponse(
                markdown_chunk=markitdown_pb2.MarkdownChunk(
                    chunk_index=chunk_index,
                    markdown=markdown_chunk,
                    is_last=chunk_index == len(chunks) - 1,
                )
            )

        completed = markitdown_pb2.ConversionCompleted(total_chunks=len(chunks))
        if conversion_result.title:
            completed.title = conversion_result.title
        yield markitdown_pb2.ConvertStreamResponse(completed=completed)

    def _convert_request(
        self,
        request: markitdown_pb2.ConvertRequest | markitdown_pb2.ConvertStreamRequest,
        context: grpc.ServicerContext,
    ) -> DocumentConverterResult:
        source_kind = request.source.WhichOneof("input")
        if source_kind is None:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "source.input is required and must set one of local_path, uri, or content.",
            )

        markitdown_client = _create_markitdown(request.service_options)
        convert_kwargs = _build_convert_kwargs(
            request.conversion_options, request.source
        )

        if source_kind == "local_path":
            return markitdown_client.convert_local(
                request.source.local_path, **convert_kwargs
            )
        if source_kind == "uri":
            return markitdown_client.convert_uri(request.source.uri, **convert_kwargs)

        assert source_kind == "content"
        return markitdown_client.convert_stream(
            io.BytesIO(request.source.content), **convert_kwargs
        )

    @staticmethod
    def _to_proto_result(
        conversion_result: DocumentConverterResult,
    ) -> markitdown_pb2.ConversionResult:
        proto_result = markitdown_pb2.ConversionResult(
            markdown=conversion_result.markdown
        )
        if conversion_result.title:
            proto_result.title = conversion_result.title
        return proto_result


def _build_convert_kwargs(
    conversion_options: markitdown_pb2.ConversionOptions,
    source: markitdown_pb2.Source,
) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if conversion_options.HasField("keep_data_uris"):
        kwargs["keep_data_uris"] = conversion_options.keep_data_uris

    stream_info = _to_stream_info(source.stream_info)
    if stream_info is not None:
        kwargs["stream_info"] = stream_info
    return kwargs


def _to_stream_info(stream_info: markitdown_pb2.StreamInfo) -> StreamInfo | None:
    values: dict[str, str] = {}

    if stream_info.HasField("mimetype"):
        values["mimetype"] = stream_info.mimetype
    if stream_info.HasField("extension"):
        values["extension"] = stream_info.extension
    if stream_info.HasField("charset"):
        values["charset"] = stream_info.charset
    if stream_info.HasField("filename"):
        values["filename"] = stream_info.filename
    if stream_info.HasField("local_path"):
        values["local_path"] = stream_info.local_path
    if stream_info.HasField("url"):
        values["url"] = stream_info.url

    if not values:
        return None
    return StreamInfo(**values)


def _create_markitdown(service_options: markitdown_pb2.ServiceOptions) -> MarkItDown:
    kwargs: dict[str, object] = {}

    if service_options.HasField("enable_builtins"):
        kwargs["enable_builtins"] = service_options.enable_builtins
    if service_options.HasField("enable_plugins"):
        kwargs["enable_plugins"] = service_options.enable_plugins

    if service_options.HasField("document_intelligence"):
        kwargs["docintel_endpoint"] = service_options.document_intelligence.endpoint

    if service_options.HasField("content_understanding"):
        cu_options = service_options.content_understanding
        kwargs["cu_endpoint"] = cu_options.endpoint
        if cu_options.HasField("analyzer_id"):
            kwargs["cu_analyzer_id"] = cu_options.analyzer_id
        if cu_options.file_types:
            kwargs["cu_file_types"] = _to_cu_file_types(cu_options.file_types)

    return MarkItDown(**kwargs)


def _to_cu_file_types(
    file_types: Iterable[int],
) -> list[ContentUnderstandingFileType]:
    converted: list[ContentUnderstandingFileType] = []
    for file_type in file_types:
        if file_type == markitdown_pb2.CONTENT_UNDERSTANDING_FILE_TYPE_UNSPECIFIED:
            continue
        converted.append(_CU_FILE_TYPE_MAP[file_type])
    return converted


def _chunk_markdown(markdown: str, chunk_size: int) -> Iterator[str]:
    if not markdown:
        return

    start = 0
    while start < len(markdown):
        end = min(start + chunk_size, len(markdown))
        yield markdown[start:end]
        start = end


def serve(bind_address: str = "127.0.0.1:50051", max_workers: int = 10) -> grpc.Server:
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    markitdown_pb2_grpc.add_MarkItDownServiceServicer_to_server(
        MarkItDownServiceServicer(), grpc_server
    )
    grpc_server.add_insecure_port(bind_address)
    grpc_server.start()
    return grpc_server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MarkItDown gRPC server.")
    parser.add_argument(
        "--bind-address",
        default="127.0.0.1:50051",
        help="Address the gRPC server listens on.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum worker threads for handling requests.",
    )
    args = parser.parse_args()

    grpc_server = serve(bind_address=args.bind_address, max_workers=args.max_workers)
    grpc_server.wait_for_termination()
