from collections.abc import Callable, Sequence
from concurrent.futures import Future
from contextlib import contextmanager
from typing import IO

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import (
    SpanExporter,
    SpanExportResult,
)


class JsonObjectSpanExporter(SpanExporter):
    """A custom exporter to allow open telemetry spans to be examined
    and verified during normal testing.
    The first span that is entered is cached in a Future for retrieval in test code.
    The span is effectively cleared when it is read, allowing a single instance of the
    SpanExporter to be reused for multiple tests.
    It is recommended to use with a SimpleSpanProcessor.
    """

    def __init__(
        self,
        service_name: str | None = "Test",
        out: IO | None = None,
        formatter: Callable[[ReadableSpan], str] | None = None,
    ):
        self.service_name = service_name
        self.top_span: Future | None = None

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if self.top_span is None or self.top_span.done():
            self.top_span = Future()
            self.top_span.set_result(spans[-1])
        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


@contextmanager
def asserting_span_exporter(
    exporter: JsonObjectSpanExporter, func_name: str, *span_args: str
):
    """Use as a with block around the function under test if decorated with
    start_as_current_span to check span creation and content.

    params:
        func_name: The name of the function being tested
        span_args: The arguments specified in its start_as_current_span decorator
        or added using Add_span_attributes.
    """
    yield
    if exporter.top_span is not None:
        span = exporter.top_span.result(timeout=5.0)
        assert span.name == func_name
        for param in span_args:
            assert param in span.attributes
