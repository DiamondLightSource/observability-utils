from typing import cast

import pytest
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.trace import SpanKind, get_current_span
from opentelemetry.trace.span import format_span_id, format_trace_id
from stomp.utils import Frame

from conftest import FAKE_INSTRUMENT
from observability_utils.tracing import (
    JsonObjectSpanExporter,
    add_span_attributes,
    get_context_propagator,
    get_tracer,
    propagate_context_in_stomp_headers,
    retrieve_context_from_stomp_headers,
)

TRACEPARENT_KEY = "traceparent"
NAME = "tests"
PREFIX = "opentelemetry.instrumentation."
NAME_KEY = "service.name"
CONSOLE_PROCESSOR_INDEX = 0
JSON_PROCESSOR_INDEX = 1


@pytest.mark.parametrize("trace_provider", [{"export": False}], indirect=True)
def test_tracing_setup_with_console_exporter(
    trace_provider: TracerProvider,
    instrument_key: str,
    unknown_instrument: str,
):
    sp = trace_provider._active_span_processor._span_processors[CONSOLE_PROCESSOR_INDEX]

    assert trace_provider.resource.attributes[NAME_KEY] == NAME
    assert trace_provider.resource.attributes[instrument_key] == unknown_instrument
    assert isinstance(sp, SimpleSpanProcessor)
    assert isinstance(sp.span_exporter, ConsoleSpanExporter)


@pytest.mark.parametrize(
    "trace_provider", [{"beamline": FAKE_INSTRUMENT, "export": False}], indirect=True
)
def test_tracing_setup_with_json_exporter(
    trace_provider: TracerProvider, instrument_key: str, fake_instrument: str
):
    sp = trace_provider._active_span_processor._span_processors[JSON_PROCESSOR_INDEX]

    assert trace_provider.resource.attributes[NAME_KEY] == NAME
    assert trace_provider.resource.attributes[instrument_key] == fake_instrument
    assert isinstance(sp, SimpleSpanProcessor)
    assert isinstance(sp.span_exporter, JsonObjectSpanExporter)


@pytest.mark.parametrize("trace_provider", [{"export": True}], indirect=True)
def test_span_exporter_selection(trace_provider: TracerProvider):
    sp = trace_provider._active_span_processor._span_processors[CONSOLE_PROCESSOR_INDEX]
    assert isinstance(sp, BatchSpanProcessor)
    assert isinstance(sp.span_exporter, OTLPSpanExporter)


def test_get_context_propagator():
    tr = cast(Tracer, get_tracer(NAME))
    with tr.start_as_current_span("test"):
        span_context = get_current_span().get_span_context()
        traceparent_string = (
            f"00-{format_trace_id(span_context.trace_id)}-"
            f"{format_span_id(span_context.span_id)}-"
            f"{span_context.trace_flags:02x}"
        )
        carrier = get_context_propagator()
    assert carrier[TRACEPARENT_KEY] == traceparent_string


def test_propagate_context_in_stomp_headers():
    headers = {}
    tr = cast(Tracer, get_tracer(NAME))
    with tr.start_as_current_span("test") as span:
        span_context = get_current_span().get_span_context()
        traceparent_string = (
            f"00-{format_trace_id(span_context.trace_id)}-"
            f"{format_span_id(span_context.span_id)}-"
            f"{span_context.trace_flags:02x}"
        )
        add_span_attributes({"x": 4})
        propagate_context_in_stomp_headers(headers)
    assert tr.instrumentation_info.name == PREFIX + NAME
    assert headers[TRACEPARENT_KEY] == traceparent_string
    attributes = span.attributes  # type: ignore
    assert attributes
    assert "x" in attributes
    assert attributes["x"] == 4


def test_retrieve_context_from_stomp_headers():
    trace_id = 128912953781416571737941496506421356054
    traceparent_string = "00-60fbbb56a2b44e1cd8e7363fb4482616-cebfdbc55ee30d3f-01"
    frame = Frame(cmd=None, headers={TRACEPARENT_KEY: traceparent_string})

    tr = cast(Tracer, get_tracer(NAME))
    with tr.start_as_current_span(
        "on_message",
        retrieve_context_from_stomp_headers(frame),
        SpanKind.CONSUMER,
    ) as span:
        add_span_attributes({"x": 4})

    assert tr.instrumentation_info.name == PREFIX + NAME
    assert span.get_span_context().trace_id == trace_id
    attributes = span.attributes  # type: ignore
    assert attributes
    assert "x" in attributes
    assert attributes["x"] == 4
