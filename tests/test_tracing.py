from typing import cast

import pytest
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import SpanKind, get_current_span, get_tracer_provider
from opentelemetry.trace.span import format_span_id, format_trace_id
from stomp.utils import Frame

from observability_utils.tracing import (
    add_span_attributes,
    get_context_propagator,
    get_tracer,
    propagate_context_in_stomp_headers,
    retrieve_context_from_stomp_headers,
    set_console_exporter,
    setup_tracing,
)

TRACEPARENT_KEY = "traceparent"
NAME = "test_service"
PREFIX = "opentelemetry.instrumentation."
NAME_KEY = "service.name"


@pytest.fixture()
def init_tracing():
    setup_tracing(NAME, False)
    set_console_exporter()


def test_setup_tracing_with_console_exporter(init_tracing):
    tp = cast(TracerProvider, get_tracer_provider())
    sp = tp._active_span_processor._span_processors[0]

    assert tp.resource.attributes[NAME_KEY] == NAME
    assert isinstance(sp, BatchSpanProcessor)
    assert isinstance(sp.span_exporter, ConsoleSpanExporter)


def test_get_context_propagator(init_tracing):
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


def test_propagate_context_in_stomp_headers(init_tracing):
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
    assert "x" in attributes
    assert attributes["x"] == 4


def test_retrieve_context_from_stomp_headers(init_tracing):
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
    assert "x" in attributes
    assert attributes["x"] == 4
