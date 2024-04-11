from typing import cast

from fastapi import FastAPI
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import SpanKind, get_current_span, get_tracer_provider
from opentelemetry.trace.span import format_span_id, format_trace_id
from stomp.utils import Frame

from observability_utils import (
    get_tracer,
    instrument_fastapi_app,
    propagate_context_in_stomp_headers,
    retrieve_context_from_stomp_headers,
    set_console_exporter,
    setup_tracing,
)

TRACEPARENT_KEY = "traceparent"
NAME = "test_service"
PREFIX = "opentelemetry.instrumentation."
NAME_KEY = "service.name"


def test_setup_tracing():
    setup_tracing(NAME)
    tp = cast(TracerProvider, get_tracer_provider())
    sp = tp._active_span_processor._span_processors[0]

    assert tp.resource.attributes[NAME_KEY] == NAME
    assert isinstance(sp, BatchSpanProcessor)
    assert isinstance(sp.span_exporter, OTLPSpanExporter)


def test_instrument_fastapi_app():
    app = FastAPI(
        docs_url="/docs",
        title="Test",
    )
    instrument_fastapi_app(app, NAME)

    tp = cast(TracerProvider, get_tracer_provider())
    sp = tp._active_span_processor._span_processors[0]

    assert tp.resource.attributes[NAME_KEY] == NAME
    assert isinstance(sp, BatchSpanProcessor)
    assert isinstance(sp.span_exporter, OTLPSpanExporter)
    assert app.user_middleware[0].cls == OpenTelemetryMiddleware


def test_set_console_exporter():
    set_console_exporter()
    sp = cast(
        TracerProvider, get_tracer_provider()
    )._active_span_processor._span_processors[1]

    assert isinstance(sp, BatchSpanProcessor)
    assert isinstance(sp.span_exporter, ConsoleSpanExporter)


def test_propagate_context_in_stomp_headers():
    headers = {}
    setup_tracing(NAME)
    tr = cast(Tracer, get_tracer(NAME))
    with tr.start_as_current_span("test") as span:
        span.set_attribute("x", 4)
        span_context = get_current_span().get_span_context()
        traceparent_string = (
            f"00-{format_trace_id(span_context.trace_id)}-"
            f"{format_span_id(span_context.span_id)}-"
            f"{span_context.trace_flags:02x}"
        )
        propagate_context_in_stomp_headers(headers)

    assert tr.instrumentation_info.name == PREFIX + NAME
    assert headers[TRACEPARENT_KEY] == traceparent_string


def test_retrieve_context_from_stomp_headers():
    trace_id = 128912953781416571737941496506421356054
    traceparent_string = "00-60fbbb56a2b44e1cd8e7363fb4482616-cebfdbc55ee30d3f-01"
    frame = Frame(cmd=None, headers={TRACEPARENT_KEY: traceparent_string})

    setup_tracing(NAME)
    tr = cast(Tracer, get_tracer(NAME))
    with tr.start_as_current_span(
        "on_message", retrieve_context_from_stomp_headers(frame), SpanKind.CONSUMER
    ) as span:
        span.set_attribute("x", 4)

    assert tr.instrumentation_info.name == PREFIX + NAME
    assert span.get_span_context().trace_id == trace_id
