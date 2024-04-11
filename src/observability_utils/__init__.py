from opentelemetry.baggage import get_baggage, set_baggage
from opentelemetry.trace import (
    SpanKind,
    Tracer,
    get_current_span,
    get_tracer_provider,
)

from ._version import __version__
from .tracing import (
    Context,
    get_trace_context,
    get_tracer,
    instrument_fastapi_app,
    propagate_context_in_stomp_headers,
    retrieve_context_from_stomp_headers,
    set_console_exporter,
    setup_tracing,
)

__all__ = [
    "__version__",
    "Tracer",
    "SpanKind",
    "Context",
    "get_tracer_provider",
    "get_current_span",
    "get_baggage",
    "set_baggage",
    "instrument_fastapi_app",
    "setup_tracing",
    "set_console_exporter",
    "get_trace_context",
    "get_tracer",
    "propagate_context_in_stomp_headers",
    "retrieve_context_from_stomp_headers",
]
