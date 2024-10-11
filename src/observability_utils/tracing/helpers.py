"""Convenience functions to simplify the use of OTEL standard functions and
context proagation setup.
"""

from typing import Any, cast

from opentelemetry.context import Context, get_current
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.propagate import get_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.trace import (
    Tracer,
    get_current_span,
    get_tracer_provider,
    set_tracer_provider,
)
from opentelemetry.util.types import AttributeValue
from stomp.utils import Frame


def setup_tracing(name: str, with_otlp_export: bool = True) -> None:
    """Sets up Open Telemetry tracing. This should be called at the start of your
    app to establish the global TracerProvider and the name of the application
    that the generated traces belong to. A SpanProcessor that will export its trace
    information using the OTLP Open Telemetry protocol is then added unless you
    specify otherwise. You will then need to instrument the rest of your code using
    e.g. the get_tracer_provider call to hook into the apps OTEL infrastructure
    when creating new SpanProcessors or setting up manual Span generation. N.B. you
    will need to call this before you use library specific functions like the
    FastAPI instrumentor.

    Args:
        name (str): The name to be used in spans to refer to the application.
        with_otlp_export (bool): Indicates whether an OTLP Exporter shoudld be set up
    """
    resource = Resource(attributes={"service.name": name})
    provider = TracerProvider(resource=resource)
    if with_otlp_export:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    set_tracer_provider(provider)


def set_console_exporter() -> None:
    """Add a SpanProcessor to route the tracing messages to the inbuilt console
    exporter so that the raw trace JSON is printed out there.
    """
    provider = cast(TracerProvider, get_tracer_provider())
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))


def get_tracer(name: str) -> Tracer:
    """A wrapper around the library function to establish the recommended naming
    convention for a module's Tracer when getting it.

    Args:
        name (str): The name to be used by the tracer to refer to the application along
                    with the standard prefix.

    Returns:
        Tracer: The currently active tracer object.
    """
    return get_tracer_provider().get_tracer("opentelemetry.instrumentation." + name)


def get_trace_context() -> Context:
    """Somewhat redundant but the fn name "get_current" is pretty ambiguous.

    Returns:
        Context: The retrieved Trace context object for the current trace.
    """
    return get_current()


def get_context_propagator() -> dict[str, Any]:
    """Retrieve the current observability context propagation details and return them in
    a dictionary ready to be passed to other processes.

    Return: A dictionary containinng the Current Span Id and any Baggage currently set
    in the current observability context.
    """
    carr = {}
    get_global_textmap().inject(carr)
    return carr


def propagate_context_in_stomp_headers(
    headers: dict[str, Any], context: Context | None = None
) -> None:
    """Utility to propagate Observability context via STOMP message header.

    Args:
        headers (Dict[str, Any]): The STOMP headers to add the context to
        context (Optional[Context]): The context object to add to the headers; if none
                                     is specified the current active one will be used.
    """
    get_global_textmap().inject(headers, context)


def retrieve_context_from_stomp_headers(frame: Frame) -> Context:
    """Utility to extract Observability context from the headers of a STOMP message.

    Args:
        frame (Frame): The message frame from whose headers the context should be
                       retrieved

    Returns:
        Context: The extracted  context.
    """
    return get_global_textmap().extract(carrier=frame.headers)


def add_span_attributes(attributes: dict[str, AttributeValue]) -> None:
    """Inserts the specified attributes into the current Span

    Args:
        attributes: the dict of attributes to add
    """
    get_current_span().set_attributes(attributes)
