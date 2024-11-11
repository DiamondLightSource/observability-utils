[![CI](https://github.com/DiamondLightSource/observability-utils/actions/workflows/ci.yml/badge.svg)](https://github.com/DiamondLightSource/observability-utils/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DiamondLightSource/observability-utils/branch/main/graph/badge.svg)](https://codecov.io/gh/DiamondLightSource/observability-utils)
[![PyPI](https://img.shields.io/pypi/v/observability-utils.svg)](https://pypi.org/project/observability-utils)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# observability_utils

A set of functions and decorators to reduce the boilerplate required to add OpenTelemetry based observability to your Python service or module.

The decorators allow spans to be initialised which automatically capture identified parameters of the decorated method as Span Attributes. The context propagation helpers provide a pair of standard functions that can be used as parameters to streamline addition of this functionality.

In the initial version the following utils are provided:

* ```setup_tracing(name)``` - Sets up basic tracing using  a standardised naming convebstion so that the application is easily identifiable in visualisation tools.
* ```set_console_exporter()``` - Turns on output of the capturesd traces in a local console/terminal to allow viewing of it without the need for an observability backend such as Jaeger or Promethus. Useful for debugging and testing.
* ```get_tracer(name)``` - Retrieves the currently active Tracer object and labels is using a standard naming convention so that traces it produces are consistent across applications.
* ```get_context_propagator``` - Retrieves the current observability context info in a form suitable for propagating.
* ```propagate_context_in_stomp_headers(headers, context)``` - Simplfies the propagation of the Tracing Context between services that support STOMP communication over a message bus.
* ```retrieve_context_from_stomp_headers(frame)``` - Simplifies th reception of the Tracing Context by services that support STOMP communication over a message bus.
* ```add_span_attributes``` - Simplifies the addition of named attributes to the current span.
* ```JsonObjectSpanExporter``` - A custom SpanExporter that allows the span content to be examined for use
in tests.
* ```asserting_span_exporter``` - A contextmanager that makes use of JsonObjectSpanExporter to allow
functions under test to be checked by enclosing them in a with block.

Source          | <https://github.com/DiamondLightSource/observability-utils>
:---:           | :---:
PyPI            | `pip install observability-utils`
Releases        | <https://github.com/DiamondLightSource/observability-utils/releases>

Usage examples:

```python
from fastapi import FastAPI
from observability_utils.decorators import start_as_current_span
from observability_utils.tracing import setup_tracing, get_tracer
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_tracing("my_rest_app")

app = FastAPI(
    docs_url="/docs",
    on_shutdown=[teardown_handler],
    title="My Rest App",
    lifespan=lifespan,
    version=REST_API_VERSION,
)

FastAPIInstrumentor().instrument_app(app)

TRACER = get_tracer("my_rest_app")

start_as_current_span(TRACER, "fruit", "fruit.colour", "amount")
def my_func(fruit : Fruit = "apple", amount : int = 0):
    #function body
```
