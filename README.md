[![CI](https://github.com/DiamondLightSource/observability-utils/actions/workflows/ci.yml/badge.svg)](https://github.com/DiamondLightSource/observability-utils/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DiamondLightSource/observability-utils/branch/main/graph/badge.svg)](https://codecov.io/gh/DiamondLightSource/observability-utils)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# observability_utils

A set of functions to reduce the boilerplate required to add observability to your service of module

At the moment there are not a great amount of util functions provided, but it is hoped that, as more observabilty code is added to our services and modules, this becomes the standard place to put code snippets that are commonly useful. 

In the initial version the following utils are provided:
* ```setup_tracing(name)``` - Sets up basic tracing using  a standardised naming convebstion so that the application is easily identifiable in visualisation tools.
* ```instrument_fastapi_app(app, name)``` - Sets up basic tracing as above and then turns on automated tracing of FastAPI calls.
* ```set_console_exporter()``` - Turns on output of the capturesd traces in a local console/terminal to allow viewing of it without the need for an observability backend such as Jaeger or Promethus. Useful for debugging and testing.
* ```get_tracer(name)``` - Retrieves the currently active Tracer object and labels is using a standard naming convention so that traces it produces are consistent across applications.
* ```get_trace_context()`` - Retrives the current trace context (this is just a more clearly named version of the library function).
* ```propagate_context_in_stomp_headers(headers, context)``` - Simplfies the propagation of the Tracing Context between services that support STOMP communication over a message bus.
* ```retrieve_context_from_stomp_headers(frame)``` - Simplifies th reception of the Tracing Context by services that support STOMP communication over a message bus.

Source          | <https://github.com/DiamondLightSource/observability-utils>
:---:           | :---:
Releases        | <https://github.com/DiamondLightSource/observability-utils/releases>

Usage examples:

```python
from fastapi import FastAPI
from observability_utils import instrument_fastapi_app, get_tracer

app = FastAPI(
    docs_url="/docs",
    on_shutdown=[teardown_handler],
    title="My Rest App",
    lifespan=lifespan,
    version=REST_API_VERSION,
)

instrument_fastapi_app(app, "my_rest_app")

TRACER = get_tracer("my_rest_app")

@TRACER.start_as_current_span("my_func",  kind=SpanKind.CLIENT)
def my_func():
    #function body
```


