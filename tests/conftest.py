import os
from typing import cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import get_tracer_provider

from observability_utils.tracing import JsonObjectSpanExporter, setup_tracing

# Prevent pytest from catching exceptions when debugging in vscode so that break on
# exception works correctly (see: https://github.com/pytest-dev/pytest/issues/7409)
if os.getenv("PYTEST_RAISE", "0") == "1":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value


@pytest.fixture(scope="session", autouse=True)
def trace_provider() -> TracerProvider:
    setup_tracing("tests", False)
    provider = cast(TracerProvider, get_tracer_provider())
    # Use SimpleSpanProcessor to keep tests quick
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    provider.add_span_processor(SimpleSpanProcessor(JsonObjectSpanExporter()))
    return provider
