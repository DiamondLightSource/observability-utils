import os
from collections.abc import Iterator
from typing import cast
from unittest.mock import patch

import opentelemetry.trace
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from observability_utils.tracing import (
    JsonObjectSpanExporter,
    set_console_exporter,
    setup_tracing,
)

FAKE_INSTRUMENT = "i600"
UNKNOWN_INSTRUMENT = "Unknown"
INSTRUMENT_KEY = "service.instrument"


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
def fake_instrument() -> str:
    return FAKE_INSTRUMENT


@pytest.fixture(scope="session", autouse=True)
def unknown_instrument() -> str:
    return UNKNOWN_INSTRUMENT


@pytest.fixture(scope="session", autouse=True)
def instrument_key() -> str:
    return INSTRUMENT_KEY


@pytest.fixture(autouse=True)
def trace_provider(request: pytest.FixtureRequest) -> Iterator[TracerProvider]:
    """Must be called by each test that needs a TracerProvider as it creates one before
    and then removes it existing one at the end of such test."""
    if hasattr(request, "param"):
        with patch.dict(
            os.environ,
            {
                "BEAMLINE": (
                    request.param["beamline"]
                    if "beamline" in request.param
                    else UNKNOWN_INSTRUMENT
                )
            },
            clear=True,
        ):
            setup_tracing("tests", request.param["export"])
    else:
        setup_tracing("tests", False)
    provider = cast(TracerProvider, opentelemetry.trace.get_tracer_provider())
    # Use SimpleSpanProcessor to keep tests quick
    set_console_exporter()
    provider.add_span_processor(SimpleSpanProcessor(JsonObjectSpanExporter()))
    yield provider
    opentelemetry.trace._TRACER_PROVIDER_SET_ONCE._done = False
    opentelemetry.trace._TRACER_PROVIDER = None
