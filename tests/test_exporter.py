from typing import cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from observability_utils.tracing import (
    add_span_attributes,
    asserting_span_exporter,
    get_tracer,
    start_as_current_span,
)

NAME = "test_service"


@pytest.fixture()
def exporter(trace_provider: TracerProvider):
    processor = cast(
        BatchSpanProcessor, trace_provider._active_span_processor._span_processors[1]
    )
    return processor.span_exporter


def test_function_and_param_name_from_decorator_captured(exporter):
    @start_as_current_span(
        get_tracer(NAME),
        "param",
    )
    def decoratee(param):
        pass

    with asserting_span_exporter(exporter, "decoratee", "param"):
        decoratee(1)
        exporter.force_flush()

    span = exporter.top_span.result(timeout=0.0)
    assert span.attributes
    assert span.attributes == {"param": 1}


def test_param_from_add_span_attributes_also_captured(exporter):
    @start_as_current_span(
        get_tracer(NAME),
        "param1",
    )
    def decoratee(param1):
        add_span_attributes({"other_param": 45})
        ...

    with asserting_span_exporter(exporter, "decoratee", "param1", "other_param"):
        decoratee(1)
        exporter.force_flush()

    span = exporter.top_span.result(timeout=0.0)
    assert span.attributes
    assert span.attributes["param1"] == 1
    assert span.attributes["other_param"] == 45
    assert len(span.attributes.keys()) == 2


def test_param_from_add_span_attributes_only_captured(exporter):
    @start_as_current_span(
        get_tracer(NAME),
    )
    def decoratee(param):
        add_span_attributes({"added_param": 45})
        add_span_attributes({"none_param": None})  # type: ignore

    with asserting_span_exporter(exporter, "decoratee", "added_param", "none_param"):
        decoratee(1)
        exporter.force_flush()

    span = exporter.top_span.result(timeout=0.0)
    assert span.attributes
    assert span.attributes["added_param"] == 45
    assert span.attributes["none_param"] == "None"
    assert len(span.attributes.keys()) == 2
