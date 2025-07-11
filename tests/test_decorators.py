import re

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation import get_current_span

from observability_utils.tracing import get_tracer
from observability_utils.tracing.decorators import (
    _attr_path,
    _attr_value_of,
    _obj_of,
    start_as_current_span,
    use_propagated_context,
)

NAME = "test_service"


def test_obj_of():
    n = "aname"
    assert _obj_of(n) == n
    n = "a.name"
    assert _obj_of(n) == "a"
    n = "a.multi.part.name"
    assert _obj_of(n) == "a"


def test_attr_value_of():
    one = Node()
    two = Node(a=5, n=one)
    three = Node(a=78, b="stuff", n=two)

    assert _attr_value_of(three) == three
    assert _attr_value_of(three, "a") == 78
    assert _attr_value_of(three, "b") == "stuff"
    assert _attr_value_of(three, "node") == two
    assert _attr_value_of(three, "node.a") == 5
    assert _attr_value_of(three, "node.b") == "blank"
    assert _attr_value_of(three, "node.node") == one
    assert _attr_value_of(three, "node.node.a") == 0
    assert _attr_value_of(three, "node.node.b") == "blank"
    assert _attr_value_of(three, "node.node.node") is None


def test_attr_path():
    n = "aname"
    assert _attr_path(n) is None
    n = "a.name"
    assert _attr_path(n) == "name"
    n = "a.multi.part.name"
    assert _attr_path(n) == "multi.part.name"


def test_use_propagated_context():
    trace_id = 164087499969805359226274920435881701965
    span_id = 1836941681154040884
    traceparent_from_ids = "00-7b721a5c2fa681d48b9815c92fe1724d-197e20b9fa4ba434-01"

    sc = get_current_span().get_span_context()
    assert sc.trace_id != trace_id
    assert sc.span_id != span_id

    @use_propagated_context
    def decoratee():
        sc = get_current_span().get_span_context()
        assert sc.trace_id == trace_id
        assert sc.span_id == span_id

    carrier = {"traceparent": traceparent_from_ids}
    decoratee(carrier)


def test_start_as_current_span(trace_provider: TracerProvider):
    @start_as_current_span(
        get_tracer(NAME),
        "param1",
        "param2.a",
        "param2.b",
        "param2.node",
        "param2.node.a",
        "param2.node.b",
        "param2.node.node",
    )
    def decoratee(param1, param2):
        attributes = get_current_span().attributes  # type: ignore
        assert attributes
        assert "param1" in attributes
        assert attributes["param1"] == 2
        assert ("param2.a") in attributes
        assert attributes["param2.a"] == 5
        assert ("param2.b") in attributes
        assert attributes["param2.b"] == "blank"
        assert ("param2.node") in attributes
        assert re.fullmatch(
            "<test_decorators.Node object at .*>", str(attributes["param2.node"])
        )
        assert ("param2.node.a") in attributes
        assert attributes["param2.node.a"] == 0
        assert ("param2.node.b") in attributes
        assert attributes["param2.node.b"] == "blank"
        assert ("param2.node.node") in attributes
        assert attributes["param2.node.node"] == "None"

    one = Node()
    two = Node(a=5, n=one)
    decoratee(2, two)


def test_start_as_current_span_capturing_args(trace_provider: TracerProvider):
    @start_as_current_span(
        get_tracer(NAME),
        "args",
    )
    def decoratee(param1, param2):
        attributes = get_current_span().attributes  # type: ignore
        assert attributes
        assert "args" in attributes
        assert re.fullmatch(
            r"\(2,\s<test_decorators.Node object at .*>\)",
            attributes["args"],
        )

    one = Node()
    decoratee(2, one)


def test_start_as_current_span_capturing_kwargs(trace_provider: TracerProvider):
    @start_as_current_span(
        get_tracer(NAME),
        "kwargs",
    )
    def decoratee(param1, param2):
        attributes = get_current_span().attributes  # type: ignore
        assert attributes
        assert "kwargs" in attributes
        assert re.fullmatch(
            r"{'param2':\s<test_decorators.Node object at .*>,\s'param1':\s2}",
            attributes["kwargs"],
        )

    one = Node()
    decoratee(param2=one, param1=2)


class Node:
    a: int
    b: str
    node: object

    def __init__(self, a: int = 0, b: str = "blank", n: object = None) -> None:
        self.a = a
        self.b = b
        self.node = n
