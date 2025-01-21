"""A set of decorators and associated functions to smooth the experience of
using the raw opentelemetry decorators and context propagation functions"""

import functools
from _collections_abc import Callable
from inspect import signature
from typing import Any, Concatenate, ParamSpec, TypeVar

from opentelemetry.context import attach
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace import SpanKind, Tracer

T = TypeVar("T")
P = ParamSpec("P")
_SEPARATOR = "."


def _obj_of(param: str) -> str:
    """Checks to see if the param name is of the form  x.y(.?) returning x if it is
    or the unmodified param name itself if not

    Args:
        param: A string indicating a required object(s)/attibute combination relating to
            a function paramter of the decorated method,acceptable forms are x, x.y,
            x.y.z.a.etc. to an arbitrary level of depth where x is the function
            parameter in question.

    Returns: The text up to the first dot in param, if there is one, or param
    """
    return param.partition(_SEPARATOR)[0] if _SEPARATOR in param else param


def _attr_value_of(obj: Any, attr: str | None = None) -> Any:
    """Parses and obtains the value of the specified attribute attr on the object obj,
    where obj is a paramter of the decorated method. N.B., attr may be of the form
    x.y.z, i.e. indicating an attribute of a member object of obj at an arbitrary
    depth. In this case the while loop will parse attr, working its way down the object
    tree until the last one is reached and then rerieve the ramaining attribute value.

    Args:
        obj: The name of a parameter in the decorated method relating to an attribute
            that should be added to the current span. Defaults to None
        attr: The name of the required attribute of obj, or a dot separated string
            indicating an attribute of a sub object of obj to an arbitrary depth.

    Returns: The value of the identified attribute if attr is not None, otherwise obj

    """
    if attr is not None:
        while _SEPARATOR in attr:
            obj = getattr(obj, attr.partition(_SEPARATOR)[0])
            attr = attr.partition(_SEPARATOR)[2]
        return getattr(obj, attr)
    return obj


def _attr_path(param: str) -> str | None:
    """The complement to obj_of, returns the section of the arg string after the first
    dot separator correspoding to the path to the required attribute, or None of there
    is no separator

    Args:
        param: A string indicating a required object(s)/attibute combination relating to
            a function paramter of the decorated method,acceptable forms are x, x.y,
            x.y.z.a.etc. to an arbitrary level of depth where x is the function
            parameter in question.

    Returns: The text after the first dot in param, there is one, or None

    """
    return param.partition(_SEPARATOR)[2] if _SEPARATOR in param else None


def start_as_current_span(tracer: Tracer, *span_args: str):
    """Decorator to wrap the opentelementry tracer function of the same name. It
    automatically add the specified function parameters to the span attributes.

    Args:
        tracer: The OpenTelemetry tracer to which the required Span should be attached
        *span_args: The set of attribute identifiers relating to the decorated method's
            parameters, whose values should be added to the Span as Attributes

    Returns: The decorator function

    """

    def inner(func: Callable[P, T]) -> Callable[P, T]:
        """Return the decorated function populated with the calculated arguments.

        Args:
            func: The function to be decorated

        Returns: The decorated function, pre-poplated with span information
        """

        # first get the ordered list of potential parameters
        sig_arg_order = list(signature(func).parameters)

        def arg_value(span_arg: str, *args, **kwargs) -> str | bool | int | float:
            """Locates the span_arg in either *args or **kwargs and returns its value

            Args:
                span_arg: The name of the object/attrribute combination, the value of
                which is to be added to the Span
                *args: the args of the decorated function
                **kwargs: the kwargs of the decorated function

            Returns:
                The value of the requested argument, stringified if it is not an int,
                bool, or float
            """
            # handle requests for set of params first
            if span_arg in ("args"):
                return str(args)
            if span_arg in ("kwargs"):
                return str(kwargs)

            arg_obj_name = _obj_of(span_arg)
            arg_attr_name = _attr_path(span_arg)
            index_from_sig = sig_arg_order.index(arg_obj_name)
            value = (
                _attr_value_of(args[index_from_sig], arg_attr_name)
                if len(args) > index_from_sig
                else _attr_value_of(kwargs[arg_obj_name], arg_attr_name)
                if arg_obj_name in kwargs
                else None
            )
            return value if isinstance(value, str | bool | int | float) else str(value)

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            """Build the attributes to pass to the underlying decorator from the
            function parameters and then wraps the target function invocation with the
            corresponding raw OpenTelemetry decorator

            Args:
                *args: The args of the function to be decorated
                **kwargs: The kwargs of the function to be decorated

            Returns:
                The output of the decorated function
            """

            attributes = {
                span_arg: arg_value(span_arg, *args, **kwargs) for span_arg in span_args
            }

            with tracer.start_as_current_span(
                func.__name__, attributes=attributes, kind=SpanKind.SERVER
            ):
                return func(*args, **kwargs)

        return wrapper

    return inner


def use_propagated_context(
    func: Callable[P, T],
) -> Callable[Concatenate[dict[str, Any] | None, P], T]:
    """Retrieves the propagated context information from the carrier param which is
    concatenated onto the target function and injects that into the local observablity
    context.

    Args:
        func: The function to be decorated

    Returns:
        A wrapped version the function which accepts an extra pre-pended parameter to
        receive the propagated context.
    """

    @functools.wraps(func)
    def wrapper(carrier: dict[str, Any] | None, *args: P.args, **kwargs: P.kwargs) -> T:
        """Receives the propagated observability context as carrier and insert this into
        the locally active one before calling the decorated function

        Args:
            carrier: The observability context propagator
            *args: The args of the function to be decorated
            **kwargs: The kwargs of the function to be decorated

        Returns:
            The output of the decorated function
        """
        if carrier:
            ctx = get_global_textmap().extract(carrier)
            attach(ctx)
        return func(*args, **kwargs)

    return wrapper
