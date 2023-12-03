import polars as pl
from polars.type_aliases import IntoExpr  # noqa: F401
from polars.utils.udfs import _get_shared_lib_location

# boilerplate needed to inform polars of the location of binary wheel.
lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("casing")
class Casing:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def pascal_case(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="pascal_case",
            is_elementwise=True,
        )

    def snake_case(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="snake_case",
            is_elementwise=True,
        )
