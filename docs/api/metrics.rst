=======
Metrics
=======

.. currentmodule:: diffly.metrics

Metrics are scalar aggregations computed per numerical column when generating a
:meth:`~diffly.comparison.DataFrameComparison.summary`. Pass them via the
``metrics`` argument as a mapping from display label to a :data:`MetricFn`
callable. :mod:`diffly.metrics` ships a set of presets; you can also supply
your own callable ``(left_expr, right_expr) -> pl.Expr``.

.. autodata:: MetricFn
   :no-value:

Presets
=======

.. autosummary::
   :toctree: _gen/

   mean
   median
   min
   max
   std
   mean_absolute_deviation
   mean_relative_deviation
   quantile
