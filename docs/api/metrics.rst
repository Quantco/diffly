=======
Metrics
=======

.. currentmodule:: diffly.metrics

Metrics are scalar aggregations computed per column when generating a
:meth:`~diffly.comparison.DataFrameComparison.summary`. Pass them via the
``metrics`` argument as a mapping from display label to a :data:`MetricFn`
callable. :mod:`diffly.metrics` ships a set of presets; you can also supply
your own callable ``(left_expr, right_expr) -> pl.Expr``.

A bare callable is only computed for numerical columns. To target a different
set of columns, wrap it in a :class:`Metric` with a column selector, e.g.
``Metric(fn, selector=cs.all())``, ``Metric(fn, selector=cs.boolean())``, or
``Metric(fn, selector=cs.by_name("my_column_name"))``.

.. autodata:: MetricFn
   :no-value:

.. autoclass:: Metric

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
   null_fraction_change
   quantile
