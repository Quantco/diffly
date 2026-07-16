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

Presets come in two families, each with its own module and default set:

- :mod:`diffly.metrics.change` describes the *change* between numeric columns by
  aggregating over ``right - left``.
- :mod:`diffly.metrics.data` describes the left and right datasets *individually*,
  so you can see how a change affects the data.

The change default set is exposed as :data:`DEFAULT_METRICS`.

.. autodata:: MetricFn
   :no-value:

.. autoclass:: Metric

.. autodata:: DEFAULT_METRICS
   :no-value:

Change metrics
==============

.. currentmodule:: diffly.metrics.change

Metrics that describe the change between numeric columns by aggregating over
``right - left``.

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

.. autodata:: DEFAULT_CHANGE_METRICS
   :no-value:

Data metrics
============

.. currentmodule:: diffly.metrics.data

Metrics that describe the left and right datasets individually, so you can
understand how a change affects the data.

.. autosummary::
   :toctree: _gen/

   null_fraction_data
   mean_data
   median_data
   min_data
   max_data
   std_data

.. autodata:: DEFAULT_DATA_METRICS
   :no-value:
