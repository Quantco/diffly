=======
Metrics
=======

.. currentmodule:: diffly.metrics

Metrics are scalar aggregations computed per column when generating a
:meth:`~diffly.comparison.DataFrameComparison.summary`. Pass them via the
``metrics`` argument as a mapping from display label to a metric. There are two
families:

- A :class:`ChangeMetric` describes the *change* between the two sides. Its
  callable takes ``(left_expr, right_expr)`` and aggregates over the difference
  (e.g. the mean delta). It is rendered as a column in the "Columns" table.
- A :class:`DataMetric` describes each dataset *individually*. Its callable takes
  a single column expression and is evaluated on the left and right side
  separately (e.g. the fraction of null entries). It is rendered in the
  "Data Inspection" section, showing the left and right value side by side.

A bare callable is resolved by its arity: a two-argument callable becomes a
:class:`ChangeMetric` (computed for numerical columns only), a one-argument
callable becomes a :class:`DataMetric` (computed for all columns). To target a
different set of columns, construct the metric explicitly with a column selector,
e.g. ``ChangeMetric(fn, selector=cs.all())`` or
``DataMetric(fn, selector=cs.boolean())``.

Presets come in two families, each with its own module and default set:

- :mod:`diffly.metrics.change` describes the *change* between numeric columns by
  aggregating over ``right - left``.
- :mod:`diffly.metrics.data` describes the left and right datasets *individually*,
  so you can see how a change affects the data.

The preset default sets are :data:`~diffly.metrics.change.DEFAULT_CHANGE_METRICS`
and :data:`~diffly.metrics.data.DEFAULT_DATA_METRICS`.

.. autodata:: ChangeMetricFn
   :no-value:

.. autodata:: DataMetricFn
   :no-value:

.. autoclass:: ChangeMetric

.. autoclass:: DataMetric

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

   null_fraction

.. autodata:: DEFAULT_DATA_METRICS
   :no-value:
