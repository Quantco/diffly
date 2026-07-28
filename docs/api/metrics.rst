=======
Metrics
=======

.. currentmodule:: diffly.metrics

Metrics are scalar aggregations computed per column when generating a
:meth:`~diffly.comparison.DataFrameComparison.summary`. There are two families,
each passed via its own argument:

- A :class:`~diffly.metrics.change.ChangeMetric`, passed via the ``change_metrics``
  argument, describes the *change* between the two sides. Its callable takes
  ``(left_expr, right_expr)`` and aggregates over the difference (e.g. the mean
  delta). It is rendered as a column in the "Columns" table.
- A :class:`~diffly.metrics.data.DataMetric`, passed via the ``data_metrics``
  argument, describes each dataset *individually*. Its callable takes a single
  column expression and is evaluated on the left and right side separately (e.g. the
  fraction of null entries). It is rendered in the "Data Inspection" section, showing
  the left and right value side by side.

Each argument is a mapping from display label to a metric. A bare callable is
wrapped in the metric of the corresponding family: ``change_metrics`` callables
become a :class:`~diffly.metrics.change.ChangeMetric` (computed for numerical
columns only), ``data_metrics`` callables become a
:class:`~diffly.metrics.data.DataMetric` (computed for all columns). To target a
different set of columns, construct the metric explicitly with a column selector,
e.g. ``ChangeMetric(fn, selector=cs.all())`` or ``DataMetric(fn, selector=cs.boolean())``.

The preset default sets are :data:`~diffly.metrics.change.DEFAULT_CHANGE_METRICS`
and :data:`~diffly.metrics.data.DEFAULT_DATA_METRICS`.

Change metrics
==============

.. currentmodule:: diffly.metrics.change

Metrics that describe the change between numeric columns by aggregating over
``right - left``.

.. autodata:: ChangeMetricFn
   :no-value:

.. autoclass:: ChangeMetric

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

.. autodata:: DataMetricFn
   :no-value:

.. autoclass:: DataMetric

.. autosummary::
   :toctree: _gen/

   null_fraction

.. autodata:: DEFAULT_DATA_METRICS
   :no-value:
