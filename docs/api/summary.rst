=======
Summary
=======

.. currentmodule:: diffly.comparison

.. autosummary::
   :toctree: _gen/

   DataFrameComparison.summary

.. currentmodule:: diffly.summary

.. autoclass:: Summary
   :no-members:

.. autosummary::
   :toctree: _gen/

   Summary.format
   Summary.to_json

Metrics
=======

.. currentmodule:: diffly.metrics

The ``metrics`` argument of :meth:`~diffly.comparison.DataFrameComparison.summary`
accepts a mapping from display label to a :data:`Metric` callable. :mod:`diffly.metrics`
ships a set of presets.

.. autodata:: Metric
   :no-value:

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
