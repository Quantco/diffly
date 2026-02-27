===============
Data Comparison
===============

.. currentmodule:: diffly

.. autofunction:: compare_frames

.. currentmodule:: diffly.comparison

.. autoclass:: DataFrameComparison
   :no-members:

   .. seealso:: :doc:`schema_comparison` — inspect column names and data types via :attr:`~DataFrameComparison.schemas`.

.. autosummary::
   :toctree: _gen/

   DataFrameComparison.equal
   DataFrameComparison.equal_num_rows
   DataFrameComparison.joined
   DataFrameComparison.joined_equal
   DataFrameComparison.joined_unequal
   DataFrameComparison.num_rows_left
   DataFrameComparison.num_rows_right
   DataFrameComparison.num_rows_joined
   DataFrameComparison.num_rows_joined_equal
   DataFrameComparison.num_rows_joined_unequal
   DataFrameComparison.left_only
   DataFrameComparison.right_only
   DataFrameComparison.num_rows_left_only
   DataFrameComparison.num_rows_right_only
   DataFrameComparison.fraction_same
   DataFrameComparison.change_counts
