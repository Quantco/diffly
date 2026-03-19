# %%
import polars as pl
# %%
df = pl.DataFrame({"a": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]}, schema={"a": pl.Array(inner=pl.UInt8, shape=(2, 2))})
# %%
df.select(pl.col("a").arr.get(1))