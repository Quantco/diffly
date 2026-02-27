<!-- LOGO -->
<br />

<div align="center">

  <h3 align="center">
  <code>diffly</code> — A utility package for comparing 🐻‍❄️ DataFrames
  </h3>

[![CI](https://img.shields.io/github/actions/workflow/status/Quantco/diffly/ci.yml?style=flat-square&branch=main)](https://github.com/Quantco/diffly/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/diffly?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/diffly)
[![pypi-version](https://img.shields.io/pypi/v/diffly.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/diffly)
[![python-version](https://img.shields.io/pypi/pyversions/diffly?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/diffly)
[![codecov](https://codecov.io/gh/Quantco/diffly/graph/badge.svg?token=N9Xwzu2Jdj)](https://codecov.io/gh/Quantco/diffly)

</div>

## 🗂 Table of Contents

- [Introduction](#-introduction)
- [Installation](#-installation)
- [Usage](#-usage)

## 📖 Introduction

Diffly is a Python package for comparing [Polars](https://pola.rs/) DataFrames with detailed analysis capabilities. It identifies differences between datasets including schema differences, row-level mismatches, missing rows, and column value changes.

## 💿 Installation

You can install `diffly` using your favorite package manager, e.g., `pixi` or `pip`:

```bash
pixi add diffly
pip install diffly
```

## 🎯 Usage

```python
import polars as pl
from diffly import compare_frames

left = pl.DataFrame({
    "id": ["a", "b", "c"],
    "value": [1.0, 2.0, 3.0],
})

right = pl.DataFrame({
    "id": ["a", "b", "d"],
    "value": [1.0, 2.5, 4.0],
})

comparison = compare_frames(left, right, primary_key="id")

if not comparison.equal():
    summary = comparison.summary(
        top_k_column_changes=1,
        show_sample_primary_key_per_change=True
    )
    print(summary)
```

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                                     Diffly Summary                                     ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
   Primary key: id

 Schemas
 ▔▔▔▔▔▔▔
   Schemas match exactly (column count: 2).

 Rows
 ▔▔▔▔
   Left count             Right count
       3      (no change)      3

   ┏━┯━┯━┯━┯━┓
   ┃-│-│-│-│-┃                1  left only   (33.33%)
   ┠─┼─┼─┼─┼─┨╌╌╌┏━┯━┯━┯━┯━┓╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╮
   ┃ │ │ │ │ ┃ = ┃ │ │ │ │ ┃  1  equal       (50.00%)  │
   ┠─┼─┼─┼─┼─┨╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌├╴  2  joined
   ┃ │ │ │ │ ┃ ≠ ┃ │ │ │ │ ┃  1  unequal     (50.00%)  │
   ┗━┷━┷━┷━┷━┛╌╌╌┠─┼─┼─┼─┼─┨╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╯
                 ┃+│+│+│+│+┃  1  right only  (33.33%)
                 ┗━┷━┷━┷━┷━┛

 Columns
 ▔▔▔▔▔▔▔
   ┌───────┬────────┬───────────────────────────┐
   │ value │ 50.00% │ 2.0 -> 2.5 (1x, e.g. "b") │
   └───────┴────────┴───────────────────────────┘
```

See more examples in the [documentation](https://diffly.readthedocs.io/stable/).
