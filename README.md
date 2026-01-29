# diffly

[![CI](https://img.shields.io/github/actions/workflow/status/Quantco/diffly/ci.yml?style=flat-square&branch=main)](https://github.com/Quantco/diffly/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/diffly?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/diffly)
[![pypi-version](https://img.shields.io/pypi/v/diffly.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/diffly)
[![python-version](https://img.shields.io/pypi/pyversions/diffly?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/diffly)
[![codecov](https://codecov.io/gh/Quantco/diffly/graph/badge.svg?token=uhk6X2XBFG)](https://codecov.io/gh/Quantco/diffly)

Utility package for comparing polars dataframes.

## Installation

This project is managed by [pixi](https://pixi.sh).
You can install the package in development mode using:

```bash
git clone https://github.com/Quantco/diffly
cd diffly

pixi run pre-commit-install
pixi run postinstall
pixi run test
```

## Testing

Our test setup consists of two components:

### Unit tests

We use unit tests to test the individual methods within our codebase. To run the unit tests, you can execute:

```bash
pixi run test
```

### Generated summary fixtures

To track changes to the comparison summary, we maintain a set of _summary fixtures_. If a code change results in a change to the summary, this should be reflected in at least one of the summary fixtures. Summary fixtures are automatically generated when running pre-commit hooks. To generate summary fixtures manually, you can execute:

```bash
pixi run pytest -m generate
```
