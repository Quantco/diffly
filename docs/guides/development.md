# Development

Thanks for deciding to work on `diffly`!
You can create a development environment with the following steps:

## Environment Installation

First, install [`pixi`](https://pixi.sh/latest/) to manage the Python environment. Then:

```bash
git clone https://github.com/Quantco/diffly
cd diffly
pixi install
```

Next make sure to install the package locally and set up pre-commit hooks:

```bash
pixi run postinstall
pixi run pre-commit-install
```

## Running the tests

```bash
pixi run test
```

To run a specific test file or test function:

```bash
pixi run test tests/test_equal.py
pixi run test tests/test_equal.py::test_equal
```

### Test fixtures

Summary output tests use fixtures stored in `tests/summary/fixtures/`. If you change the summary output format, regenerate the fixtures with:

```bash
pixi run test -m generate
```

## Documentation

We use [Sphinx](https://www.sphinx-doc.org/en/master/index.html) together
with [MyST](https://myst-parser.readthedocs.io/), and write user documentation in markdown.
If you are not yet familiar with this setup,
the [MyST docs for Sphinx](https://myst-parser.readthedocs.io/en/v0.17.2/sphinx/intro.html) are a good starting point.

When updating the documentation, you can compile a localized build of the
documentation and then open it in your web browser using the commands below:

```bash
# Run build
pixi run -e docs postinstall
pixi run docs

# Open documentation
open docs/_build/html/index.html
```
