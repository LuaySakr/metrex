Contributing to metrex
======================

Thanks for your interest in contributing! This guide helps you get set up and make effective changes.

Getting started
---------------

Prereqs: Python 3.8+, git, and a shell.

1. Clone and install in editable mode with dev extras:

   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -U pip
   pip install -e .[dev]
   ```

2. Sanity check the CLI:

   ```bash
   metrex list
   metrex metrics --help
   metrex rank --help
   ```

3. Data expectations: Freqtrade `.feather` files named `PAIR-TF.feather` (e.g., `BTC_USDT-1h.feather`) with columns: `date` (or `timestamp`), `open, high, low, close, volume`.

Common workflows
----------------

- Run metrics:
  ```bash
  metrex metrics --datafolder ./data --timeframe 1h \
                 --timerange 20240101-20240131 \
                 --metrics breadth_sma50,market_vol_regime \
                 --output ./results/market_metrics.feather
  ```

- Per‑pair ranking outputs:
  ```bash
  metrex rank --datafolder ./data --timeframe 1h \
              --timerange 20240101-20240131 \
              --outputfolder ./results
  ```

Coding guidelines
-----------------

- Keep changes focused and minimal; prefer small, reviewable PRs.
- Use type hints and docstrings; follow existing code style.
- Run format/lint locally:
  ```bash
  black .
  flake8
  ```
- Add/update documentation when behavior or CLI changes.

Adding a new metric
-------------------

1. Create a module `metrex/metrics/<your_metric>.py` implementing `MetricProtocol`:
   - `compute(market_df, ctx)` returns a DataFrame with `date` plus one or more metric columns.
   - Do not include a `pair` column in the returned frame (market‑level).
2. Register the metric at the end of the module:
   ```python
   from . import register
   register(YourMetric())
   ```
3. Ensure your module is imported so it registers:
   - Add an import in `metrex/metrics/__init__.py` (explicit imports are used).
4. Validate with a small dataset and document your metric’s columns in the README if user‑facing.

Tests
-----

- If adding logic with edge cases, please include/extend tests under `tests/`.
- Prefer small, synthetic datasets for deterministic tests.

Releases
--------

- Bump version in `metrex/__init__.py` when making user‑visible changes.
- Update the README and changelog (if present).

Issues and PRs
--------------

- Use the provided issue templates (bug, feature, docs) for clarity.
- In PRs, include reproduction commands and expected outputs when applicable.

Thank you for helping improve metrex!

