# Getting Started with DuckDB

This repository provides the supporting code for my presentation entitled [Getting Started with DuckDB](https://www.catallaxyservices.com/presentations/getting-started-with-duckdb/).

## Running the Code

If you are running things locally, the `code\DuckDB_Examples.ipynb` notebook is the complete code base for this talk.

### Setting up the environment with uv

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. To get started:

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/) if you don't have it already.
2. From the repo root, create a virtual environment and install dependencies:

```bash
uv sync
```

3. To run the Jupyter notebook:

```bash
uv run jupyter notebook code/DuckDB_Examples.ipynb
```

You will also need to download the data files for this demonstration. Links are available in the `data\DataLocations.txt` file. You should download the files to a folder called `\duckdbdata\`, at the same level as `\code\` and `\data\`.
