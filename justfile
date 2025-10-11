# just manual: https://github.com/casey/just#readme

_default:
    just --list

# Install dependencies used by this project
bootstrap default="3.12":
    uv venv --python {{default}}
    just sync

# Sync dependencies with environment
sync:
    uv sync

# Build the project as a package (uv build)
build *args:
    uv build

# Run the code formatter
format:
    uv run ruff format raft tests

# Run code quality checks
check:
    #!/bin/bash -eux
    uv run ruff check raft tests

    # Need to more work for this
    # just check-types

# Run mypy checks
check-types:
    #!/bin/bash -eux
    uv run mypy raft

# Run all tests locally
test *args:
    #!/bin/bash -eux
    uv run pytest {{args}}

# Run the project tests for CI environment (e.g. with code coverage)
ci-test coverage_dir='./coverage':
    uv run pytest --cov=raft --cov-report xml --junitxml=./coverage/unittest.junit.xml

# Build documentation locally
docs-build *args:
    uv run mkdocs build {{args}}

# Serve documentation locally with auto-reload
docs-serve:
    uv run mkdocs serve

# Run the demo with 3 nodes
run-demo node_id="3":
    uv run python -m raft -c raft.ini -n {{node_id}}

# Run an example
example script="demo_snapshots":
    uv run python -m examples.{{script}}