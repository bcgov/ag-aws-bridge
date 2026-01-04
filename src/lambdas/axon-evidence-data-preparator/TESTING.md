# Testing — axon-evidence-data-preparator

Quick instructions to run unit tests for the `axon-evidence-data-preparator` lambda.

Prerequisites
- Python 3.11+ (project uses a virtual environment at `./.venv`)
- Project virtualenv activated or use full path to the venv Python / pytest binary

Run the single data-mapper test file

```bash
# from repo root
./.venv/bin/pytest -q src/lambdas/axon-evidence-data-preparator/esl_processor/data_mapper_test.py
```

Run all tests for the lambda module

```bash
# from repo root
./.venv/bin/pytest -q src/lambdas/axon-evidence-data-preparator
```

Run full repository test suite (may be slow)

```bash
./.venv/bin/pytest -q
```

Notes
- Tests in `esl_processor` are standard pytest tests and rely on adding the package parent to `sys.path` to allow relative imports.
- If you see import errors, ensure the venv is activated and the repo root is the current working directory when invoking pytest.
- To run a single test function, append `::test_name` to the test path.

Examples

```bash
# run a single test function
./.venv/bin/pytest -q src/lambdas/axon-evidence-data-preparator/esl_processor/data_mapper_test.py::test_valid_with_time
```
