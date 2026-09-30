.PHONY: test test-generators build

test:
	python -m pytest -ra

test-generators:
	CROWDB_TPC_GENERATOR_TESTS=1 python -m pytest tests/integration/test_generators.py -ra

build:
	python -m build
