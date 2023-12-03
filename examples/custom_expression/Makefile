



develop:
	poetry run maturin develop --release -m Cargo.toml


bench-grouped:
	poetry run python -m pytest

hist:
	poetry run python -m pytest --benchmark-group-by=param --benchmark-histogram="bench/hist"

bench:
	poetry run python -m pytest --benchmark-group-by=param
