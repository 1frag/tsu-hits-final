fmt:
	cargo fmt
build:
	cargo build
install:
	cd postgres_gateway && pip install -e . && cd -
tests:
	pytest
