HUB ?= decisym
TAG ?= latest
VERSION ?= 0.0.0-test

init:
	scripts/download-sample-bench.sh
	@command -v cargo-machete >/dev/null 2>&1 || cargo install cargo-machete
	@command -v cargo-deb >/dev/null 2>&1 || cargo install cargo-deb

lint:
	@command -v cargo-machete >/dev/null 2>&1 || cargo install cargo-machete
	@command -v cargo-deb >/dev/null 2>&1 || cargo install cargo-deb
	cargo fmt --check
	cargo machete
	cargo clippy --workspace --all-targets --no-deps
	cargo clippy --workspace --all-targets --all-features --no-deps
	cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic

test: init
	cargo test --all-features

presubmit: lint test

bench: init
	cargo bench

build:
	cargo build --features=server,mcp

clean:
	cargo clean

docker: release
	docker build -f scripts/Dockerfile -t ${HUB}/de:${TAG} \
		--build-arg VERSION=${VERSION} \
		.
	docker run --rm -v ${PWD}/tests/resources:/data \
	    ${HUB}/de:${TAG} \
	    query --data /data/pineapple.ttl --sparql /data/query-fruit-color.rq

docker.run: docker
	docker run -it --rm -v ${PWD}/tests:/data ${HUB}/de:${TAG}

docker.test: docker
	docker run --rm -v ${PWD}/tests/resources:/data \
	${HUB}/de:${TAG} \
	query --data /data/superhero.ttl --sparql /data/hero-height.rq -q | tr -d '\r'| diff - tests/goldens/superhero-query.csv

release: init
	cargo build --release --features=server,mcp
	cargo deb --deb-version ${VERSION} --features=server,mcp

serve: docker
	docker run -it --rm -v ${PWD}/tests/resources:/data -p 7878:7878 ${HUB}/de:${TAG} serve -l /data --bind 0.0.0.0:7878 -vvv
