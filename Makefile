
HUB ?= decisym
TAG ?= latest
VERSION ?= 0.0.0-test

init:
	scripts/download-sample-bench.sh
	
lint:
	cargo install cargo-deb cargo-machete
	cargo fmt --check
	cargo machete
	cargo clippy --benches --tests --bins
	cargo clippy --benches --tests --bins --all-features

test: init
	cargo test

presubmit: lint test

bench: init
	cargo bench

build:
	cargo build --features=server

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
	docker run -it --rm -v ${PWD}/tests/resources:/data \
	${HUB}/de:${TAG} \
	de query --data /data/superhero.ttl --sparql /data/hero-height.rq

release: init
	cargo build --release --features=server
	cargo deb --deb-version ${VERSION} --features=server
