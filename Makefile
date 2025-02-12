
HUB ?= decisym
TAG ?= latest
VERSION ?= 0.0.0-test

init:
	mkdir -p deps
	scripts/download-rdf2hdt.sh
	scripts/download-sample-bench.sh
	

lint:
	cargo install cargo-deb cargo-machete
	cargo fmt --check
	cargo machete
	cargo clippy --benches --tests --bins

test: init
	cargo test

presubmit: lint test

bench: init
	cargo bench

build:
	cargo build

clean:
	cargo clean

docker: release
	docker build -f scripts/Dockerfile -t ${HUB}/de:${TAG} \
		--build-arg DE=${VERSION} \
		.

docker.run: docker
	docker run -it --rm -v ${PWD}/tests:/data ${HUB}/de:${TAG}

docker.test: docker
	docker run -it --rm -v ${PWD}/tests/resources:/data \
	${HUB}/de:${TAG} \
	de query --data /data/superhero.ttl --sparql /data/hero-height.rq

release: init
	cargo build --release
	cargo deb --deb-version ${VERSION}
