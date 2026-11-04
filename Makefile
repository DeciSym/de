HUB ?= decisym
TAG ?= latest
VERSION ?= 0.0.0-test
# Dataset scale for the trainmarks benchmarks: medium (~100K triples),
# large (~1M) or xlarge (~10M). Must match DE_BENCH_SCALE at bench time.
BENCH_SCALE ?= large
# WatDiv dataset scale: 10 (~1M triples) or 100 (~10M). Matches Comunica's
# benchmark-watdiv-hdt and benchmark-watdiv-hdt-100.
WATDIV_SCALE ?= 10

init:
	scripts/download-sample-bench.sh
	@command -v cargo-machete >/dev/null 2>&1 || cargo install cargo-machete
	@command -v cargo-deb >/dev/null 2>&1 || cargo install cargo-deb

# Clippy runs once per feature set: `server` gates code in and out, so the
# default build can warn about things the all-features build can't see.
lint:
	@command -v cargo-machete >/dev/null 2>&1 || cargo install cargo-machete
	cargo fmt --check
	cargo machete
	cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic
	cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic

test: init
	cargo test --all-features

presubmit: lint test

# Check out the trainmarks submodule and generate the N-Triples and Turtle
# fixtures the trainmarks bench target reads. Kept apart from `init` because
# they are ~150 MB at the default scale and only the benchmarks need them.
bench-init:
	git submodule update --init benches/trainmarks
	python3 scripts/gen-trainmarks-data.py $(BENCH_SCALE)

bench: bench-init
	DE_BENCH_SCALE=$(BENCH_SCALE) cargo bench --bench trainmarks

# Download the WatDiv fixtures Comunica publishes (an already-built dataset.hdt
# plus the twenty query templates) and run the WatDiv query benchmarks.
bench-watdiv:
	scripts/fetch-watdiv-data.sh $(WATDIV_SCALE)
	DE_WATDIV_SCALE=$(WATDIV_SCALE) cargo bench --bench watdiv

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
	docker run --rm -v ${PWD}/tests/resources:/data \
	${HUB}/de:${TAG} \
	query --data /data/superhero.ttl --sparql /data/hero-height.rq -q | tr -d '\r'| diff - tests/goldens/superhero-query.csv

# cargo-deb runs the release build itself; a separate `cargo build --release`
# beforehand only made it compile `de` a second time.
release: init
	cargo deb --deb-version ${VERSION} --features=server

serve: docker
	docker run -it --rm -v ${PWD}/tests/resources:/data -p 7878:7878 ${HUB}/de:${TAG} serve -l /data --bind 0.0.0.0:7878 -vvv
