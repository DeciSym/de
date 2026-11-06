# Benchmarks

`de` carries three criterion targets:

| Target | Workload | What it is for |
| --- | --- | --- |
| `trainmarks` | trainmarks e-commerce graph | HDT creation and a handful of queries, tracked over time |
| `watdiv` | WatDiv, 20 templates x 5 instances | Query evaluation over pure basic graph patterns |
| `bsbm` | Berlin SPARQL Benchmark "explore", 12 templates x 5 instances | Query evaluation over `OPTIONAL`/`UNION`/`FILTER`/`ORDER BY`/`DESCRIBE`/`CONSTRUCT` |

`watdiv` and `bsbm` are the two workloads Comunica runs for its own HDT engine
(`performance/benchmark-watdiv-hdt`, `performance/benchmark-bsbm-hdt`), imported
so that the work in [`SPARQL-QUERY-PERFORMANCE.md`](SPARQL-QUERY-PERFORMANCE.md)
can be judged against the same data other engines publish numbers for.

## Running them

```sh
make bench-watdiv                       # WatDiv at scale 10 (default)
WATDIV_SCALE=100 make bench-watdiv      # ~10M triples

make bench-bsbm                         # BSBM at 1000 products (default)
BSBM_PRODUCT_COUNT=10000 make bench-bsbm

make bench-query                        # both
make bench                              # trainmarks only
```

Each target downloads or generates its fixtures first and then runs. Fixtures
live under `benches/watdiv/<scale>/` and `benches/bsbm/<product-count>/` and are
git-ignored. Running a bench target without its fixtures prints how to get them
and measures nothing, so `cargo bench` still works on a fresh clone.

Fixture provenance:

* **WatDiv** — `scripts/fetch-watdiv-data.sh` downloads the archive Comunica
  publishes at `comunica/comunica-performance-assets`, which holds an
  already-built `dataset.hdt` and the twenty query templates. Using their
  archive rather than re-running the WatDiv generator keeps the two engines'
  numbers comparable.
* **BSBM** — `scripts/gen-bsbm-data.py` downloads the official `bsbmtools`
  (needs Java) and runs its generator, then substitutes the twelve "explore"
  query templates with parameters drawn from the generated graph. The official
  Java test driver measures a SPARQL endpoint over HTTP and does not fit an
  in-process criterion target, so only the driver's parameter-pool role is
  reimplemented. Both halves are deterministic, so a given product count always
  yields the same fixtures.

## What is measured

Opening the package and parsing the queries happen once, outside the timed
region. What is timed is planning, evaluation and result serialisation — into a
sink, so the write syscall stays out of it.

One criterion iteration runs all five instances of a template. That is the unit
Comunica reports, and averaging the five smooths out the fact that WatDiv and
BSBM parameterisation makes some instances match nothing.

The `*_open/get_snapshot` groups time `AggregateHdt::get_snapshot` on its own.
That is the per-request cost `de serve` pays today, and it is tracked separately
so the effect of caching HDT handles is visible without disturbing the query
numbers.

## Baseline

Recorded before any of the work in `SPARQL-QUERY-PERFORMANCE.md` landed.

* commit `bd5e4ee`, rustc 1.97.1, `--release`
* 13th Gen Intel Core i7-1365U, 6 cores / 12 threads, 31 GiB
* criterion median of 10 samples; one sample = all five instances of a template

### WatDiv, scale 10 (1,000,000 triples, 7.2 MiB package)

| Template | Median |
| --- | ---: |
| `open/get_snapshot` | 304.00 µs |
| C1 | 4.5570 ms |
| C2 | 51.555 ms |
| C3 | 5.3576 s |
| F1 | 1.4200 ms |
| F2 | 908.74 µs |
| F3 | 669.61 µs |
| F4 | 908.12 µs |
| F5 | 3.3752 ms |
| L1 | 532.13 µs |
| L2 | 1.2877 ms |
| L3 | 407.56 µs |
| L4 | 485.87 µs |
| L5 | 1.0541 ms |
| S1 | 3.0571 ms |
| S2 | 516.12 µs |
| S3 | 1.4064 ms |
| S4 | 9.5781 ms |
| S5 | 1.6511 ms |
| S6 | 173.56 µs |
| S7 | 58.392 µs |

C3 is three orders of magnitude slower than its neighbours and is the single
best signal in the suite: it is a 48,802-row complex pattern whose cost is
dominated by the order its joins run in.

### BSBM, 1000 products (371,911 triples, 18.7 MiB package)

| Template | Median |
| --- | ---: |
| `open/get_snapshot` | 429.10 µs |
| q01 | 303.91 µs |
| q02 | 1.3658 ms |
| q03 | 450.84 µs |
| q04 | 785.23 µs |
| q05 | 12.726 ms |
| q06 | 15.614 ms |
| q07 | 2.0837 ms |
| q08 | 1.6093 ms |
| q09 | 101.93 µs |
| q10 | 1.3726 ms |
| q11 | 121.71 µs |
| q12 | 338.03 µs |

q05 (similar products: a self-join over shared features with two range filters)
and q06 (a regex scan over every product label) are the two that do real work;
the rest are anchored on a bound subject and finish in microseconds.

### Reading `get_snapshot`

At these sizes opening is a few hundred microseconds, which is small next to
most queries — but it is paid *per HTTP request* by `de serve`, and it scales
with the package rather than with the query. The cost is dominated by
bincode-decoding the wavelet tree out of the `.index.v5-rust-cache` sidecar, so
it grows with triple count: the 2.3 GiB sidecar next to a 5 GiB `dblp.hdt` is
the case that matters.

## Results

Each row is the criterion median. "vs baseline" is measured against the table
above; changes inside roughly +/-3% are run-to-run variance on this machine and
are not read as movement.

### After: local fixes in `de/src/sparql.rs` (`SPARQL-QUERY-PERFORMANCE.md` item 4)

Three changes, measured one at a time:

1. Drop `de`'s fork of `triples_with_pattern` and call `hdt`'s, which brings back
   `TripleCache` and `id_to_arc` (one allocation per materialised term instead of
   two, and a hit when consecutive triples repeat a term).
2. Add `HdtGeneric::triples_with_arc_pattern` upstream and call that. The `&str`
   entry point copies every bound term into a fresh `Arc` on each call, which in
   a nested-loop join is one allocation per bound term per probe; passing the
   `Arc` the caller already holds makes it a refcount increment. Step 1 alone
   regressed `L1`/`L2` by 4-8% for exactly this reason.
3. Resolve graph selection and the `Arc<str>` graph IRIs once, when the snapshot
   is built, instead of rebuilding a filtered `Vec` and re-allocating the IRI on
   every probe.

#### WatDiv, scale 10

| Template | Baseline | After | vs baseline |
| --- | ---: | ---: | ---: |
| C1 | 4.5570 ms | 4.2758 ms | -6.2% |
| C2 | 51.555 ms | 49.201 ms | -4.6% |
| C3 | 5.3576 s | 5.1342 s | -4.2% |
| F1 | 1.4200 ms | 1.3241 ms | -6.8% |
| F2 | 908.74 µs | 893.19 µs | -1.7% |
| F3 | 669.61 µs | 619.00 µs | -7.6% |
| F4 | 908.12 µs | 839.01 µs | -7.6% |
| F5 | 3.3752 ms | 2.8564 ms | -15.4% |
| L1 | 532.13 µs | 508.53 µs | -4.4% |
| L2 | 1.2877 ms | 1.2207 ms | -5.2% |
| L3 | 407.56 µs | 373.95 µs | -8.2% |
| L4 | 485.87 µs | 477.97 µs | -1.6% |
| L5 | 1.0541 ms | 967.60 µs | -8.2% |
| S1 | 3.0571 ms | 2.7432 ms | -10.3% |
| S2 | 516.12 µs | 470.94 µs | -8.8% |
| S3 | 1.4064 ms | 1.2966 ms | -7.8% |
| S4 | 9.5781 ms | 9.2569 ms | -3.4% |
| S5 | 1.6511 ms | 1.5883 ms | -3.8% |
| S6 | 173.56 µs | 163.72 µs | -5.7% |
| S7 | 58.392 µs | 53.531 µs | -8.3% |

#### BSBM, 1000 products

| Template | Baseline | After | vs baseline |
| --- | ---: | ---: | ---: |
| q01 | 303.91 µs | 271.00 µs | -10.8% |
| q02 | 1.3658 ms | 1.2536 ms | -8.2% |
| q03 | 450.84 µs | 401.08 µs | -11.0% |
| q04 | 785.23 µs | 591.89 µs | -24.6% |
| q05 | 12.726 ms | 11.666 ms | -8.3% |
| q06 | 15.614 ms | 14.098 ms | -9.7% |
| q07 | 2.0837 ms | 1.7012 ms | -18.4% |
| q08 | 1.6093 ms | 1.2369 ms | -23.1% |
| q09 | 101.93 µs | 70.436 µs | -30.9% |
| q10 | 1.3726 ms | 1.0424 ms | -24.1% |
| q11 | 121.71 µs | 101.19 µs | -16.9% |
| q12 | 338.03 µs | 345.40 µs | +2.2% |

BSBM gains more than WatDiv because its templates are mostly anchored on a bound
subject and probe a small number of triples each, so per-call and per-term
overhead is a larger share of the total. q04, q06, q07 and q11 had wide
confidence intervals at baseline, so part of their movement is noise; q08, q09,
q10 and q12 were tight in every run and can be read at face value.

`get_snapshot` is unchanged by this work. The baseline figures for it were taken
on the first run after the index sidecar was written, so they are a cold-cache
reading; subsequent runs settle at 280-305 µs for both workloads.

### After: cardinality-aware join ordering (`SPARQL-QUERY-PERFORMANCE.md` item 1)

Measured against the state after item 4, so the numbers below are the effect of
planning from real counts alone.

`hdt` gained `cardinality(s, p, o) -> Cardinality { value, exact }`; `spareval`
gained an optional `QueryableDataset::estimated_cardinality`, and `sparopt` a
`CardinalityEstimator` the optimizer consults before falling back to its shape
table; `de` implements it over the loaded graphs.

#### WatDiv, scale 10

| Template | After item 4 | With cardinality | change |
| --- | ---: | ---: | ---: |
| C1 | 4.1719 ms | 4.2382 ms | +1.6% |
| C2 | 48.142 ms | 18.901 ms | **-61%** |
| C3 | 5.0920 s | 211.74 ms | **-96%** |
| F1 | 1.3020 ms | 395.84 µs | **-70%** |
| F2 | 876.49 µs | 886.85 µs | +1.2% |
| F3 | 617.48 µs | 638.60 µs | +3.4% |
| F4 | 849.05 µs | 891.99 µs | +5.1% |
| F5 | 2.8114 ms | 2.8819 ms | +2.5% |
| L1 | 507.17 µs | 520.75 µs | +2.7% |
| L2 | 1.1942 ms | 1.2168 ms | +1.9% |
| L3 | 372.50 µs | 384.61 µs | +3.2% |
| L4 | 460.01 µs | 480.60 µs | +4.5% |
| L5 | 953.04 µs | 973.14 µs | +2.1% |
| S1 | 2.7359 ms | 2.7918 ms | +2.0% |
| S2 | 467.97 µs | 438.82 µs | -6.2% |
| S3 | 1.3540 ms | 1.9485 ms | +44% |
| S4 | 9.0164 ms | 2.1967 ms | **-76%** |
| S5 | 1.5677 ms | 1.9169 ms | +22% |
| S6 | 163.87 µs | 221.02 µs | +35% |
| S7 | 56.323 µs | 65.903 µs | +17% |

The whole suite goes from 5.14 s to 0.25 s, almost all of it C3. The four big
wins are the queries where the shape table's guesses were furthest from the
truth; the small regressions are what is left of the planning cost after
memoisation, which lands hardest on the templates that were already the
cheapest. S3 and S5 are the two whose *plans* changed for the worse.

#### BSBM, 1000 products

| Template | After item 4 | With cardinality | change |
| --- | ---: | ---: | ---: |
| q01 | 271.00 µs | 342.35 µs | +26% |
| q02 | 1.2536 ms | 1.2377 ms | -1.3% |
| q03 | 401.08 µs | 447.83 µs | +12% |
| q04 | 591.89 µs | 674.94 µs | +14% |
| q05 | 11.666 ms | 11.467 ms | -1.7% |
| q06 | 14.098 ms | 13.440 ms | -4.7% |
| q07 | 1.7012 ms | 1.5309 ms | -10% |
| q08 | 1.2369 ms | 1.2683 ms | +2.5% |
| q09 | 70.436 µs | 70.345 µs | 0% |
| q10 | 1.0424 ms | 1.1006 ms | +5.6% |
| q11 | 101.19 µs | 95.379 µs | -5.7% |
| q12 | 345.40 µs | 358.10 µs | +3.7% |

BSBM is close to a wash, which is the expected result rather than a
disappointment: eleven of its twelve templates are anchored on a bound subject or
offer, so there is little join order left to get wrong and the counts mostly
confirm what the shape table already guessed. What it does show is the residual
planning cost, clearest on the cheapest templates.

#### Two things that had to be fixed before this was a win

Both are recorded because the intermediate measurements are the reason the final
design looks the way it does.

**A badly calibrated estimate is worse than no estimate.** The first version
answered `?P?` by scaling the predicate's distinct subject-predicate pair count
by the graph-wide triples-per-pair ratio. That is a sound *average* and a
useless *ranking*: on WatDiv it reported 1048 triples for `schema:caption`
(really 224) and 627 for `schema:actor` (really 1708), inverting the two. C1
regressed 5x because the optimizer dutifully started from the pattern the
estimate said was smallest. Sampling adjacency-run lengths across the
predicate's occurrences instead gets `caption` exactly right and `actor` to
within 5%, and C1 returns to its original plan.

**Planning has to be cheap enough to disappear.** With accurate estimates but no
caching, S7 went from 56 µs to 609 µs and S1 from 2.7 ms to 17.5 ms — while
S1's plan was *byte-for-byte identical*. Greedy reordering re-estimates each
pattern once per candidate it considers, so an n-pattern BGP asks O(n²) times,
and each `?P?` answer was sampling the index afresh. Two caches fixed it: one in
`hdt` keyed by predicate ID, since a loaded graph's counts never change, and one
in `spareval`'s estimator adapter for the duration of a single optimization.

### Effect on the `trainmarks` target

`trainmarks` was not part of the loop above, so it was checked separately after
the fact. The short answer is no regression: two of its five queries get a better
join order, three are unaffected, and HDT creation is untouched.

**Creation is untouched by construction.** `create_hdt_from_nt`,
`create_hdt_from_ttl` and the conversion half of `q1_count_from_ttl` go through
`Hdt::read_nt`, which none of this work changes beyond one added struct field.
Back-to-back runs of the two builds measured within 3% of each other, which on
this machine is nothing.

**Plans.** Dumped with `--debug-query-plan` before and after — exact, not a
measurement:

| Query | Plan | Why |
| --- | --- | --- |
| `q1_count` | no join to order | single `?s ?p ?o` |
| `q2_customer_orders` | **changed** | now starts from `?customer rdfs:label ?name` (12,000) instead of `?order :placedBy ?customer` (133,000) |
| `q3_join_3_entities` | identical | the shape table already put `:country :Norway` first, being the only pattern with a bound object |
| `q4_optional_aggregation` | identical | same reason, for `rdf:type :Customer` |
| `q5_construct` | **changed** | now starts from `?customer :country :Norway` (1,021) instead of `?customer a :Customer` (10,000) |

Both changes move a 10x more selective pattern to the front of a for-loop join
chain, and both produce the same results.

**Timing.** This machine was too noisy for criterion to say anything useful about
`trainmarks` — its confidence intervals ran to +/-20%, and a control run of the
*unmodified* build came out slower than the modified one across the board.
Ten in-process evaluations per measurement, with the two arms alternated and the
best of three taken, gives numbers consistent with the plan table:

| Query | change | plan |
| --- | ---: | --- |
| `q1_count` | -3% | unchanged |
| `q2_customer_orders` | -11% | changed |
| `q3_join_3_entities` | 0% | unchanged |
| `q4_optional_aggregation` | +4% | unchanged |
| `q5_construct` | -17% | unchanged |

Read these as directions, not figures. What they support is the plan table: where
the plan did not change, the effect is somewhere between nothing and a few
percent of planning cost; where it did, it is a double-digit win.

### A note on benchmarking this machine

The i7-1365U is a 15 W laptop part, and after roughly forty minutes of sustained
benchmarking it throttles hard: the same benchmark on the same binary ran 40-50%
slower late in a session than at the start. Every comparison in this file was
taken from runs adjacent in time, and the `trainmarks` section above was
re-measured with the two arms interleaved after a sequential A/B produced an
impossible result (HDT creation "regressing" by 33%). Treat any single run as
unreliable, and interleave.

### After: caching open HDT handles (`SPARQL-QUERY-PERFORMANCE.md` item 3)

`AggregateHdt` now keeps opened packages and hands them out as `Arc<HdtAny>`,
keyed by each file's modification time and length and bounded by heap footprint.

#### `get_snapshot`

| Workload | Before | After | change |
| --- | ---: | ---: | ---: |
| WatDiv scale 10 (7.2 MiB package) | 280.42 µs | 964.17 ns | **-99.6%** |
| BSBM 1000 products (18.7 MiB package) | 305.00 µs | 965.94 ns | **-99.7%** |

What is left is a `stat` per graph and a refcount increment.

#### The case this was for: a 5 GB package behind `de serve`

`dblp.hdt` is 5.3 GB with a 2.3 GB index sidecar. Five sequential SPARQL
requests, each a pattern that matches nothing so the timing is all overhead:

| Request | Wall time | Server RSS |
| --- | ---: | ---: |
| 1 (cold) | 0.98 s | 663 MB |
| 2 | 0.00 s | 663 MB |
| 3-5 | 0.00 s | 663 MB |

The first request is what *every* request used to cost. The RSS is the decoded
wavelet tree, which used to be allocated and freed per request and is now
allocated once.

Concurrency is the sharper version of the same point. Eight simultaneous
requests against that server moved peak RSS from 679,396 KB to 680,220 KB — an
increase of under a megabyte, because all eight share one handle. Before, each
in-flight request decoded its own copy, so eight concurrent requests meant eight
copies of a 663 MB structure.

#### Query benchmarks

Unchanged, as they should be: the benchmark builds its snapshot once, outside
the timed region, so the cache is not on the measured path. A run with the cache
and an interleaved control without it agreed to within a few percent in both
directions.

Both runs sat about 10% above the numbers recorded for item 1, which is the
machine and not the code — see the note at the end of this file. The comparison
that means anything is the interleaved one.
