# SPARQL-over-HDT query performance: findings and plan

Status: items 1, 3 and 4 done; items 2 and 5 open.
Started 2026-09-16, last updated 2026-09-18.

This note records what a read of [comunica-feature-hdt] (Comunica's SPARQL-over-HDT
support) suggests for `de` and for the [`hdt`] crate it builds on, why each item
matters, and how each one is measured. It is a working document: the "Status"
line on each item is updated as work lands, and a claim that measurement
contradicts is corrected in place rather than quietly dropped -- item 2 is the
example, and the correction is more useful than the original.

| Item | Status | Effect |
| --- | --- | --- |
| 1. Cardinality-aware join ordering | done | WatDiv 5.14 s -> 0.25 s; BSBM a wash |
| 2. Join values as dictionary IDs | open, scoped | 2.7-4.6x on the probe path, whole-query share unmeasured |
| 3. Open each HDT once per process | done | `get_snapshot` -99.6%; 5 GB package decoded once, not per request |
| 4. Local fixes in `de/src/sparql.rs` | done | WatDiv -2 to -15%, BSBM -8 to -31% |
| 5. `hdt/src/sparql.rs` materialises everything | open | `hdt` crate hygiene; no effect on `de` |

Measurements live in [`BENCHMARKS.md`](BENCHMARKS.md), including a note on why
this machine cannot be trusted for a sequential A/B.

**Local build overrides.** Items 1 and 4 change `hdt` and the DeciSym oxigraph
fork as well as `de`. Until those are pushed and the pins in `Cargo.toml` are
bumped, `de` builds against working trees through a git-ignored
`.cargo/config.toml`:

| Crate | Working tree | Branch, commit |
| --- | --- | --- |
| `hdt` | `../hdt` | `perf-qwt-opus-max`, `8a4f52b` (committed, unpushed) |
| `spareval`, `sparopt`, `spargebra`, `sparesults`, `oxrdfxml` | `../oxigraph-cardinality` | `cardinality-estimator-hook`, `85f04251` (committed, unpushed) -- a worktree off the pinned `ed7335a2` |

The five oxigraph crates have to move as a set; patching only some of them would
put two versions of `spargebra`'s types in the graph. A checkout without that
file keeps using the pinned revisions and builds `de` as it was.

[comunica-feature-hdt]: https://github.com/comunica/comunica-feature-hdt
[`hdt`]: https://github.com/DeciSym/hdt

## Why look at Comunica at all

Comunica's HDT support is a thin actor (`packages/actor-query-source-identify-hdt`)
over the `hdt` npm binding to hdt-cpp. It is roughly 250 lines, so it is not a
source of algorithms. It is useful because it is a second implementation of the
same problem — "answer SPARQL out of an HDT package" — that made different
structural choices, and two of those choices are ones `de` has not made yet.

The parts worth taking:

| Comunica does | `de` when this was written | now |
| --- | --- | --- |
| Asks HDT for a real cardinality per pattern and plans joins from it | Planned from a fixed 8-entry constant table that never sees the data | done (item 1) |
| Hands bindings straight out of the source, no intermediate term objects | Round-trips every join value through a dictionary string and back | open (item 2) |
| Opens each HDT document once and keeps it until invalidated | Re-opened every graph on every request | done (item 3) |

The parts not worth taking are recorded in [Deliberately not adopted](#deliberately-not-adopted).

---

## 1. Cardinality-aware join ordering

**Status: done, across three repositories.** Measured in
[`BENCHMARKS.md`](BENCHMARKS.md): WatDiv scale 10 goes from 5.14 s to 0.25 s for
the whole suite, with C3 -96%, S4 -76%, F1 -70% and C2 -61%; BSBM is a wash. On
`trainmarks`, two of five queries get a better join order (-11% and -17%), the
other three keep the plan they had, and HDT creation is untouched.

What landed:

* **`hdt`** — `HdtGeneric::cardinality(s, p, o) -> Cardinality { value, exact }`
  and `cardinality_with_id_pattern`, plus `size_hint` on all four pattern
  iterators and `ExactSizeIterator` on the two that can promise it. `?P?` is
  answered by sampling adjacency-run lengths and cached per predicate; every
  other shape is O(1) or O(log n) off the existing index.
* **`spareval` / `sparopt`** (DeciSym fork, branch `cardinality-estimator-hook`)
  — an optional `QueryableDataset::estimated_cardinality` defaulting to `None`,
  a `sparopt::CardinalityEstimator` trait the optimizer consults before falling
  back to `estimate_triple_pattern_size`, and an adapter that memoises answers
  for the duration of one optimization.
* **`de`** — `estimated_cardinality` over the loaded graphs.

The `--debug-query-plan` output for the `cwe.hdt` case below now shows the
selective pattern first whichever order the query was written in, and both
orders run in 0.00 s.

Two findings from the intermediate measurements are written up under
"Two things that had to be fixed" in `BENCHMARKS.md`: a mis-calibrated estimate
reordered predicates and made C3's neighbour C1 five times slower, and accurate
estimates without caching cost more than the better plans saved.

### Still open

* Property paths still use the shape table: `estimate_path_size` was left alone.
* The estimate is taken before the query's `FROM`/`FROM NAMED` restriction, so a
  query that narrows the dataset gets counts for the whole of it.
* S3, S5 and BSBM q01/q03/q04 are the cases where the new plans are worse. They
  are small in absolute terms, but they are the evidence that greedy reordering
  with a leaf-only cost model does not always improve when the leaves get more
  accurate — a join-order search that estimates *intermediate* sizes is the next
  thing to try.

### Background: what this replaced

### What Comunica does

`HdtIterator` calls `countTriples(s, p, o)` before it reads anything and publishes
the answer as iterator metadata:

```ts
// packages/actor-query-source-identify-hdt/lib/HdtIterator.ts:57
this.hdtDocument.countTriples(subject, predicate, object)
  .then(({ totalCount, hasExactCount }) => {
    this.setProperty('metadata', {
      cardinality: { type: hasExactCount ? 'exact' : 'estimate', value: totalCount },
      ...
    });
  });
```

Comunica's join actors pick join order and join algorithm from that number. Note
the `hasExactCount` flag: HDT can answer some patterns exactly and cheaply, and
others only approximately, and the planner is told which it got.

### What `de` does

Nothing equivalent exists, at either layer.

`sparopt` — the optimizer `spareval` runs — estimates every triple pattern from
its *shape* alone:

```rust
// sparopt/src/optimizer.rs:1005
fn estimate_triple_pattern_size(subject_bound: bool, predicate_bound: bool, object_bound: bool) -> usize {
    match (subject_bound, predicate_bound, object_bound) {
        (true, true, true) => 1,
        (true, true, false) => 10,
        (true, false, true) => 2,
        (false, true, true) => 10_000,
        (true, false, false) => 100,
        (false, false, false) => 1_000_000_000,
        (false, true, false) => 1_000_000,
        (false, false, true) => 100_000,
    }
}
```

Every `?s <p> ?o` pattern scores 1,000,000 regardless of whether `<p>` occurs
three times or sixteen thousand times. `Optimizer::reorder_joins` picks the
minimum by this key, so when two patterns tie — which is the common case inside a
BGP of same-shaped patterns — the order is whatever the query text said.

Because `spareval` evaluates a `QuadPattern` by calling
`internal_quads_for_pattern` **once per input tuple** (`spareval/src/eval.rs:783`,
the `Lateral`/for-loop join path), a bad first pattern multiplies through every
later one.

### Measured, on `cwe.hdt`

Same BGP, same (empty) result, only the two lines swapped:

```sparql
# a.rq
?s cwe:startsWith ?x .    # 3 triples
?s cwe:modification ?m .  # 16,447 triples

# b.rq — same two patterns, other order
?s cwe:modification ?m .
?s cwe:startsWith ?x .
```

| | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| `a.rq` (selective first) | 0.00 s | 0.01 s | 0.01 s |
| `b.rq` (selective last) | 0.06 s | 0.05 s | 0.11 s |

`de query --debug-query-plan` confirms the cause: both plans are a `Lateral` with
the patterns in source order. The planner scored both at 1,000,000 and had no
tiebreaker. This is on a 3.2 MB package; the factor grows with the data.

### The counts are already sitting in the iterators

None of this needs new index structures. Every HDT pattern iterator computes its
own bounds in its constructor and then throws the information away:

| Pattern | Source | Cost | Exact? |
| --- | --- | --- | --- |
| `S??`, `SP?`, `SPO`, `???` | `max_z - min_z` (`triples/subject_iter.rs:61`) | O(1) | yes |
| `??O` | `op_index.last(o) - op_index.find(o) + 1` (`triples/object_iter.rs:170`) | O(1) | yes |
| `?PO` | `max_index - pos_index + 1` after the binary search (`triples/predicate_object_iter.rs:264`) | O(log n) | yes |
| `?P?` | `wavelet_y.rank(p)` (`triples/predicate_iter.rs:211`) | O(1) | no — counts distinct SP pairs, a lower bound |
| `S?O` | `max_y - min_y` | O(1) | no — upper bound |

That maps one-to-one onto Comunica's `{ totalCount, hasExactCount }`.

None of these iterators implements `size_hint`, so they all report `(0, None)`.
Fixing that is nearly free once the bounds are exposed, and it also improves
`.count()` and `collect()` preallocation everywhere else in the crate.

### Plan

1. `hdt`: add `HdtGeneric::cardinality(s, p, o) -> Cardinality { value, exact }`
   plus an id-pattern variant, and implement `size_hint` / `ExactSizeIterator` on
   the four pattern iterators.
2. `spareval` (DeciSym fork): add an optional
   `fn estimated_cardinality(...) -> Option<usize>` to `QueryableDataset`,
   defaulting to `None`, and thread a callback into `sparopt`'s
   `estimate_graph_pattern_size` so `estimate_triple_pattern_size` becomes the
   fallback rather than the only answer.
3. `de`: implement it over `AggregateHdtSnapshot`, summing across selected graphs.

---

## 2. Keep join values as dictionary IDs

**Status: not started, and scoped out.** Measured 2026-09-18; the earlier version
of this section misdiagnosed where the cost was, and its sketch was unsound. Both
are corrected below.

### What Comunica does

Commit `623d9af`, "Avoid creation of intermediary triple objects", replaced
`searchTriples` with `searchBindings` so the source produces bindings directly and
no intermediate triple objects are allocated per result.

### What is actually being paid for

`de`'s `InternalTerm` is `Arc<str>`, so every value that flows between patterns is
a string. Timing 20,000 probes per predicate on WatDiv scale 10, against the same
walk done entirely in dictionary IDs:

| Predicate | results/probe | string path | ID path | speedup | of which `string_to_id` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `wsdbm:friendOf` | 128 | 391.6 ms | 132.5 ms | 3.0x | 8.5 ms (2%) |
| `rev:reviewer` | 1 | 13.7 ms | 5.0 ms | 2.7x | 6.8 ms (50%) |
| `foaf:givenName` | 1 | 8.5 ms | 3.1 ms | 2.7x | 3.1 ms (37%) |
| `schema:text` | 1 | 3.8 ms | 0.8 ms | 4.6x | 1.1 ms (28%) |

This section used to claim the cost was `string_to_id` -- re-resolving a string
the previous pattern had just produced. That is real but it is the *smaller* half,
and only dominates at low fan-out. The larger half is materialising result terms
that a nested-loop join consumes and discards: roughly 250 ms of `friendOf`'s
391 ms. Keeping intermediates as IDs avoids both, and defers `externalize_term`
to the terms that actually reach the result set -- the same shape as Comunica's
`searchBindings` change.

### The sketch that was wrong

```rust
enum HdtTerm { Id(u32), Str(Arc<str>) }   // WRONG as written
```

The claim attached to it -- "`Eq`/`Hash` stay sound" -- is false. HDT numbers
subjects and objects in overlapping spaces, so the same integer names two
different terms depending on position. On WatDiv:

```
id 47961 as subject: "http://db.uwaterloo.ca/~galuc/wsdbm/City236"
id 47961 as object:  "\"0\""
```

A pattern like `?a :p ?x . ?x :q ?b` binds `?x` from an object position and probes
it as a subject. Built as sketched, that silently joins a city against the literal
`"0"`. An ID is only a term identity when paired with the position it came from,
so the representation has to carry a canonical ID, not a raw one.

Canonicalisation is still cheap, but it is the design rather than a footnote. The
four dictionary sections partition cleanly, so `shared | subjects | objects` lay
into one space with

```rust
canonical(id, Object) = if id <= shared { id } else { id + num_subjects }
canonical(id, Subject) = id
```

Predicates need their own range plus a small table, because a predicate IRI may
also appear as a subject or object and the two must unify. Predicate counts are
tiny -- 86 on WatDiv -- so that table is built once per graph.

### What it would take

1. **`hdt`** -- expose canonical term IDs, or enough of the section layout for
   `de` to compute them, plus per-graph `local id <-> canonical id` conversion.
2. **`de`** -- `InternalTerm` becomes the ID representation. 36 `Arc<str>` sites
   in `src/sparql.rs` and nothing outside it, so no oxigraph change and no pin
   bump.
3. **Blank nodes** -- `scoped_blank_node` currently does string surgery
   (`_:g<hex>_<id>`) to keep two graphs' blank nodes apart. With IDs a blank
   node's identity is naturally `(graph, id)`, so this gets simpler, but it is a
   rewrite rather than a port.
4. **Multi-graph** -- IDs are per file, so cross-graph joins cannot compare them.
   Single-graph fast path first (the CLI, and most server queries), then a
   per-graph `local id -> global symbol` table, backed by the interner already in
   `hdt` (`src/concurrent_interner.rs`).
5. **`quads_for_pattern_strings`** -- public, returns strings, no in-tree callers;
   externalises at the boundary.

### Two reasons to measure before building

**The whole-query share is unknown.** The table above is the *probe path*. C3
answers 48,802 rows in ~220 ms, about 4.5 us per row, while the probe path costs
~0.15 us per result. The gap is `spareval`'s tuple machinery -- hashing, cloning,
`InternalTuple` bookkeeping -- which this item does not touch. A 3x probe path
may move a join-heavy query far less than 3x. An attempt to model C3's join by
hand was discarded as unsound: it short-circuited where the evaluator builds a
cross-product, giving 76 rows against the real 48,802.

**There is a plausible regression.** `FILTER`, `ORDER BY` and aggregation call
`externalize_expression_term` per evaluation. Today the string is already in
hand; with IDs each evaluation pays a dictionary lookup, and both WatDiv and BSBM
have numeric filters. The `externalize_expression_term` hook exists to cache
this, but it has to be designed in rather than discovered afterwards.

The cheap next step is to instrument `internal_quads_for_pattern` to count probes
and materialised terms for C3 and BSBM q05. That turns "2.7-4.6x on a path of
unknown weight" into a whole-query estimate, for an hour's work against a change
that touches term representation everywhere.

---

## 3. Open each HDT once, not once per request

**Status: done.** Measured in [`BENCHMARKS.md`](BENCHMARKS.md): `get_snapshot`
drops 99.6% on the benchmark packages, and behind `de serve` a 5 GB package goes
from ~1 s of decode on every request to ~1 s once.

`AggregateHdt` keeps opened packages and hands them out as `Arc<HdtAny>`. Three
things made this straightforward:

* `HdtAny` is `Send + Sync` and has no interior mutability beyond its own
  caches, so queries share a handle with no lock at all. Only the cache map is
  locked, and only to look up.
* `serve` already stats every package on every request, to decide whether to
  re-`sync`. Keying handles on `(modification time, length)` reuses a check that
  was already being paid for, and a package rebuilt in place is noticed without
  the store being told.
* `HdtAny::size_in_bytes` counts decoded structures but not memory-mapped file
  pages -- those are the kernel's to reclaim -- so it is exactly the right thing
  to budget against.

Eviction is least-recently-used against a byte budget defaulting to a quarter of
system memory, and never touches a package the calling snapshot is about to use:
those are alive through that snapshot's `Arc` regardless, so evicting them would
free nothing and only force the next request to decode them again. The budget
therefore bounds what is retained *between* queries; the peak is unchanged at
the working set of a single request, which is what it always was.

`AggregateHdt::with_handle_cache_budget` sets the ceiling, and
`cached_handle_count` / `cached_handle_bytes` report what is held.

### Still open

* No CLI flag: the budget is settable through the library but `de serve` has no
  `--handle-cache-bytes`. A ten-line follow-up.
* Two requests racing on the same unopened package both decode it and the second
  insert wins. Avoiding that means making one wait on the other, which is worse
  for the common case.
* The CLI gains nothing: `do_query` already reuses one snapshot across the query
  files of an invocation, and a process-lifetime cache cannot outlive the
  process.

### What Comunica does

`ActorQuerySourceIdentifyHdt.run` opens the document once, holds the source in
`createdSources`, and only disposes it when an invalidation event arrives:

```ts
// ActorQuerySourceIdentifyHdt.ts:35
this.httpInvalidator.addInvalidateListener(({ url }) => { if (!url) this.clearCache(); });
```

### What `de` does

`serve.rs:675` and `serve.rs:706` call `store.get_snapshot(graph_filter)` per HTTP
request. `get_snapshot` (`src/sparql.rs:143`) calls `HdtAny::open_with_threshold`
for every selected graph, and the hybrid path bincode-decodes the **entire wavelet
tree** into memory each time (`hdt/src/triples/hybrid_cache.rs:637`). For a package
the size of `dblp.hdt` that is hundreds of megabytes of deserialisation per
request.

The struct comment says instances are per-request "for better concurrency", but
`HdtGeneric` has no interior mutability — the only `Mutex` in the crate is inside
the N-Triples interner — so sharing `&HdtAny` across request threads is sound.

### Plan

A `HashMap<PathBuf, Weak<HdtAny>>` cache inside `AggregateHdt`, handing out
`Arc<HdtAny>`, invalidated by `sync()` and by mtime. `AggregateHdtSnapshot` then
holds `Arc`s and costs nothing to build. This is the direct analogue of
Comunica's WeakRef-plus-invalidator pattern.

---

## 4. Local fixes in `de/src/sparql.rs`

**Status: done.** Measured in [`BENCHMARKS.md`](BENCHMARKS.md): -2% to -15% on
WatDiv, -8% to -31% on BSBM.

Landed in three steps so each could be measured on its own.

**4a. Delete `de`'s fork of the pattern dispatch.** The TODO at
`src/sparql.rs:364` said `indexed_triples_with_pattern` existed only because
`hdt::HdtGeneric::triples_with_pattern` tied the input `&str` lifetime to the
returned iterator. The `hdt` working tree had already relaxed that to
`sp: Option<&str>`, so the fork and its generic helper -- 129 lines -- were
replaced by a call to upstream. That also restored `TripleCache` and
`id_to_arc` (`hdt/src/hdt.rs:485`, `hdt/src/four_sect_dict.rs:156`), which
materialise a term in one allocation using a reusable scratch buffer and
memoise the last id per position; `de`'s copy did `id_to_string()` → `String`
→ `Arc::from(&str)`, two allocations and a copy per term per result.

This alone was a wash on WatDiv (-9% to +8%, depending on the template) because
it traded one cost for another, which is what 4b is about.

**4b. New in `hdt`: `triples_with_arc_pattern`.** The `&str` entry point copies
every bound term into a fresh `Arc` before it can hand it back out with each
matching triple. `de` had been avoiding that by holding `Arc<str>` and cloning,
so step 4a introduced one allocation per bound term per probe — visible as a
4-8% regression on the linear templates `L1` and `L2`. Upstream now has a
sibling entry point taking `Option<Arc<str>>`, with the id resolution and match
dispatch factored into a shared `triples_with_resolved_pattern`; both
regressions reversed into 7-10% improvements.

This is worth having upstream regardless of `de`: any engine driving HDT through
a nested-loop join is holding its bound terms as `Arc<str>` already.

**4c. Resolve graph selection once.** `internal_quads_for_pattern` used to
rebuild a filtered `Vec` by scanning the whole `hdts` map and re-allocate
`Arc::from(graph_name.as_str())` per graph, on every probe.
`AggregateHdtSnapshot` now stores graphs in a `Vec<LoadedGraph>` with their IRIs
pre-wrapped, plus precomputed default/named index lists and an IRI index; a
pattern call borrows a slice (or does one hash lookup for `GRAPH <iri>`) and
allocates nothing.

**Still open:** `hdt`'s `triples_with_pattern` `.unwrap()`s dictionary
extraction, so a corrupt package still panics the worker thread rather than
surfacing an error. Moving the fork upstream did not change that; upstream
carries its own `// TODO: improve error handling` on the same line.

---

## 5. Upstream: `hdt/src/sparql.rs` is now the worst of the three impls

**Status: not started. Still relevant, and it has grown a second defect.**
(`hdt` crate only, behind its optional `sparql` feature, which `de` does not
enable and which `hdt`'s CI does not build -- its workflow checks `cache` and
`sophia` only.)

Three lines carry both problems:

```rust
// hdt/src/sparql.rs:62
let v: Vec<_> = self.triples_with_pattern(ps, pp, po)
    .map(|at| at.map(|a| a.to_string()))
    .map(|[s, p, o]| Ok(InternalQuad { subject: s, predicate: p, object: o, graph_name: None }))
    .collect();
v.into_iter()
```

**The `collect` is unbounded.** `?s ?p ?o` over a large package builds the whole
result set in memory before returning anything -- 500M+ `String`s on a graph the
size of dblp. This existed only to escape the old input lifetime, and that
lifetime was relaxed as part of item 4a, so it can stream today. Returning
`Box<dyn Iterator<Item = ...> + 'a>` satisfies the trait's `impl Iterator + use<'a>`
and lets the named-graph early-return share the type.

**`InternalTerm = String` throws away item 4's work.** `triples_with_pattern`
yields `[Arc<str>; 3]`, and `.map(|a| a.to_string())` copies every one into a
fresh `String`. That is a second allocation per term on top of the one
`TripleCache` just made, and it defeats the cache's memoisation outright: when
consecutive triples repeat a term, `TripleCache` hands back the same `Arc`, and
this converts each clone into a separate `String` anyway. Changing `InternalTerm`
to `Arc<str>` removes the copy and restores the memoisation -- the same finding
as item 4b, in the crate that item 4b was added to.

### Is it still worth doing?

Yes, but as maintenance of the `hdt` crate rather than as `de` performance work.
Nothing in `de` runs this code: `de` has its own `QueryableDataset` over
`AggregateHdtSnapshot`, which streams, supports named graphs, and answers
cardinality. The audience is someone using the `hdt` crate's built-in SPARQL
support, for whom the `collect` is a straightforward out-of-memory bug.

That leaves a choice worth making deliberately rather than by default:

* **Fix in place** -- stream, and switch `InternalTerm` to `Arc<str>`. Small,
  and fixes the bug for that audience.
* **Lift `de`'s impl upstream** -- `StreamingInternalQuadIter` (`de/src/sparql.rs`)
  is strictly better: it streams, and it supports the named graphs `hdt`'s
  version rejects outright. The cardinality hook cannot come with it, because
  `hdt` depends on crates.io `spareval 0.2.6` and `estimated_cardinality` exists
  only in the DeciSym fork -- that part has to wait for the hook to be upstreamed.
* **Drop the feature** -- defensible given it is untested in CI and unused by its
  only in-tree consumer, and honest about which impl is the maintained one.

Whichever way it goes, the feature should be added to `hdt`'s CI matrix. It
compiles today only by luck: nothing builds it.

---

## Deliberately not adopted

- **`pageSize` / offset+limit paging** (Comunica `#36`, "Read HDT documents in
  growing pages instead of buffer-sized chunks", 1.5× on WatDiv-10). This is a
  workaround for the JS binding's offset-based API, where seeking to an offset is
  linear in the offset for patterns with a bound predicate, so small pages make a
  full traversal quadratic. Rust already returns a lazy iterator over the
  adjacency list; adding paging would be a regression.
- **`maxBufferSize` read-ahead.** Same reason — it buffers across an async FFI
  boundary that does not exist here.

Both still hold as of 2026-09-18. Nothing in items 1, 3 or 4 changed the shape of
the iterator these would wrap, and item 2 would make paging less attractive
still: the cheaper a probe gets, the more a buffering layer costs relative to it.

## Also borrowed: the benchmarks

Comunica added WatDiv-10/100 and BSBM 1k/10k HDT benchmarks with PR-versus-base
comparison in CI (`#38`–`#41`), which is how the 1.5× above was substantiated.
`de` now carries the same two workloads as criterion targets — see
`benches/watdiv.rs` and `benches/bsbm.rs`, and `BENCHMARKS.md` for how to run them
and for the recorded baseline.
