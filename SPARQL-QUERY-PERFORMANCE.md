# SPARQL-over-HDT query performance: findings and plan

Status: analysis complete, work in progress.
Date: 2026-09-16.

This note records what a read of [comunica-feature-hdt] (Comunica's SPARQL-over-HDT
support) suggests for `de` and for the [`hdt`] crate it builds on, why each item
matters, and how each one will be measured. It is a working document: the
"Status" line on each item is updated as work lands.

**Local build overrides.** Items 1 and 4 change `hdt` and the DeciSym oxigraph
fork as well as `de`. Until those are pushed and the pins in `Cargo.toml` are
bumped, `de` builds against working trees through a git-ignored
`.cargo/config.toml`:

| Crate | Working tree | Branch |
| --- | --- | --- |
| `hdt` | `../hdt` | `perf-qwt-opus-max` (uncommitted) |
| `spareval`, `sparopt`, `spargebra`, `sparesults`, `oxrdfxml` | `../oxigraph-cardinality` | `cardinality-estimator-hook`, a worktree off the pinned `ed7335a2` |

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

| Comunica does | `de` today |
| --- | --- |
| Asks HDT for a real cardinality per pattern and plans joins from it | Plans from a fixed 8-entry constant table that never sees the data |
| Hands bindings straight out of the source, no intermediate term objects | Round-trips every join value through a dictionary string and back |
| Opens each HDT document once and keeps it until invalidated | Re-opens every graph on every request |

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

## 2. Stop round-tripping join values through dictionary strings

**Status: not started.**

### What Comunica does

Commit `623d9af`, "Avoid creation of intermediary triple objects", replaced
`searchTriples` with `searchBindings` so the source produces bindings directly and
no intermediate triple objects are allocated per result.

### What `de` does

`de`'s `InternalTerm` is `Arc<str>`. `spareval` passes bound join values back into
the next pattern as `InternalTerm`s, so on every probe
`indexed_triples_with_pattern_generic` does:

```rust
// src/sparql.rs:404
let id = hdt.dict.string_to_id(term.as_ref(), IdKind::Subject);
```

on a string that the *previous* pattern produced with `id_to_string` from the same
dictionary. Each such call is a front-coded PFC binary search plus a block
decompression, per bound term, per probe — to recover an integer HDT had already
handed out. Joins then hash and compare `Arc<str>` rather than integers.

`hdt` already exposes the raw material: `triple_ids_with_pattern` and
`triple_ids_with_id_pattern` (`src/hdt.rs:244`, `src/hdt.rs:260`).

### Sketch

```rust
enum HdtTerm { Id(u32), Str(Arc<str>) }   // Str only for terms absent from the dictionary
```

`Eq`/`Hash` stay sound: a term that is not in the dictionary can never equal one
that is, so the two variants never need to compare equal across the split.

Canonicalising ids across sections is cheap. `four_sect_dict.rs:175` shows subject
and object ids coincide below `shared_size`, so subject/object canonicalisation is
O(1) arithmetic. Only predicates need a precomputed table, and predicate counts
are small (thousands, not millions).

`externalize_term` then runs only on terms that actually reach the result set —
the same shape as Comunica's `searchBindings` change.

Multi-graph is the wrinkle: ids are per file, so cross-graph joins cannot compare
raw ids. Staging:

1. Single-HDT fast path (covers the CLI and most server queries).
2. Multi-graph via a per-graph `local id -> global symbol` table, backed by the
   interner already in `hdt` (`src/concurrent_interner.rs`).

---

## 3. Open each HDT once, not once per request

**Status: not started.**

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

## 5. Upstream: `hdt/src/sparql.rs` materialises whole result sets

**Status: not started.** (`hdt` crate only; `de` does not enable its `sparql` feature.)

```rust
// hdt/src/sparql.rs:62
let v: Vec<_> = self.triples_with_pattern(ps, pp, po)
    .map(|at| at.map(|a| a.to_string()))
    .map(|[s, p, o]| Ok(InternalQuad { subject: s, predicate: p, object: o, graph_name: None }))
    .collect();
v.into_iter()
```

`?s ?p ?o` over a large package builds the entire result set in memory before
returning. The `collect` existed only to escape the old input lifetime; with
`triples_with_pattern` now taking `Option<&str>` it can stream.

`de`'s `StreamingInternalQuadIter` (`src/sparql.rs:323`) is the shape to lift
upstream, and it also adds the named-graph support `hdt`'s version rejects.

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

## Also borrowed: the benchmarks

Comunica added WatDiv-10/100 and BSBM 1k/10k HDT benchmarks with PR-versus-base
comparison in CI (`#38`–`#41`), which is how the 1.5× above was substantiated.
`de` now carries the same two workloads as criterion targets — see
`benches/watdiv.rs` and `benches/bsbm.rs`, and `BENCHMARKS.md` for how to run them
and for the recorded baseline.
