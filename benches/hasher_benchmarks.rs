use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use swactor::actor::ActorAddress;

// ─── Reproduce the identity hasher for benchmarking ─────────────────────────
// (The real one is pub(crate) in delivery.rs — recreate here for bench access)

struct AddrHasher(u64);

impl Hasher for AddrHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {}

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

#[derive(Default, Clone)]
struct AddrBuildHasher;

impl BuildHasher for AddrBuildHasher {
    type Hasher = AddrHasher;
    #[inline]
    fn build_hasher(&self) -> AddrHasher {
        AddrHasher(0)
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn random_addresses(n: usize) -> Vec<ActorAddress> {
    (0..n).map(|_| ActorAddress::new_random()).collect()
}

// ─── Hash benchmarks ────────────────────────────────────────────────────────

fn bench_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash");

    let addr = ActorAddress::new_random();

    // Default hasher (SipHash) — hashes all 32 bytes via derived Hash,
    // but our custom Hash impl only writes 8 bytes
    group.bench_function("siphash_custom_hash", |b| {
        b.iter(|| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            addr.hash(&mut hasher);
            std::hint::black_box(hasher.finish())
        })
    });

    // Identity hasher — reads the u64 from our custom Hash impl directly
    group.bench_function("identity", |b| {
        b.iter(|| {
            let mut hasher = AddrHasher(0);
            addr.hash(&mut hasher);
            std::hint::black_box(hasher.finish())
        })
    });

    group.finish();
}

// ─── Lookup benchmarks ─────────────────────────────────────────────────────

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup");

    for &size in &[100, 1000] {
        let addrs = random_addresses(size);
        let lookup_targets: Vec<ActorAddress> = addrs.iter().cloned().collect();

        // SipHash HashMap (but with our custom 8-byte Hash impl)
        let sip_map: HashMap<ActorAddress, usize> =
            addrs.iter().enumerate().map(|(i, a)| (*a, i)).collect();

        group.bench_with_input(
            BenchmarkId::new("siphash", size),
            &size,
            |b, _| {
                let mut idx = 0;
                b.iter(|| {
                    let addr = &lookup_targets[idx % lookup_targets.len()];
                    idx += 1;
                    std::hint::black_box(sip_map.get(addr))
                })
            },
        );

        // Identity HashMap
        let identity_map: HashMap<ActorAddress, usize, AddrBuildHasher> = {
            let mut m = HashMap::with_capacity_and_hasher(size, AddrBuildHasher);
            for (i, a) in addrs.iter().enumerate() {
                m.insert(*a, i);
            }
            m
        };

        group.bench_with_input(
            BenchmarkId::new("identity", size),
            &size,
            |b, _| {
                let mut idx = 0;
                b.iter(|| {
                    let addr = &lookup_targets[idx % lookup_targets.len()];
                    idx += 1;
                    std::hint::black_box(identity_map.get(addr))
                })
            },
        );
    }

    group.finish();
}

// ─── Insert benchmarks ─────────────────────────────────────────────────────

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    let addrs = random_addresses(1000);

    group.bench_function("siphash", |b| {
        b.iter_batched(
            || addrs.clone(),
            |addrs| {
                let mut m: HashMap<ActorAddress, usize> = HashMap::with_capacity(addrs.len());
                for (i, a) in addrs.iter().enumerate() {
                    m.insert(*a, i);
                }
                std::hint::black_box(m.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("identity", |b| {
        b.iter_batched(
            || addrs.clone(),
            |addrs| {
                let mut m: HashMap<ActorAddress, usize, AddrBuildHasher> =
                    HashMap::with_capacity_and_hasher(addrs.len(), AddrBuildHasher);
                for (i, a) in addrs.iter().enumerate() {
                    m.insert(*a, i);
                }
                std::hint::black_box(m.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_hash, bench_lookup, bench_insert);
criterion_main!(benches);
