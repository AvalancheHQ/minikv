use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use minikv::common::hash::{blake3_hash, hrw_hash, select_replicas, shard_key, ConsistentHashRing};

fn bench_blake3_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("blake3_hash");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let data = vec![0u8; *size];
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| blake3_hash(black_box(data)))
        });
    }

    group.finish();
}

fn bench_shard_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_key");

    for num_shards in [16, 64, 256, 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_shards),
            num_shards,
            |b, &num_shards| {
                b.iter(|| shard_key(black_box("test-key-12345"), black_box(num_shards)))
            },
        );
    }

    group.finish();
}

fn bench_hrw_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hrw_hash");

    for num_nodes in [3, 5, 10, 20].iter() {
        let nodes: Vec<String> = (0..*num_nodes).map(|i| format!("node-{}", i)).collect();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_nodes),
            &nodes,
            |b, nodes| b.iter(|| hrw_hash(black_box("test-key"), black_box(nodes))),
        );
    }

    group.finish();
}

fn bench_select_replicas(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_replicas");

    let nodes: Vec<String> = (0..10).map(|i| format!("node-{}", i)).collect();

    for num_replicas in [1, 3, 5].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_replicas),
            num_replicas,
            |b, &num_replicas| {
                b.iter(|| {
                    select_replicas(
                        black_box("test-key"),
                        black_box(&nodes),
                        black_box(num_replicas),
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_consistent_hash_ring_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("consistent_hash_ring_lookup");

    for num_shards in [64, 256, 1024].iter() {
        let mut ring = ConsistentHashRing::new(*num_shards);
        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];
        ring.rebalance(&nodes, 2);

        group.bench_with_input(BenchmarkId::from_parameter(num_shards), &ring, |b, ring| {
            b.iter(|| ring.get_nodes(black_box("test-key-12345")))
        });
    }

    group.finish();
}

fn bench_consistent_hash_ring_rebalance(c: &mut Criterion) {
    let mut group = c.benchmark_group("consistent_hash_ring_rebalance");

    for num_shards in [64, 256, 1024].iter() {
        let nodes: Vec<String> = (0..5).map(|i| format!("node-{}", i)).collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_shards),
            num_shards,
            |b, &num_shards| {
                b.iter(|| {
                    let mut ring = ConsistentHashRing::new(num_shards);
                    ring.rebalance(black_box(&nodes), black_box(3));
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_blake3_hash,
    bench_shard_key,
    bench_hrw_hash,
    bench_select_replicas,
    bench_consistent_hash_ring_lookup,
    bench_consistent_hash_ring_rebalance
);
criterion_main!(benches);
