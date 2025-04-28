use beluga_core::{
    beluga::{EntryKey, EntryValue},
    dictionary::{DictFile, DictNode},
    lru::LruCache,
    tree::{Node, Record},
};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};
use tokio::{runtime::Runtime, sync::RwLock};

static TEST_DATA_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| std::env::current_dir().unwrap().join("test_data"));

async fn parse(cache_id: u32) {
    let file = TEST_DATA_DIR.join("index.bel");
    DictFile::new(file, cache_id).await.unwrap();
}

async fn search(dict: &mut DictFile, cache: Option<Arc<RwLock<LruCache<(u32, u64), DictNode>>>>) {
    dict.search(cache, "name", true, 10).await;
}

fn dict_bench(c: &mut Criterion) {
    let cache_id = 1;

    let mut group = c.benchmark_group("dictionary");
    let rt = Runtime::new().unwrap();

    group.bench_function("parse", |b| {
        b.to_async(&rt).iter(|| async {
            parse(cache_id).await;
        })
    });

    let dict = Runtime::new().unwrap().block_on(async {
        let file = TEST_DATA_DIR.join("index.bel");
        DictFile::new(file, 1).await.unwrap()
    });
    let dict = Arc::new(Mutex::new(dict));

    group.bench_function("search", |b| {
        b.to_async(&rt).iter(|| async {
            let mut dict = dict.lock().unwrap();
            search(&mut dict, None).await;
        })
    });
}

fn lru_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("LRU");

    let mut lru = LruCache::new(100 * 1024 * 1024);
    let mut node = Node::<EntryKey, EntryValue>::default();
    node.records = vec![
        Record {
            key: EntryKey("hello".to_string()),
            value: Some(EntryValue(vec![0; 1024]))
        };
        64
    ];
    let dnode = DictNode::new(node);
    let mut offset = 1;
    let cache_id = 1;

    group.bench_function("put", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let oft = offset;
                offset = offset + 1;
                lru.put((cache_id, oft), dnode.clone());
            }
        });
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                lru.get(&(1000, 1000));
            }
        });
    });
}

criterion_group!(benches, dict_bench, lru_bench);
criterion_main!(benches);
