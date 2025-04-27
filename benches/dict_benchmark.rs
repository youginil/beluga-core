use beluga_core::{
    dictionary::{DictFile, DictNode},
    lru::LruCache,
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

fn dict_benchmark(c: &mut Criterion) {
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

criterion_group!(benches, dict_benchmark);
criterion_main!(benches);
