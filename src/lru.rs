struct CacheNode {
    key: String,
    value: DictNode,
    prev: *mut CacheNode,
    next: *mut CacheNode
}

struct LRUCache {
}