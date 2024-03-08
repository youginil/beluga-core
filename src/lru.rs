use core::hash::Hash;
use std::{collections::HashMap, ptr::NonNull};

pub trait SizedValue {
    fn size(&self) -> u64;
}

#[derive(Debug)]
struct LruNode<K, V: Clone> {
    key: K,
    val: V,
    size: u64,
    prev: Option<NonNull<LruNode<K, V>>>,
    next: Option<NonNull<LruNode<K, V>>>,
}

#[derive(Debug)]
pub struct LruCache<K, V: SizedValue + Clone> {
    cap: u64,
    len: u64,
    map: NonNull<HashMap<K, NonNull<LruNode<K, V>>>>,
    head: Option<NonNull<LruNode<K, V>>>,
    tail: Option<NonNull<LruNode<K, V>>>,
}

unsafe impl<K, V: SizedValue + Clone + Send> Send for LruCache<K, V> {}
unsafe impl<K, V: SizedValue + Clone + Sync> Sync for LruCache<K, V> {}

impl<K: Hash + Eq + Copy, V: SizedValue + Clone> LruCache<K, V> {
    pub fn new(cap: u64) -> Self {
        let map = Box::new(HashMap::new());
        let map_ptr = NonNull::from(Box::leak(map));
        Self {
            cap,
            len: 0,
            map: map_ptr,
            head: None,
            tail: None,
        }
    }

    pub fn put(&mut self, key: K, val: V) -> V {
        match unsafe { self.map.as_mut().get_mut(&key) } {
            Some(v) => {
                let node = unsafe { v.as_mut() };
                node.val = val;
                match node.next {
                    Some(mut n) => {
                        if let Some(mut p) = node.prev {
                            unsafe { p.as_mut().next = Some(n) };
                            unsafe { n.as_mut().prev = Some(p) };
                            node.prev = None;
                            node.next = self.head;
                            self.head = Some(*v);
                        }
                    }
                    None => {
                        if let Some(mut p) = node.prev {
                            unsafe { p.as_mut().next = None };
                            node.next = self.head;
                            self.head = Some(*v);
                            self.tail = Some(p);
                        }
                    }
                }
            }
            None => {
                let size = val.size();
                let node = Box::new(LruNode {
                    key,
                    val,
                    size,
                    prev: None,
                    next: self.head,
                });
                let mut node_ptr = NonNull::from(Box::leak(node));
                match self.head {
                    Some(mut h) => {
                        unsafe { h.as_mut().prev = Some(node_ptr) };
                        unsafe { node_ptr.as_mut().next = Some(h) };
                    }
                    None => {
                        self.tail = Some(node_ptr);
                    }
                }
                self.head = Some(node_ptr);
                unsafe { self.map.as_mut().insert(key, node_ptr) };
            }
        }
        self.shrink();
        unsafe { self.head.unwrap().as_ref().val.clone() }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match unsafe { self.map.as_ref().get(key) } {
            Some(v) => Some(unsafe { v.as_ref().val.clone() }),
            None => None,
        }
    }

    pub fn resize(&mut self, size: u64) {
        self.cap = size;
        self.shrink();
    }

    fn shrink(&mut self) {
        while self.len > self.cap {
            if let Some(mut tail) = self.tail {
                let tail_node = unsafe { tail.as_mut() };
                let key = tail_node.key;
                unsafe { self.map.as_mut().remove(&key) };
                self.tail = tail_node.prev;
                self.len -= tail_node.size;
            } else {
                break;
            }
        }
    }
}
