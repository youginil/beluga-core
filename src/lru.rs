use core::hash::Hash;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

pub trait SizedValue {
    fn size(&self) -> u64;
}

type LruNodeRef<K, V> = Rc<RefCell<LruNode<K, V>>>;
pub type LruValue<V> = Rc<RefCell<V>>;

#[derive(Debug)]
struct LruNode<K, V> {
    key: K,
    val: LruValue<V>,
    size: u64,
    prev: Option<LruNodeRef<K, V>>,
    next: Option<LruNodeRef<K, V>>,
}

#[derive(Debug)]
pub struct LruCache<K, V: SizedValue> {
    cap: u64,
    len: u64,
    map: HashMap<K, LruNodeRef<K, V>>,
    head: Option<LruNodeRef<K, V>>,
    tail: Option<LruNodeRef<K, V>>,
}

impl<K: Hash + Eq + Copy, V: SizedValue> LruCache<K, V> {
    pub fn new(cap: u64) -> Self {
        Self {
            cap,
            len: 0,
            map: HashMap::new(),
            head: None,
            tail: None,
        }
    }
    pub fn put(&mut self, key: K, val: V) -> LruValue<V> {
        match self.map.get_mut(&key) {
            Some(value) => {
                let mut v = value.borrow_mut();
                v.val = Rc::new(RefCell::new(val));
                match &v.next {
                    Some(n) => {
                        if let Some(p) = &v.prev {
                            p.borrow_mut().next = Some(Rc::clone(&n));
                            n.borrow_mut().prev = Some(Rc::clone(&p));
                            v.prev = None;
                            v.next = self.head.clone();
                            self.head = Some(Rc::clone(value));
                        }
                    }
                    None => {
                        if let Some(p) = &v.prev {
                            p.borrow_mut().next = None;
                            value.borrow_mut().next = self.head.clone();
                            self.head = Some(Rc::clone(value));
                            self.tail = Some(Rc::clone(&p));
                        }
                    }
                }
            }
            None => {
                let size = val.size();
                let v = LruNode {
                    key,
                    val: Rc::new(RefCell::new(val)),
                    size,
                    prev: None,
                    next: self.head.clone(),
                };
                let value = Rc::new(RefCell::new(v));
                match &self.head {
                    Some(h) => {
                        h.borrow_mut().prev = Some(Rc::clone(&value));
                        value.borrow_mut().next = Some(Rc::clone(&h));
                    }
                    None => {
                        self.tail = Some(Rc::clone(&value));
                    }
                }
                self.head = Some(Rc::clone(&value));
                self.map.insert(key, Rc::clone(&value));
            }
        }
        self.shrink();
        self.head.as_mut().unwrap().borrow().val.clone()
    }

    pub fn get(&self, key: &K) -> Option<LruValue<V>> {
        match self.map.get(key) {
            Some(v) => Some(v.borrow().val.clone()),
            None => None,
        }
    }

    pub fn resize(&mut self, size: u64) {
        self.cap = size;
        self.shrink();
    }

    fn shrink(&mut self) {
        while self.len > self.cap {
            if let Some(v) = &self.tail {
                let tail = Rc::clone(v);
                let key = tail.borrow().key;
                self.map.remove(&key);
                self.tail = tail.borrow().prev.clone();
                self.len -= tail.borrow().size;
            } else {
                break;
            }
        }
    }
}
