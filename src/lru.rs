use core::hash::Hash;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

trait SizedValue {
    fn size(&self) -> usize;
}

type LruNodeRef<K, V> = Rc<RefCell<LruNode<K, V>>>;

struct LruNode<K, V> {
    key: K,
    val: V,
    size: usize,
    prev: Option<LruNodeRef<K, V>>,
    next: Option<LruNodeRef<K, V>>,
}

struct LruCache<K, V: SizedValue> {
    cap: usize,
    len: usize,
    map: HashMap<K, LruNodeRef<K, V>>,
    head: Option<LruNodeRef<K, V>>,
    tail: Option<LruNodeRef<K, V>>,
}

impl<K: Hash + Eq + Copy, V: SizedValue> LruCache<K, V> {
    pub fn put(&mut self, key: K, val: V) {
        match self.map.get_mut(&key) {
            Some(value) => {
                let mut v = value.borrow_mut();
                v.val = val;
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
                    val,
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
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            Some(v) => unsafe { Some(&(*v.as_ptr()).val) },
            None => None,
        }
    }

    pub fn resize(&mut self, size: usize) {
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
