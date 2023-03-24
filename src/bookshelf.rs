use std::{cell::RefCell, rc::Rc};

use crate::{
    dictionary::{Dictionary, LruCacheRef},
    error::LaputaResult,
    laputa::Metadata,
    lru::LruCache,
};

pub struct Bookshelf {
    dict_id: u32,
    dictionaries: Vec<(u32, Dictionary)>,
    cache_id: u32,
    cache: LruCacheRef,
}

impl Bookshelf {
    pub fn new(cap: u64) -> Self {
        Self {
            dict_id: 0,
            dictionaries: Vec::new(),
            cache_id: 0,
            cache: Rc::new(RefCell::new(LruCache::new(cap))),
        }
    }

    pub fn add(&mut self, path: &str) -> LaputaResult<(u32, Metadata)> {
        let (dict, cache_id) = Dictionary::new(path, &self.cache, self.cache_id)?;
        let metadata = dict.metadata();
        self.cache_id = cache_id + 1;
        self.dictionaries.push((self.dict_id, dict));
        self.dict_id += 1;
        Ok((self.dict_id, metadata))
    }

    pub fn remove(&mut self, id: u32) {
        let mut index: usize = 0;
        let mut exists = false;
        for (i, item) in self.dictionaries.iter().enumerate() {
            if id == item.0 {
                index = i;
                exists = true;
            }
        }
        if exists {
            self.dictionaries.remove(index);
        }
    }

    pub fn clear(&mut self) {
        self.dictionaries.clear();
    }

    pub fn search(&mut self, id: u32, word: &str, limit: usize) -> Vec<String> {
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search(word, limit);
            }
        }
        vec![]
    }

    pub fn search_word(&mut self, id: u32, name: &str) -> Option<String> {
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search_word(name);
            }
        }
        None
    }

    pub fn search_resource(&mut self, id: u32, name: &str) -> Option<Vec<u8>> {
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search_resource(name);
            }
        }
        None
    }

    pub fn get_static_files(&self, id: u32) -> Option<(String, String)> {
        for (i, d) in &self.dictionaries {
            if *i == id {
                return Some((d.js.clone(), d.css.clone()));
            }
        }
        None
    }

    pub fn resize_cache(&mut self, cap: u64) {
        self.cache.borrow_mut().resize(cap);
    }
}
