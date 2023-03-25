use std::{cell::RefCell, rc::Rc};

use tracing::{error, info, instrument, warn};

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
            dictionaries: vec![],
            cache_id: 0,
            cache: Rc::new(RefCell::new(LruCache::new(cap))),
        }
    }

    #[instrument(skip(self))]
    pub fn add(&mut self, path: &str) -> LaputaResult<(u32, Metadata)> {
        let (dict, cache_id) = Dictionary::new(path, &self.cache, self.cache_id)?;
        let metadata = dict.metadata();
        self.cache_id = cache_id + 1;
        let dict_id = self.dict_id;
        self.dictionaries.push((dict_id, dict));
        self.dict_id += 1;
        info!("dict ID: {}", dict_id);
        Ok((dict_id, metadata))
    }

    #[instrument(skip(self))]
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
        } else {
            info!("Not exists");
        }
    }

    #[instrument(skip(self))]
    pub fn clear(&mut self) {
        self.dictionaries.clear();
    }

    #[instrument(skip(self))]
    pub fn search(&mut self, id: u32, word: &str, limit: usize) -> Vec<String> {
        if word.len() == 0 {
            warn!("Empty word");
            return vec![];
        }
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search(word, limit);
            }
        }
        error!("Invalid id");
        vec![]
    }

    #[instrument(skip(self))]
    pub fn search_word(&mut self, id: u32, name: &str) -> Option<String> {
        if name.len() == 0 {
            warn!("Empty name");
            return None;
        }
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search_word(name);
            }
        }
        error!("Invalid id");
        None
    }

    #[instrument(skip(self))]
    pub fn search_resource(&mut self, id: u32, name: &str) -> Option<Vec<u8>> {
        if name.len() == 0 {
            warn!("Empty name");
            return None;
        }
        for (_, d) in self.dictionaries.iter_mut().enumerate() {
            if d.0 == id {
                return d.1.search_resource(name);
            }
        }
        error!("Invalid id");
        None
    }

    #[instrument(skip(self))]
    pub fn get_static_files(&self, id: u32) -> Option<(String, String)> {
        for (i, d) in &self.dictionaries {
            if *i == id {
                return Some((d.js.clone(), d.css.clone()));
            }
        }
        error!("Invalid id");
        None
    }

    #[instrument(skip(self))]
    pub fn resize_cache(&mut self, cap: u64) {
        info!("Resize to {}B", cap);
        self.cache.borrow_mut().resize(cap);
    }
}
