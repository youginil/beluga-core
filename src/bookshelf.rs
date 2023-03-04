use crate::{dictionary::Dictionary, error::LaputaResult};

pub type ID = u32;

pub struct Bookshelf {
    id: ID,
    dictionaries: Vec<(ID, Dictionary)>,
}

impl Bookshelf {
    pub fn new() -> Self {
        Self {
            id: 0,
            dictionaries: Vec::new(),
        }
    }

    pub fn add(&mut self, path: &str) -> LaputaResult<ID> {
        let id = self.id + 1;
        let dict = Dictionary::new(path)?;
        self.dictionaries.push((id, dict));
        self.id = id;
        Ok(self.id)
    }

    pub fn remove(&mut self, id: ID) {
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

    pub fn search(&self, id: ID, word: &str) -> Vec<String> {
        for d in &self.dictionaries {
            if d.0 == id {
                return d.1.search(word);
            }
        }
        vec![]
    }
}
