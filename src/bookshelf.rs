use crate::{
    error::{LaputaError, LaputaResult},
    laputa::Laputa,
};

pub type ID = u32;

struct Dictionary {
    id: ID,
    path: String,
    laputa: Laputa,
}

pub struct Bookshelf {
    id: ID,
    dictionaries: Vec<Dictionary>,
}

impl Bookshelf {
    pub fn new() -> Self {
        Self {
            id: 0,
            dictionaries: Vec::new(),
        }
    }

    pub fn add(&mut self, path: &String) -> LaputaResult<ID> {
        let laputa = match Laputa::from_file(path.as_str()) {
            Ok(l) => l,
            Err(e) => return Err(e),
        };
        self.id = self.id + 1;
        self.dictionaries.push(Dictionary {
            id: self.id,
            path: path.clone(),
            laputa,
        });
        Ok(self.id)
    }

    pub fn remove(&mut self, id: ID) {
        let mut index: usize = 0;
        let mut exists = false;
        for (i, item) in self.dictionaries.iter().enumerate() {
            if id == item.id {
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

    pub fn search(&self, id: ID, word: &str) -> LaputaResult<String> {
        Err(LaputaError::NotFound)
    }
}
