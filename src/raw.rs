use crate::laputa::{LapFileType, Laputa, Metadata};
use rusqlite::{params, Connection};

const TABLE_NAME: &str = "word";

struct Word {
    id: u64,
    name: String,
    text: Option<String>,
    binary: Option<Vec<u8>>,
}

pub struct RawDict {
    conn: Connection,
    cache: Vec<Word>,
    cache_size: usize,
}

impl RawDict {
    pub fn new(dest: &str) -> Self {
        let conn = Connection::open(dest).unwrap();
        conn.execute(
            "CREATE TABLE word (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                name   TEXT,
                text   TEXT,
                binary BLOB,
                plain  TEXT
            )",
            [],
        )
        .unwrap();
        Self {
            conn,
            cache: Vec::new(),
            cache_size: 200,
        }
    }

    pub fn from(dest: &str) -> Self {
        let conn = Connection::open(dest).unwrap();
        Self {
            conn,
            cache: Vec::new(),
            cache_size: 200,
        }
    }

    pub fn flush(&mut self, is_text: bool) {
        let field = if is_text { "text" } else { "binary" };
        let tx = self.conn.transaction().unwrap();
        let sql = format!(
            "INSERT INTO {} (name, {}) VALUES ($1, $2)",
            TABLE_NAME, field
        );
        {
            let mut stmt = tx.prepare(sql.as_str()).unwrap();
            for wd in &self.cache {
                if field == "text" {
                    stmt.execute(params![wd.name, wd.text]).unwrap();
                } else {
                    stmt.execute(params![wd.name, wd.binary]).unwrap();
                }
            }
        }
        tx.commit().unwrap();
        self.cache.clear();
    }

    pub fn insert(&mut self, name: &str, value: &[u8], is_text: bool) {
        if is_text {
            self.cache.push(Word {
                id: 0,
                name: String::from(name),
                text: Some(String::from_utf8(value.to_vec()).unwrap()),
                binary: None,
            });
        } else {
            self.cache.push(Word {
                id: 0,
                name: String::from(name),
                text: None,
                binary: Some(value.to_vec()),
            });
        }
        if self.cache.len() >= self.cache_size {
            self.flush(is_text);
        }
    }

    pub fn to_laputa(&self, dest: &str, ft: LapFileType) {
        let meta = Metadata::new();
        let mut lp = Laputa::new(meta, ft);
        let mut offset = 0;
        let limit = 100;
        loop {
            let mut stmt = self
                .conn
                .prepare(format!("SELECT * FROM {} LIMIT $1 OFFSET $2", TABLE_NAME).as_str())
                .unwrap();
            let mut list = stmt.query(params![offset, limit]).unwrap();
            let mut rows: Vec<Word> = Vec::new();
            while let Ok(Some(row)) = list.next() {
                rows.push(Word {
                    id: row.get(0).unwrap(),
                    name: row.get(1).unwrap(),
                    text: row.get(2).unwrap(),
                    binary: row.get(3).unwrap(),
                })
            }
            let mut counter = 0;
            for word in rows {
                let value = match ft {
                    LapFileType::Word => word.text.unwrap().as_bytes().to_vec(),
                    LapFileType::Resource => word.binary.unwrap(),
                };
                lp.input_word(word.name, value);
                counter = counter + 1;
            }
            if counter < limit {
                break;
            }
            offset = offset + limit;
        }
        lp.save(dest);
    }
}
