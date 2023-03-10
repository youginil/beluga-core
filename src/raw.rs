use crate::laputa::{LapFileType, Laputa, Metadata, EXT_RAW_WORD};
use rusqlite::{params, Connection};

const TABLE_NAME: &str = "word";

#[derive(Debug)]
struct Word {
    name: String,
    text: Option<String>,
    binary: Option<Vec<u8>>,
}

pub struct RawDict {
    file_type: LapFileType,
    conn: Connection,
    cache: Vec<Word>,
    cache_size: usize,
}

impl RawDict {
    pub fn new(filepath: &str) -> Self {
        let file_type = if filepath.ends_with(EXT_RAW_WORD) {
            LapFileType::Word
        } else {
            LapFileType::Resource
        };
        let conn = Connection::open(filepath).unwrap();
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
        conn.execute("CREATE INDEX name_idx ON word (name)", [])
            .unwrap();
        Self {
            file_type,
            conn,
            cache: Vec::new(),
            cache_size: 200,
        }
    }

    pub fn from(filepath: &str) -> Self {
        let file_type = if filepath.ends_with(EXT_RAW_WORD) {
            LapFileType::Word
        } else {
            LapFileType::Resource
        };
        let conn = Connection::open(filepath).unwrap();
        Self {
            file_type,
            conn,
            cache: Vec::new(),
            cache_size: 200,
        }
    }

    pub fn total(&self) -> u64 {
        let mut stmt = self
            .conn
            .prepare("SELECT count(*) as total from word")
            .unwrap();
        let mut rows = stmt.query(params![]).unwrap();
        let row = rows.next().unwrap().unwrap();
        row.get(0).unwrap()
    }

    pub fn flush(&mut self) {
        let field = if self.file_type == LapFileType::Word {
            "text"
        } else {
            "binary"
        };
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

    pub fn insert(&mut self, name: &str, value: &[u8]) {
        if self.file_type == LapFileType::Word {
            self.cache.push(Word {
                name: String::from(name),
                text: Some(String::from_utf8(value.to_vec()).unwrap()),
                binary: None,
            });
        } else {
            self.cache.push(Word {
                name: String::from(name),
                text: None,
                binary: Some(value.to_vec()),
            });
        }
        if self.cache.len() >= self.cache_size {
            self.flush();
        }
    }

    pub fn to_laputa<F>(&self, dest: &str, mut step: F)
    where
        F: FnMut(),
    {
        let meta = Metadata::new();
        let mut lp = Laputa::new(meta, self.file_type);
        let mut id = 0;
        let limit = 100;
        loop {
            let mut stmt = self
                .conn
                .prepare(
                    format!(
                        "SELECT * FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
                        TABLE_NAME
                    )
                    .as_str(),
                )
                .unwrap();
            let mut list = stmt.query(params![id, limit]).unwrap();
            let mut rows: Vec<Word> = Vec::new();
            while let Ok(Some(row)) = list.next() {
                id = row.get(0).unwrap();
                rows.push(Word {
                    name: row.get(1).unwrap(),
                    text: row.get(2).unwrap(),
                    binary: row.get(3).unwrap(),
                })
            }
            let mut counter = 0;
            for word in rows {
                let value = match self.file_type {
                    LapFileType::Word => word.text.unwrap().as_bytes().to_vec(),
                    LapFileType::Resource => word.binary.unwrap(),
                };
                lp.input_word(word.name, value);
                counter = counter + 1;
                step();
            }
            if counter < limit {
                break;
            }
        }
        lp.save(dest);
    }
}
