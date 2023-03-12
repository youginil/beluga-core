use crate::error::{LaputaError, LaputaResult};
use crate::raw::RawDict;
use crate::tree::{Serializable, Tree};
use crate::utils::*;
use pbr::ProgressBar;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::Path;

const LEAF_NODE_SIZE: usize = 64 * 1024;
const INDEX_NODE_SIZE: usize = 64 * 1024;
pub const EXT_WORD: &str = "lpw";
pub const EXT_RESOURCE: &str = "lpr";
pub const EXT_RAW_WORD: &str = "lpwdb";
pub const EXT_RAW_RESOURCE: &str = "lprdb";

pub fn parse_file_type(file: &str) -> LaputaResult<LapFileType> {
    let ext = file.split(".").last();
    match ext {
        Some(EXT_WORD) => Ok(LapFileType::Word),
        Some(EXT_RESOURCE) => Ok(LapFileType::Resource),
        _ => Err(LaputaError::InvalidDictName),
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub spec: u8,
    pub version: String,
    pub word_num: u64,
    pub author: String,
    pub email: String,
    pub create_time: String,
    pub comment: String,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            spec: 1,
            version: String::from(""),
            word_num: 0,
            author: String::from(""),
            email: String::from(""),
            create_time: String::from(""),
            comment: String::from(""),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LapFileType {
    Word,
    Resource,
}

#[derive(Clone)]
pub struct Key(pub String);

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.to_lowercase().partial_cmp(&other.0.to_lowercase())
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.to_lowercase().cmp(&other.0.to_lowercase())
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_lowercase() == other.0.to_lowercase()
    }
}

impl Eq for Key {}

impl Serializable for Key {
    fn bytes(&self) -> Vec<u8> {
        self.0.bytes().collect()
    }

    fn size(&self) -> usize {
        self.0.bytes().len()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self(String::from_utf8(bytes.to_vec()).unwrap())
    }
}

pub struct Value(Vec<u8>);

impl Serializable for Value {
    fn bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn size(&self) -> usize {
        self.0.len()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

pub struct Laputa {
    pub metadata: Metadata,
    pub file_type: LapFileType,
    pub tree: Tree<Key, Value>,
}

impl Laputa {
    pub fn new(metadata: Metadata, file_type: LapFileType) -> Self {
        Self {
            metadata,
            file_type,
            tree: Tree::new(64 * 1024, 64 * 1024),
        }
    }

    pub fn from_file(filepath: &str) -> Self {
        let ext = parse_file_type(filepath).unwrap();
        let mut file = File::open(filepath).unwrap();
        let mut buf = file_read(&mut file, 4).unwrap();
        let metadata_length = u8v_to_u32(&buf[..]) as usize;
        buf = file_read(&mut file, metadata_length).unwrap();
        let metadata = serde_json::from_slice(&buf[..]).unwrap();
        let mut po = Self::new(metadata, ext);
        // root node
        file_seek(&mut file, SeekFrom::End(-12)).unwrap();
        buf = file_read(&mut file, 12).unwrap();
        let mut scanner = Scanner::new(buf);
        let root_offset = scanner.read_u64();
        let root_size = scanner.read_u32();
        po.tree = Tree::from_file(
            &mut file,
            root_offset,
            root_size,
            INDEX_NODE_SIZE,
            LEAF_NODE_SIZE,
        );
        po
    }

    pub fn input_word(&mut self, name: String, value: Vec<u8>) {
        self.metadata.word_num += 1;
        self.tree.insert(Key(name), Value(value));
    }

    pub fn save(&mut self, dest: &str) {
        println!("Writing to file...");
        let file_path = Path::new(dest);
        if file_path.exists() {
            panic!("Destination exists: {}", dest);
        }
        let file_path = Path::new(dest);
        let mut file = File::create(file_path)
            .expect(format!("Fail to create file: {}", file_path.display()).as_str());
        // metadata
        let metadata = serde_json::to_string(&self.metadata).expect("Fail to serialize metdata");
        let metadata_bytes = metadata.as_bytes();
        let metadata_length = u32_to_u8v(metadata_bytes.len() as u32);
        file.write_all(&metadata_length)
            .expect("Fail to write file");
        file.write_all(metadata_bytes).expect("Fail to write");
        // tree
        let (root_offset, root_size) = self.tree.write_to(&mut file);
        let offset_buf = u64_to_u8v(root_offset);
        file.write_all(&offset_buf).expect("Fail to write");
        let size_buf = u32_to_u8v(root_size);
        file.write_all(&size_buf).expect("Fail to write");
        let file_size = ((root_offset + root_size as u64 + 12) as f64) / 1024.0 / 1024.0;
        println!("{} - {:.2}M", dest, file_size);
    }

    pub fn to_raw(&self, dest: &str)
    {
        if !((dest.ends_with(EXT_RAW_WORD) && self.file_type == LapFileType::Word)
            || (dest.ends_with(EXT_RAW_RESOURCE) && self.file_type == LapFileType::Resource))
        {
            panic!("Invalid destination filename");
        }
        let mut pb = ProgressBar::new(self.tree.record_num() as u64);
        let mut raw = RawDict::new(dest);
        self.tree.traverse(|key, value| {
            raw.insert(key.0.as_str(), &value.0);
            pb.inc();
        });
        raw.flush();
        pb.finish();
    }
}
