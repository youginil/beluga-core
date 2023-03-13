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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LapFileType {
    Word,
    Resource,
}

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

#[derive(Clone)]
pub struct EntryKey(pub String);

impl Display for EntryKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.to_lowercase().partial_cmp(&other.0.to_lowercase())
    }
}

impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.to_lowercase().cmp(&other.0.to_lowercase())
    }
}

impl PartialEq for EntryKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_lowercase() == other.0.to_lowercase()
    }
}

impl Eq for EntryKey {}

impl Serializable for EntryKey {
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

pub struct EntryValue(Vec<u8>);

impl Serializable for EntryValue {
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
    entry_tree: Tree<EntryKey, EntryValue>,
    token_tree: Tree<EntryKey, EntryValue>,
}

impl Laputa {
    pub fn new(metadata: Metadata, file_type: LapFileType) -> Self {
        Self {
            metadata,
            file_type,
            entry_tree: Tree::new(INDEX_NODE_SIZE, LEAF_NODE_SIZE),
            token_tree: Tree::new(INDEX_NODE_SIZE, LEAF_NODE_SIZE),
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
        file_seek(&mut file, SeekFrom::End(-24)).unwrap();
        buf = file_read(&mut file, 24).unwrap();
        let mut scanner = Scanner::new(buf);
        let entry_root_offset = scanner.read_u64();
        let entry_root_size = scanner.read_u32();
        let token_root_offset = scanner.read_u64();
        let token_root_size = scanner.read_u32();
        println!("Parsing entry tree...");
        po.entry_tree = Tree::from_file(
            &mut file,
            entry_root_offset,
            entry_root_size,
            INDEX_NODE_SIZE,
            LEAF_NODE_SIZE,
        );
        println!("Parsing token tree...");
        po.token_tree = Tree::from_file(
            &mut file,
            token_root_offset,
            token_root_size,
            INDEX_NODE_SIZE,
            LEAF_NODE_SIZE,
        );
        po
    }

    pub fn input_word(&mut self, name: String, value: Vec<u8>) {
        self.metadata.word_num += 1;
        self.entry_tree.insert(EntryKey(name), EntryValue(value));
    }

    pub fn input_token(&mut self, name: String, value: Vec<String>) {
        let key = EntryKey(name);
        let mut data: Vec<u8> = vec![];
        for item in value {
            let bs = item.as_bytes();
            let mut size = u16_to_u8v(bs.len() as u16);
            data.append(&mut size);
            data.append(&mut bs.to_vec());
        }
        self.token_tree.insert(key, EntryValue(data));
    }

    pub fn parse_token_entries(data: Vec<u8>) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        let mut scanner = Scanner::new(data);
        loop {
            if scanner.is_end() {
                break;
            }
            let size = scanner.read_u16();
            let str = scanner.read_string(size as usize);
            result.push(str);
        }
        result
    }

    pub fn save(&mut self, dest: &str) {
        println!("Writing to {}...", dest);
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
        // entry tree
        println!("Writing entries...");
        let (entry_root_offset, entry_root_size) = self.entry_tree.write_to(&mut file);
        // token tree
        println!("Writing tokens...");
        let (token_root_offset, token_root_size) = self.token_tree.write_to(&mut file);
        file.write_all(&u64_to_u8v(entry_root_offset)).unwrap();
        file.write_all(&u32_to_u8v(entry_root_size)).unwrap();
        file.write_all(&u64_to_u8v(token_root_offset)).unwrap();
        file.write_all(&u32_to_u8v(token_root_size)).unwrap();
        let file_size = (file.metadata().unwrap().len() as f64) / 1024.0 / 1024.0;
        println!("{} - {:.2}M", dest, file_size);
    }

    pub fn to_raw(&self, dest: &str) {
        if !((dest.ends_with(EXT_RAW_WORD) && self.file_type == LapFileType::Word)
            || (dest.ends_with(EXT_RAW_RESOURCE) && self.file_type == LapFileType::Resource))
        {
            panic!("Invalid destination filename");
        }
        let mut pb = ProgressBar::new(self.entry_tree.record_num() as u64);
        let mut raw = RawDict::new(dest);
        self.entry_tree.traverse(|key, value| {
            raw.insert_entry(key.0.as_str(), &value.0);
            pb.inc();
        });
        raw.flush_entry_cache();
        pb.finish();
    }
}
