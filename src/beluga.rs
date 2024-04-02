use crate::dictionary::SPEC;
use crate::error::{Error, Result};
use crate::tree::{Serializable, Smoothable, Tree};
use crate::utils::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const LEAF_NODE_SIZE: usize = 64 * 1024;
const INDEX_NODE_SIZE: usize = 64 * 1024;
pub const EXT_ENTRY: &str = "bel";
pub const EXT_RESOURCE: &str = "beld";
pub const EXT_RAW_ENTRY: &str = "bel-db";
pub const EXT_RAW_RESOURCE: &str = "beld-db";

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BelFileType {
    Entry,
    Resource,
}

pub fn parse_file_type(file: &str) -> Result<BelFileType> {
    let ext = file.split(".").last();
    match ext {
        Some(EXT_ENTRY) => Ok(BelFileType::Entry),
        Some(EXT_RESOURCE) => Ok(BelFileType::Resource),
        _ => Err(Error::Msg("Invalid file extension".to_string())),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub version: String,
    pub entry_num: u64,
    pub author: String,
    pub email: String,
    pub create_time: String,
    pub comment: String,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            version: String::from(""),
            entry_num: 0,
            author: String::from(""),
            email: String::from(""),
            create_time: String::from(""),
            comment: String::from(""),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntryKey(pub String);

impl Display for EntryKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialEq for EntryKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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

impl Smoothable for EntryKey {
    fn smooth(&self) -> Self {
        EntryKey(self.0.to_lowercase())
    }
}

#[derive(Debug, Clone)]
pub struct EntryValue(pub Vec<u8>);

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

pub struct Beluga {
    pub metadata: Metadata,
    pub file_type: BelFileType,
    entry_tree: Tree<EntryKey, EntryValue>,
    token_tree: Tree<EntryKey, EntryValue>,
}

impl Beluga {
    pub fn new(metadata: Metadata, file_type: BelFileType) -> Self {
        Self {
            metadata,
            file_type,
            entry_tree: Tree::new(INDEX_NODE_SIZE, LEAF_NODE_SIZE),
            token_tree: Tree::new(INDEX_NODE_SIZE, LEAF_NODE_SIZE),
        }
    }

    pub async fn from_file(filepath: &str) -> Self {
        let ext = parse_file_type(filepath).expect("fail to parse file type");
        let mut file = File::open(filepath).await.expect("fail to open file");
        let spec = file.read_u16().await.expect("fail to read spec");
        if spec == SPEC {
            let metadata_length =
                file.read_u32().await.expect("fail to read metadata length") as usize;
            let mut buf = vec![0; metadata_length];
            file.read_exact(&mut buf)
                .await
                .expect("fail to read metadata");
            let metadata = serde_json::from_slice(&buf[..]).expect("invalid metadata");
            let mut po = Self::new(metadata, ext);
            // root node
            file.seek(SeekFrom::End(-24)).await.expect("seek to -24");
            let mut buf = vec![0; 24];
            file.read_exact(&mut buf).await.expect("fail to read roots");
            let mut scanner = Scanner::new(&buf);
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
            )
            .await
            .expect("fail to parse entry tree");
            println!("Parsing token tree...");
            po.token_tree = Tree::from_file(
                &mut file,
                token_root_offset,
                token_root_size,
                INDEX_NODE_SIZE,
                LEAF_NODE_SIZE,
            )
            .await
            .expect("fail to parse token tree");
            po
        } else {
            panic!("invalid beluga spec");
        }
    }

    pub fn input_entry(&mut self, name: String, value: Vec<u8>) {
        self.metadata.entry_num += 1;
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

    pub fn parse_token_entries(data: &[u8]) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        let mut scanner = Scanner::new(&data);
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

    pub async fn save(&mut self, dest: &str) -> Result<()> {
        println!("Writing to {}...", dest);
        let file_path = Path::new(dest);
        if file_path.exists() {
            panic!("Destination exists: {}", dest);
        }
        let file_path = Path::new(dest);
        let mut file = File::create(file_path).await?;
        // spec
        file.write_u16(SPEC).await?;
        // metadata
        let metadata = serde_json::to_string(&self.metadata).expect("Fail to serialize metdata");
        let metadata_length = metadata.as_bytes().len() as u32;
        file.write_u32(metadata_length).await?;
        file.write(metadata.as_bytes()).await?;
        // entry tree
        println!("Writing entry nodes...");
        let (entry_root_offset, entry_root_size) = self.entry_tree.write_to(&mut file).await?;
        // token tree
        println!("Writing token nodes...");
        let (token_root_offset, token_root_size) = self.token_tree.write_to(&mut file).await?;
        file.write_u64(entry_root_offset).await?;
        file.write_u32(entry_root_size).await?;
        file.write_u64(token_root_offset).await?;
        file.write_u32(token_root_size).await?;
        file.flush().await?;
        let file_metadata = file.metadata().await?;
        let file_size = (file_metadata.len() as f64) / 1024.0 / 1024.0;
        println!("{} - {:.2}M", dest, file_size);
        Ok(())
    }

    pub fn traverse_entry<F>(&self, walk: &mut F)
    where
        F: FnMut(&EntryKey, &EntryValue),
    {
        self.entry_tree.traverse(walk);
    }

    pub fn traverse_token<F>(&self, walk: &mut F)
    where
        F: FnMut(&EntryKey, &EntryValue),
    {
        self.token_tree.traverse(walk);
    }
}
