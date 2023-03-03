use crate::{
    error::{LaputaError, LaputaResult},
    laputa::Metadata,
    utils::{file_metadata, file_open, file_read, file_seek, u8v_to_u32, u8v_to_u64},
};
use std::{fs::File, io::SeekFrom, path::Path};

struct DictWord {
    name: String,
    value: Vec<u8>,
}

struct DictNode {
    words: Vec<DictWord>,
    children: Vec<(u64, u32)>,
    size: u64,
}

impl DictNode {
    fn new() -> Self {
        Self {
            words: Vec::new(),
            children: Vec::new(),
            size: 0,
        }
    }
}

struct DictFile {
    path: String,
    metadata: Metadata,
    file: File,
    root: (u64, u32),
}

impl DictFile {
    fn new(filepath: &str) -> LaputaResult<Self> {
        let mut file = file_open(filepath)?;
        let mut buf = file_read(&mut file, 4)?;
        let metadata_length = u8v_to_u32(&buf[..]);
        buf = file_read(&mut file, metadata_length as usize)?;
        let metadata = match serde_json::from_slice(&buf[..]) {
            Ok(r) => r,
            Err(_) => {
                return Err(LaputaError::InvalidDictionary);
            }
        };
        file_seek(&mut file, SeekFrom::End(-8))?;
        buf = file_read(&mut file, 8)?;
        let root_offset = u8v_to_u64(&buf[..]);
        let m = file_metadata(&file)?;
        let file_size = m.len();
        let root_size = (file_size - root_offset - 8) as u32;
        Ok(Self {
            path: String::from(filepath),
            metadata,
            file,
            root: (root_offset, root_size),
        })
    }
}

pub struct Dictionary {
    word: DictFile,
    resources: Vec<DictFile>,
    js: String,
    css: String,
}

impl Dictionary {
    pub fn new(filepath: &str) -> LaputaResult<Self> {
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            return Err(LaputaError::InvalidDictionary);
        }
        let word = DictFile::new(filepath)?;
        let mut resources: Vec<DictFile> = Vec::new();
        if let Some(dir) = p.parent() {
            for ret in dir.read_dir().expect("Fail to read dictionary directory") {
                if let Ok(entry) = ret {
                    if entry.metadata().unwrap().is_file() {
                        //
                    }
                }
            }
        }
        let js = String::new();
        let css = String::new();
        Ok(Self {
            word,
            resources,
            js,
            css,
        })
    }
}
