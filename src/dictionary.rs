use crate::{
    error::{LaputaError, LaputaResult},
    laputa::{parse_file_type, LapFileType, Metadata, EXT_RESOURCE},
    utils::{file_metadata, file_open, file_read, file_seek, u8v_to_u32, u8v_to_u64},
};
use std::{
    fs::{self, File},
    io::SeekFrom,
    path::Path,
};

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
    id: String,
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
                return Err(LaputaError::InvalidDictFile);
            }
        };
        file_seek(&mut file, SeekFrom::End(-8))?;
        buf = file_read(&mut file, 8)?;
        let root_offset = u8v_to_u64(&buf[..]);
        let m = file_metadata(&file)?;
        let file_size = m.len();
        let root_size = (file_size - root_offset - 8) as u32;
        Ok(Self {
            id: String::from(""),
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
        let file_type = parse_file_type(filepath)?;
        if !matches!(file_type, LapFileType::Word) {
            return Err(LaputaError::InvalidDictName);
        }
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            return Err(LaputaError::InvalidDictFile);
        }
        let word = DictFile::new(filepath)?;
        let basename = p.file_stem().unwrap().to_str().unwrap();
        let mut resources: Vec<DictFile> = Vec::new();
        let dir = match p.parent() {
            Some(d) => d,
            None => return Err(LaputaError::InvalidDictFile),
        };
        let res_ext = String::from(".") + EXT_RESOURCE;
        for ret in dir.read_dir().expect("Fail to read dictionary directory") {
            if let Ok(entry) = ret {
                if !entry.metadata().unwrap().is_file() {
                    continue;
                }
                let name = entry.file_name().into_string().unwrap();
                if name.ends_with(res_ext.as_str()) {
                    let res_name = &name[0..name.len() - res_ext.len()];
                    if Some(0) == res_name.find(basename)
                        && res_name.len() > basename.len() + 1
                        && res_name.as_bytes()[basename.len() + 1] as char == '.'
                    {
                        let res_id = &res_name[basename.len() + 1..];
                        let mut res = DictFile::new(dir.join(&name).to_str().unwrap())?;
                        res.id = String::from(res_id);
                        resources.push(res);
                    }
                }
            }
        }
        let mut js = String::new();
        let js_file = dir.join(String::from(basename) + ".js");
        if js_file.is_file() {
            if let Ok(text) = fs::read_to_string(js_file) {
                js = text;
            } else {
                return Err(LaputaError::InvalidJS);
            }
        }
        let mut css = String::new();
        let css_file = dir.join(String::from(basename) + ".css");
        if css_file.is_file() {
            if let Ok(text) = fs::read_to_string(css_file) {
                css = text;
            } else {
                return Err(LaputaError::InvalidCSS);
            }
        }
        Ok(Self {
            word,
            resources,
            js,
            css,
        })
    }

    pub fn search(&self, word: &str) -> Vec<String> {
        let result: Vec<String> = Vec::new();
        result
    }
}
