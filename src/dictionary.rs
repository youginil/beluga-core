use flate2::read::DeflateDecoder;
use tracing::{error, info, instrument, warn};

use crate::{
    error::LaputaResult,
    laputa::{parse_file_type, EntryKey, EntryValue, LapFileType, Laputa, Metadata, EXT_RESOURCE},
    lru::{LruCache, LruValue, SizedValue},
    tree::{Node, Serializable},
    utils::{file_open, file_read, file_seek, u8v_to_u32, Scanner},
};
use std::{
    cell::RefCell,
    fs::{self, File},
    io::{Read, SeekFrom},
    path::Path,
    rc::Rc,
};

type EntryNode = Node<EntryKey, EntryValue>;
pub type LruCacheRef = Rc<RefCell<LruCache<(u32, u64), DictNode>>>;

#[derive(Debug)]
pub struct DictNode {
    node: EntryNode,
    children: Vec<(u64, u32)>,
    size: u64,
}

impl DictNode {
    fn new(node: EntryNode) -> Self {
        Self {
            node,
            children: Vec::new(),
            size: 0,
        }
    }
}

impl SizedValue for DictNode {
    fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug)]
struct DictFile {
    id: String,
    metadata: Metadata,
    file: File,
    entry_root: (u64, u32),
    token_root: (u64, u32),
    cache_id: u32,
    cache: LruCacheRef,
}

impl DictFile {
    #[instrument(name = "DictFile::new", skip(cache))]
    fn new(filepath: &str, cache: LruCacheRef, cache_id: u32) -> LaputaResult<Self> {
        let mut file = file_open(filepath)?;
        let mut buf = file_read(&mut file, 4)?;
        let metadata_length = u8v_to_u32(&buf[..]);
        info!("Read metadata: {}B", metadata_length);
        buf = file_read(&mut file, metadata_length as usize)?;
        let metadata = match serde_json::from_slice(&buf[..]) {
            Ok(r) => r,
            Err(_) => {
                error!("Fail to parse metadata");
                return Err("Fail to parse metadata".to_string());
            }
        };
        file_seek(&mut file, SeekFrom::End(-24))?;
        buf = file_read(&mut file, 24)?;
        let mut scanner = Scanner::new(buf);
        let entry_root_offset = scanner.read_u64();
        let entry_root_size = scanner.read_u32();
        let token_root_offset = scanner.read_u64();
        let token_root_size = scanner.read_u32();
        info!(
            entry_root_offset,
            entry_root_size, token_root_offset, token_root_size
        );
        Ok(Self {
            id: String::from(""),
            metadata,
            file,
            entry_root: (entry_root_offset, entry_root_size),
            token_root: (token_root_offset, token_root_size),
            cache_id,
            cache,
        })
    }

    #[instrument(skip(self))]
    fn get_node(&mut self, offset: u64, size: u32) -> Option<LruValue<DictNode>> {
        if let Some(node) = self.cache.borrow().get(&(self.cache_id, offset)) {
            info!("Found in cache");
            return Some(node);
        }
        if let Err(e) = file_seek(&mut self.file, SeekFrom::Start(offset)) {
            error!("File Seeking error. {}", e);
            return None;
        }
        match file_read(&mut self.file, size as usize) {
            Ok(data) => {
                let mut decode = DeflateDecoder::new(&data[..]);
                let mut data: Vec<u8> = vec![];
                decode.read_to_end(&mut data).unwrap();
                let (node, children) = Node::<EntryKey, EntryValue>::from_bytes(data);
                let mut dnode = DictNode::new(node);
                dnode.children = children;
                let value = self.cache.borrow_mut().put((self.cache_id, offset), dnode);
                Some(value)
            }
            Err(e) => {
                error!("File Reading Error. {}", e);
                None
            }
        }
    }

    #[instrument(skip(self))]
    pub fn search(&mut self, name: &str, fuzzy_limit: usize) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        let mut offset = self.entry_root.0;
        let mut size = self.entry_root.1;
        let lower_name = name.clone().to_lowercase();
        loop {
            let dict_node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    error!("Node not exists: offset: {}, size: {}", offset, size);
                    return result;
                }
            };
            let dn = dict_node.borrow();
            let node = &dn.node;
            let key = EntryKey(name.to_string());
            let (wi, cr) = dn.node.index_of(&key);
            if node.is_leaf {
                info!("Node is LEAF");
                if cr.is_ge() {
                    for i in wi..node.records.len() {
                        let k = &node.records[i].key;
                        info!("Checking match: {}", k,);
                        if k.0.to_lowercase().starts_with(lower_name.as_str()) {
                            result.push(k.0.clone());
                        } else {
                            return result;
                        }
                        if result.len() >= fuzzy_limit {
                            return result;
                        }
                    }
                }
                let mut next_offset = dn.children[0].0;
                let mut next_size = dn.children[0].1;
                loop {
                    info!("Searching from next sibling");
                    if next_offset == 0 {
                        info!("No next sibling");
                        return result;
                    }
                    if let Some(dict_node) = self.get_node(next_offset, next_size) {
                        let dn = dict_node.borrow();
                        for rec in &dn.node.records {
                            let k = &rec.key.0;
                            info!("Checking match: {}", k);
                            if k.to_lowercase().starts_with(lower_name.as_str()) {
                                result.push(k.clone());
                            } else {
                                return result;
                            }
                            if result.len() >= fuzzy_limit {
                                return result;
                            }
                        }
                        next_offset = dn.children[0].0;
                        next_size = dn.children[0].1;
                    } else {
                        return result;
                    }
                }
            } else {
                info!("Node is INDEX");
                if cr.is_le() {
                    (offset, size) = dn.children[wi];
                } else {
                    (offset, size) = dn.children[wi + 1];
                };
            }
        }
    }

    #[instrument(skip(self))]
    pub fn search_entry(&mut self, root: (u64, u32), name: &str) -> Option<Vec<u8>> {
        let mut offset = root.0;
        let mut size = root.1;
        loop {
            let dict_node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    error!("Node not exists. offset: {}, size: {}", offset, size);
                    return None;
                }
            };
            let dn = dict_node.borrow();
            let node = &dn.node;
            let key = EntryKey(name.to_string());
            let (index, cr) = node.index_of(&key);
            if node.is_leaf {
                info!("Node is LEAF");
                let records = &node.records;
                if cr.is_ge() {
                    for i in index..records.len() {
                        let rec = &records[i];
                        info!("Checking match. {}", rec.key);
                        if rec.key == key {
                            return Some(rec.value.as_ref().unwrap().bytes());
                        }
                    }
                    let mut next_offset = dn.children[0].0;
                    let mut next_size = dn.children[0].1;
                    loop {
                        if next_offset == 0 {
                            return None;
                        }
                        if let Some(dict_node) = self.get_node(next_offset, next_size) {
                            let dn = dict_node.borrow();
                            for rec in &dn.node.records {
                                let k = &rec.key.0;
                                info!("Checking match: {}", k);
                                if k == name {
                                    return Some(rec.value.as_ref().unwrap().bytes());
                                }
                                if k.to_lowercase() != name {
                                    return None;
                                }
                            }
                            next_offset = dn.children[0].0;
                            next_size = dn.children[0].1;
                        } else {
                            return None;
                        }
                    }
                }
                warn!("Entry not exists");
                return None;
            } else {
                info!("Node is INDEX");
                (offset, size) = if cr.is_le() {
                    dn.children[index]
                } else {
                    dn.children[index + 1]
                };
            }
        }
    }
}

pub struct Dictionary {
    word: DictFile,
    resources: Vec<DictFile>,
    pub js: String,
    pub css: String,
}

impl Dictionary {
    #[instrument(name = "Dictionary::new", skip(cache))]
    pub fn new(
        filepath: &str,
        cache: &LruCacheRef,
        mut cache_id: u32,
    ) -> LaputaResult<(Self, u32)> {
        let file_type = parse_file_type(filepath)?;
        if !matches!(file_type, LapFileType::Word) {
            error!("Invalid WORD extension");
            return Err("Not a word file".to_string());
        }
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            error!("File not exists or it is a directory");
            return Err(format!("Invalid path: {:?}", p.as_os_str()));
        }
        info!("Load WORD file");
        let word = DictFile::new(filepath, Rc::clone(&cache), cache_id)?;
        let basename = p.file_stem().unwrap().to_str().unwrap();
        let mut resources: Vec<DictFile> = Vec::new();
        let dir = match p.parent() {
            Some(d) => d,
            None => {
                error!("File has no parent directory, weird???");
                return Err("Invalid file path".to_string());
            }
        };
        let res_ext = String::from(".") + EXT_RESOURCE;
        info!("Search related resource files");
        for ret in dir.read_dir().expect("Fail to read dictionary directory") {
            if let Ok(entry) = ret {
                if !entry.metadata().unwrap().is_file() {
                    continue;
                }
                let name = entry.file_name().into_string().unwrap();
                if name.ends_with(res_ext.as_str()) {
                    let res_name = &name[0..name.len() - res_ext.len()];
                    if Some(0) == res_name.find(basename) {
                        let mut res_id = "";
                        let mut is_res = false;
                        if res_name.len() == basename.len() {
                            is_res = true
                        } else if res_name.len() > basename.len() + 1
                            && res_name.as_bytes()[basename.len() + 1] as char == '.'
                        {
                            is_res = true;
                            res_id = &res_name[basename.len() + 1..];
                        }
                        if is_res {
                            cache_id += 1;
                            info!("Load resource file. {}", name);
                            let mut res = DictFile::new(
                                dir.join(&name).to_str().unwrap(),
                                Rc::clone(&cache),
                                cache_id,
                            )?;
                            res.id = String::from(res_id);
                            resources.push(res);
                        }
                    }
                }
            }
        }
        let mut js = String::new();
        let js_file = dir.join(String::from(basename) + ".js");
        if js_file.is_file() {
            info!("Load JavaScript file. {:?}", js_file);
            match fs::read_to_string(js_file) {
                Ok(text) => js = text,
                Err(e) => {
                    error!("Fail to read file. {}", e);
                    return Err("Invald Javascript file".to_string());
                }
            }
        }
        let mut css = String::new();
        let css_file = dir.join(String::from(basename) + ".css");
        if css_file.is_file() {
            info!("Load CSS file. {:?}", css_file);
            match fs::read_to_string(css_file) {
                Ok(text) => css = text,
                Err(e) => {
                    error!("Fail to read file. {}", e);
                    return Err("Invalid CSS file".to_string());
                }
            }
        }
        Ok((
            Self {
                word,
                resources,
                js,
                css,
            },
            cache_id,
        ))
    }

    pub fn metadata(&self) -> Metadata {
        self.word.metadata.clone()
    }

    #[instrument(skip(self))]
    pub fn search(&mut self, name: &str, fuzzy_limit: usize) -> Vec<String> {
        info!("Search WORD entries");
        let mut result = self.word.search(name, fuzzy_limit);
        info!("Search TOKEN entries");
        if let Some(data) = self.word.search_entry(self.word.token_root, name) {
            let entries = Laputa::parse_token_entries(data);
            info!("Found {} entry(ies) by TOKEN", entries.len());
            for entry_name in entries {
                if !result.contains(&entry_name) {
                    result.push(entry_name);
                }
            }
        }
        result
    }

    #[instrument(skip(self))]
    pub fn search_word(&mut self, name: &str) -> Option<String> {
        if let Some(data) = self.word.search_entry(self.word.entry_root, name) {
            if let Ok(s) = String::from_utf8(data) {
                return Some(s);
            }
        }
        None
    }

    #[instrument(skip(self))]
    pub fn search_resource(&mut self, name: &str) -> Option<Vec<u8>> {
        let (id, n) = match name.split_once("//") {
            Some(r) => r,
            None => ("", name),
        };
        info!("Resource ID: {}, name: {}", id, n);
        for (_, dict) in self.resources.iter_mut().enumerate() {
            if dict.id == id {
                return dict.search_entry(dict.entry_root, n);
            }
        }
        info!("Invalid resource ID");
        None
    }
}
