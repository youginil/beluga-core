use crate::{
    error::{LaputaError, LaputaResult},
    laputa::{parse_file_type, Key, LapFileType, Metadata, Value, EXT_RESOURCE},
    lru::{LruCache, LruValue, SizedValue},
    tree::{Node, Serializable},
    utils::{file_open, file_read, file_seek, u8v_to_u32, Scanner},
};
use std::{
    cell::RefCell,
    fs::{self, File},
    io::SeekFrom,
    path::Path,
    rc::Rc,
};

type LapNode = Node<Key, Value>;
pub type LruCacheRef = Rc<RefCell<LruCache<(u32, u64), DictNode>>>;

pub struct DictNode {
    node: LapNode,
    children: Vec<(u64, u32)>,
    size: u64,
}

impl DictNode {
    fn new(node: LapNode) -> Self {
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

struct DictFile {
    id: String,
    metadata: Metadata,
    file: File,
    root: (u64, u32),
    cache_id: u32,
    cache: LruCacheRef,
}

impl DictFile {
    fn new(filepath: &str, cache: LruCacheRef, cache_id: u32) -> LaputaResult<Self> {
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
        file_seek(&mut file, SeekFrom::End(-12))?;
        buf = file_read(&mut file, 12)?;
        let mut scanner = Scanner::new(buf);
        let root_offset = scanner.read_u64();
        let root_size = scanner.read_u32();
        Ok(Self {
            id: String::from(""),
            metadata,
            file,
            root: (root_offset, root_size),
            cache_id,
            cache,
        })
    }

    fn get_node(&mut self, offset: u64, size: u32) -> Option<LruValue<DictNode>> {
        if let Some(node) = self.cache.borrow().get(&(self.cache_id, offset)) {
            return Some(node);
        }
        if let Err(_) = file_seek(&mut self.file, SeekFrom::Start(offset)) {
            return None;
        }
        if let Ok(data) = file_read(&mut self.file, size as usize) {
            let (node, children) = Node::<Key, Value>::from_bytes(data);
            let mut dnode = DictNode::new(node);
            dnode.children = children;
            let value = self.cache.borrow_mut().put((self.cache_id, offset), dnode);
            return Some(value);
        }
        None
    }

    pub fn search(&mut self, name: &str, limit: usize) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        let mut offset = self.root.0;
        let mut size = self.root.1;
        loop {
            let dict_node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    return result;
                }
            };
            let dn = dict_node.borrow();
            let node = &dn.node;
            let key = Key(name.to_string());
            let (wi, cr) = dn.node.index_of(&key);
            if node.is_leaf {
                if cr.is_ge() {
                    for i in wi..node.records.len() {
                        let k = &node.records[i].key;
                        if k.0.starts_with(name) {
                            result.push(k.0.clone());
                        } else {
                            return result;
                        }
                        if result.len() >= limit {
                            return result;
                        }
                    }
                }
                loop {
                    let (next_offset, next_size) = dn.children[0];
                    if next_offset == 0 {
                        return result;
                    }
                    if let Some(dict_node) = self.get_node(next_offset, next_size) {
                        let dn = dict_node.borrow();
                        for rec in &dn.node.records {
                            if rec.key.0.starts_with(name) {
                                result.push(rec.key.0.clone());
                            } else {
                                return result;
                            }
                            if result.len() >= limit {
                                return result;
                            }
                        }
                    } else {
                        return result;
                    }
                }
            } else {
                if cr.is_le() {
                    (offset, size) = dn.children[wi];
                } else {
                    (offset, size) = dn.children[wi + 1];
                };
            }
        }
    }

    pub fn search_entry(&mut self, name: &str) -> Option<Vec<u8>> {
        let mut offset = self.root.0;
        let mut size = self.root.1;
        loop {
            let dict_node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    return None;
                }
            };
            let dn = dict_node.borrow();
            let node = &dn.node;
            let key = Key(name.to_string());
            let (index, cr) = node.index_of(&key);
            if node.is_leaf {
                let records = &node.records;
                if cr.is_ge() {
                    for i in index..records.len() {
                        let rec = &records[i];
                        if rec.key == key {
                            return Some(rec.value.as_ref().unwrap().bytes());
                        }
                    }
                }
                return None;
            } else {
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
    pub fn new(
        filepath: &str,
        cache: &LruCacheRef,
        mut cache_id: u32,
    ) -> LaputaResult<(Self, u32)> {
        let file_type = parse_file_type(filepath)?;
        if !matches!(file_type, LapFileType::Word) {
            return Err(LaputaError::InvalidDictName);
        }
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            return Err(LaputaError::InvalidDictFile);
        }
        let word = DictFile::new(filepath, Rc::clone(&cache), cache_id)?;
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
                        cache_id += 1;
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

    pub fn search(&mut self, name: &str, limit: usize) -> Vec<String> {
        self.word.search(name, limit)
    }

    pub fn search_word(&mut self, name: &str) -> Option<String> {
        if let Some(data) = self.word.search_entry(name) {
            if let Ok(s) = String::from_utf8(data) {
                return Some(s);
            }
        }
        None
    }

    pub fn search_resource(&mut self, name: &str) -> Option<Vec<u8>> {
        let (id, n) = match name.split_once("//") {
            Some(r) => r,
            None => ("", name),
        };
        for (_, dict) in self.resources.iter_mut().enumerate() {
            if dict.id == id {
                return dict.search_entry(n);
            }
        }
        None
    }
}
