use crate::{
    error::{LaputaError, LaputaResult},
    laputa::{parse_file_type, LapFileType, Metadata, EXT_RESOURCE},
    lru::{LruCache, LruValue, SizedValue},
    utils::{file_metadata, file_open, file_read, file_seek, u8v_to_u32, u8v_to_u64, Scanner},
};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fs::{self, File},
    io::SeekFrom,
    path::Path,
    rc::Rc,
};

pub type LruCacheRef = Rc<RefCell<LruCache<(u32, u64), DictNode>>>;

struct DictWord {
    name: String,
    value: Option<Vec<u8>>,
}

pub struct DictNode {
    is_leaf: bool,
    words: Vec<DictWord>,
    children: Vec<(u64, u32)>,
    size: u64,
}

impl DictNode {
    fn new() -> Self {
        Self {
            is_leaf: true,
            words: Vec::new(),
            children: Vec::new(),
            size: 0,
        }
    }

    fn search(&self, name: &str) -> (usize, Ordering) {
        let words = &self.words;
        let mut hi = words.len() - 1;
        let mut li = 0;
        let n = name.to_string();
        loop {
            let mut mi = (hi + li) / 2;
            let cr = words[mi].name.cmp(&n);
            match cr {
                Ordering::Greater => {
                    hi = mi;
                }
                Ordering::Less => {
                    li = mi;
                }
                Ordering::Equal => {
                    while mi > li {
                        mi -= 1;
                        if !words[mi].name.cmp(&n).is_eq() {
                            mi += 1;
                            break;
                        }
                    }
                    return (mi, Ordering::Equal);
                }
            }
            if hi == li {
                return (hi, cr);
            }
            if hi - li == 1 {
                if mi == li {
                    return (hi, words[hi].name.cmp(&n));
                }
                let cr = words[li].name.cmp(&n);
                if cr.is_ge() {
                    return (li, Ordering::Less);
                }
                return (hi, Ordering::Less);
            }
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
        file_seek(&mut file, SeekFrom::End(-8))?;
        buf = file_read(&mut file, 8)?;
        let root_offset = u8v_to_u64(&buf[..]);
        let m = file_metadata(&file)?;
        let file_size = m.len();
        let root_size = (file_size - root_offset - 8) as u32;
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
            let mut node = DictNode::new();
            node.size = data.len() as u64;
            let mut scanner = Scanner::new(data);
            node.is_leaf = scanner.read_u8() == 0;
            let wc = scanner.read_u32();
            for _ in 0..wc {
                let size = scanner.read_u32();
                if let Ok(name) = scanner.read_string(size as usize) {
                    let size = scanner.read_u32();
                    let value = if size > 0 {
                        Some(scanner.read(size as usize))
                    } else {
                        None
                    };
                    node.words.push(DictWord { name, value });
                } else {
                    return None;
                }
            }
            let cc = if node.is_leaf { 1 } else { wc + 1 };
            for _ in 0..cc {
                let offset = scanner.read_u64();
                let size = scanner.read_u32();
                node.children.push((offset, size));
            }
            let value = self.cache.borrow_mut().put((self.cache_id, offset), node);
            return Some(value);
        }
        None
    }

    pub fn search(&mut self, name: &str, limit: usize) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        let mut offset = self.root.0;
        let mut size = self.root.1;
        loop {
            let node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    return result;
                }
            };
            let dn = node.borrow();
            let (wi, cr) = dn.search(name);
            if dn.is_leaf {
                if cr.is_ge() {
                    for i in wi..dn.words.len() {
                        let wd = &dn.words[i].name;
                        if wd.starts_with(name) {
                            result.push(wd.clone());
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
                    if let Some(node) = self.get_node(next_offset, next_size) {
                        let dn = node.borrow();
                        for word in &dn.words {
                            if word.name.starts_with(name) {
                                result.push(word.name.clone());
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
            let node = match self.get_node(offset, size) {
                Some(nd) => nd,
                None => {
                    return None;
                }
            };
            let dn = node.borrow();
            let (index, cr) = dn.search(name);
            if dn.is_leaf {
                let words = &dn.words;
                if cr.is_ge() {
                    for i in index..words.len() {
                        let wd = &words[i];
                        if wd.name == name {
                            return Some(wd.value.as_ref().unwrap().clone());
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
