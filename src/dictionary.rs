use crate::error::{Error, Result};
use flate2::read::DeflateDecoder;
use tokio::sync::RwLock;
use tracing::{error, info, instrument, warn};

use crate::{
    beluga::{parse_file_type, Beluga, EntryKey, EntryValue, LapFileType, Metadata, EXT_RESOURCE},
    lru::{LruCache, SizedValue},
    tree::{Node, Serializable},
    utils::{file_open, file_read, file_seek, u8v_to_u32, Scanner},
};
use std::{
    fs::{self, File},
    io::{Read, SeekFrom},
    path::Path,
    sync::Arc,
};

type EntryNode = Node<EntryKey, EntryValue>;
pub type NodeCache = LruCache<(u32, u64), DictNode>;

#[derive(Debug, Clone)]
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
}

impl DictFile {
    fn new(filepath: &str, cache_id: u32) -> Result<Self> {
        let mut file = file_open(filepath)?;
        let mut buf = file_read(&mut file, 4)?;
        let metadata_length = u8v_to_u32(&buf[..]);
        info!("Read metadata: {}B", metadata_length);
        buf = file_read(&mut file, metadata_length as usize)?;
        let metadata = match serde_json::from_slice(&buf[..]) {
            Ok(r) => r,
            Err(_) => {
                error!("Fail to parse metadata");
                return Err(Error::Msg("fail to parse metadata".to_string()));
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
        })
    }

    #[instrument(skip(self, cache))]
    async fn get_node(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        offset: u64,
        size: u32,
    ) -> Option<DictNode> {
        let cache_lock = cache.read().await;
        if let Some(node) = cache_lock.get(&(self.cache_id, offset)) {
            info!("Found in cache");
            return Some(node);
        }
        drop(cache_lock);
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
                let mut dnode = DictNode::new(*node);
                dnode.children = children;
                let mut cache_lock = cache.write().await;
                let value = cache_lock.put((self.cache_id, offset), dnode);
                drop(cache_lock);
                Some(value)
            }
            Err(e) => {
                error!("File Reading Error. {}", e);
                None
            }
        }
    }

    #[instrument(skip(self, cache))]
    pub async fn search(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
        fuzzy_limit: usize,
    ) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        let mut offset = self.entry_root.0;
        let mut size = self.entry_root.1;
        let lower_name = name.to_lowercase();
        loop {
            let dict_node = match self.get_node(cache.clone(), offset, size).await {
                Some(nd) => nd,
                None => {
                    error!("Node not exists: offset: {}, size: {}", offset, size);
                    return result;
                }
            };
            let dn = dict_node;
            let node = &dn.node;
            let key = EntryKey(name.to_string());
            let (wi, cr) = dn.node.index_of(&key);
            if node.is_leaf {
                info!("Node is LEAF");
                let idx = if cr.is_le() { wi } else { wi + 1 };
                for i in idx..node.records.len() {
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
                let mut next_offset = dn.children[0].0;
                let mut next_size = dn.children[0].1;
                loop {
                    info!("Searching from next sibling");
                    if next_offset == 0 {
                        info!("No next sibling");
                        return result;
                    }
                    if let Some(dn) = self.get_node(cache.clone(), next_offset, next_size).await {
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

    #[instrument(skip(self, cache))]
    pub async fn search_entry(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        root: (u64, u32),
        name: &str,
    ) -> Option<Vec<u8>> {
        let mut offset = root.0;
        let mut size = root.1;
        loop {
            let dict_node = match self.get_node(cache.clone(), offset, size).await {
                Some(nd) => nd,
                None => {
                    error!("Node not exists. offset: {}, size: {}", offset, size);
                    return None;
                }
            };
            let node = dict_node.node;
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
                    let mut next_offset = dict_node.children[0].0;
                    let mut next_size = dict_node.children[0].1;
                    loop {
                        if next_offset == 0 {
                            return None;
                        }
                        if let Some(dict_node) =
                            self.get_node(cache.clone(), next_offset, next_size).await
                        {
                            let node = dict_node.node;
                            for rec in &node.records {
                                let k = &rec.key.0;
                                info!("Checking match: {}", k);
                                if k == name {
                                    return Some(rec.value.as_ref().unwrap().bytes());
                                }
                                if k.to_lowercase() != name {
                                    return None;
                                }
                            }
                            next_offset = dict_node.children[0].0;
                            next_size = dict_node.children[0].1;
                        } else {
                            return None;
                        }
                    }
                }
                warn!("Entry not exists");
                return None;
            }
            info!("Node is INDEX");
            (offset, size) = if cr.is_le() {
                dict_node.children[index]
            } else {
                dict_node.children[index + 1]
            };
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
    pub fn new(filepath: &str, mut cache_id: u32) -> Result<(Self, u32)> {
        let file_type = parse_file_type(filepath)?;
        if !matches!(file_type, LapFileType::Word) {
            error!("Invalid WORD extension");
            return Err(Error::Msg("not a word file".to_string()));
        }
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            error!("File not exists or it is a directory");
            return Err(Error::Msg(format!("invalid path. {:?}", p)));
        }
        info!("Load WORD file");
        let word = DictFile::new(filepath, cache_id)?;
        let basename = p.file_stem().unwrap().to_str().unwrap();
        let mut resources: Vec<DictFile> = Vec::new();
        let dir = match p.parent() {
            Some(d) => d,
            None => {
                error!("File has no parent directory, weird???");
                return Err(Error::Msg("invalid file path".to_string()));
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
                            let mut res =
                                DictFile::new(dir.join(&name).to_str().unwrap(), cache_id)?;
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
                    return Err(Error::Msg("invalid javascript file".to_string()));
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
                    return Err(Error::Msg("invalid css file".to_string()));
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

    #[instrument(skip(self, cache))]
    pub async fn search(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
        fuzzy_limit: usize,
        result_limit: usize,
    ) -> Vec<String> {
        info!("Search WORD entries");
        let mut result = self.word.search(cache.clone(), name, fuzzy_limit).await;
        info!("Search TOKEN entries");
        if let Some(data) = self
            .word
            .search_entry(cache.clone(), self.word.token_root, name)
            .await
        {
            let entries = Beluga::parse_token_entries(data);
            info!("Found {} entry(ies) by TOKEN", entries.len());
            let mut token_count = 0;
            for entry_name in entries {
                if !result.contains(&entry_name) {
                    if token_count >= result_limit {
                        break;
                    }
                    result.push(entry_name);
                    token_count += 1;
                }
            }
        }
        result
    }

    #[instrument(skip(self, cache))]
    pub async fn search_word(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
    ) -> Option<String> {
        if let Some(data) = self
            .word
            .search_entry(cache.clone(), self.word.entry_root, name)
            .await
        {
            if let Ok(s) = String::from_utf8(data) {
                return Some(s);
            }
        }
        None
    }

    #[instrument(skip(self, cache))]
    pub async fn search_resource(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
    ) -> Option<Vec<u8>> {
        info!("Resource name: {}", name);
        for (_, dict) in self.resources.iter_mut().enumerate() {
            if let Some(v) = dict
                .search_entry(cache.clone(), dict.entry_root, name)
                .await
            {
                return Some(v);
            }
        }
        info!("Invalid resource ID");
        None
    }
}
