use crate::error::{Error, Result};
use flate2::read::DeflateDecoder;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt},
    sync::RwLock,
};
use tracing::{error, info, instrument, warn};

use crate::{
    beluga::{parse_file_type, BelFileType, Beluga, EntryKey, EntryValue, Metadata, EXT_RESOURCE},
    lru::{LruCache, SizedValue},
    tree::{Node, Serializable},
    utils::Scanner,
};
use std::{
    io::{Read, SeekFrom},
    path::Path,
    sync::Arc,
};

pub const SPEC: u16 = 1;

static REDIRECT: &str = "@@@LINK=";

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
    async fn new(filepath: &str, cache_id: u32) -> Result<Self> {
        let mut file = File::open(filepath).await?;
        let spec = file.read_u16().await?;
        if spec == SPEC {
            let metadata_length = file.read_u32().await?;
            info!("Read metadata: {}B", metadata_length);
            let mut buf = vec![0; metadata_length as usize];
            file.read_exact(&mut buf).await?;
            let metadata = match serde_json::from_slice(&buf[..]) {
                Ok(r) => r,
                Err(_) => {
                    error!("Fail to parse metadata");
                    return Err(Error::Msg("fail to parse metadata".to_string()));
                }
            };
            file.seek(SeekFrom::End(-24)).await?;
            let mut buf = vec![0; 24];
            file.read_exact(&mut buf).await?;
            let mut scanner = Scanner::new(&buf);
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
        } else {
            Err(Error::Msg("invalid beluga spec".to_string()))
        }
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
        if let Err(e) = self.file.seek(SeekFrom::Start(offset)).await {
            error!("File Seeking error. {}", e);
            return None;
        }
        let mut buf = vec![0; size as usize];
        match self.file.read_exact(&mut buf).await {
            Ok(_) => {
                let mut decode = DeflateDecoder::new(&buf[..]);
                let mut data: Vec<u8> = vec![];
                decode.read_to_end(&mut data).unwrap();
                let (node, children) = Node::<EntryKey, EntryValue>::from_bytes(&data);
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
        strict: bool,
        prefix_limit: usize,
    ) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        let mut offset = self.entry_root.0;
        let mut size = self.entry_root.1;
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
                let lower_name = name.to_lowercase();
                let idx = if cr.is_le() { wi } else { wi + 1 };
                for i in idx..node.records.len() {
                    let k = &node.records[i].key;
                    info!("Checking match: {}", k,);
                    if k.0.to_lowercase().starts_with(lower_name.as_str()) {
                        if (strict && k.0.starts_with(name)) || !strict {
                            result.push(k.0.clone());
                        }
                    } else {
                        return result;
                    }
                    if result.len() >= prefix_limit {
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
                                if (strict && k.starts_with(name)) || !strict {
                                    result.push(k.clone());
                                }
                            } else {
                                return result;
                            }
                            if result.len() >= prefix_limit {
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
    dir: String,
    basename: String,
    entry: DictFile,
    resources: Vec<DictFile>,
    css_js: Option<(String, String)>,
}

impl Dictionary {
    pub async fn new(filepath: &str, mut cache_id: u32) -> Result<(Self, u32)> {
        let file_type = parse_file_type(filepath)?;
        if !matches!(file_type, BelFileType::Entry) {
            error!("invalid entry file extension");
            return Err(Error::Msg("not a entry file".to_string()));
        }
        let p = Path::new(filepath);
        if !p.exists() || p.is_dir() {
            error!("File not exists or it is a directory");
            return Err(Error::Msg(format!("invalid path. {:?}", p)));
        }
        info!("Load entry file");
        let entry = DictFile::new(filepath, cache_id).await?;
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
                                DictFile::new(dir.join(&name).to_str().unwrap(), cache_id).await?;
                            res.id = String::from(res_id);
                            resources.push(res);
                        }
                    }
                }
            }
        }
        Ok((
            Self {
                dir: dir.to_str().unwrap().to_string(),
                basename: basename.to_string(),
                entry,
                resources,
                css_js: None,
            },
            cache_id,
        ))
    }

    pub async fn get_css_js(&mut self, disable_cache: bool) -> Result<(String, String)> {
        if let Some(v) = &self.css_js {
            if disable_cache {
                self.css_js = None;
            } else {
                return Ok(v.clone());
            }
        }
        let dir = Path::new(&self.dir);
        let mut js = String::new();
        let js_file = dir.join(format!("{}.js", self.basename));
        if js_file.is_file() {
            info!("Load JavaScript file. {:?}", js_file);
            match fs::read_to_string(js_file).await {
                Ok(text) => js = text,
                Err(e) => {
                    error!("Fail to read file. {}", e);
                    return Err(Error::Msg("invalid javascript file".to_string()));
                }
            }
        }
        let mut css = String::new();
        let css_file = dir.join(format!("{}.css", self.basename));
        if css_file.is_file() {
            info!("Load CSS file. {:?}", css_file);
            match fs::read_to_string(css_file).await {
                Ok(text) => css = text,
                Err(e) => {
                    error!("Fail to read file. {}", e);
                    return Err(Error::Msg("invalid css file".to_string()));
                }
            }
        }
        if !cfg!(debug_assertions) || !disable_cache {
            self.css_js = Some((css.clone(), js.clone()));
        }
        Ok((css, js))
    }

    pub fn metadata(&self) -> Metadata {
        self.entry.metadata.clone()
    }

    #[instrument(skip(self, cache))]
    pub async fn search(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
        strict: bool,
        prefix_limit: usize,
        phrase_limit: usize,
    ) -> Vec<String> {
        info!("Search entry");
        let mut result = self
            .entry
            .search(cache.clone(), name, strict, prefix_limit)
            .await;
        if phrase_limit > 0 && self.entry.token_root.1 != 0 {
            info!("Search TOKEN entries");
            if let Some(data) = self
                .entry
                .search_entry(cache.clone(), self.entry.token_root, name)
                .await
            {
                let entries = Beluga::parse_token_entries(&data);
                info!("Found {} entry(ies) by TOKEN", entries.len());
                let mut token_count = 0;
                for entry_name in entries {
                    if !result.contains(&entry_name) {
                        if token_count >= phrase_limit {
                            break;
                        }
                        result.push(entry_name);
                        token_count += 1;
                    }
                }
            }
        }
        result
    }

    #[instrument(skip(self, cache))]
    pub async fn search_entry(
        &mut self,
        cache: Arc<RwLock<NodeCache>>,
        name: &str,
    ) -> Option<String> {
        let max_redirects = 3;
        let mut keyword = name.to_string();
        for _ in 0..max_redirects {
            if let Some(data) = self
                .entry
                .search_entry(cache.clone(), self.entry.entry_root, &keyword)
                .await
            {
                if let Ok(content) = String::from_utf8(data) {
                    let s = content.trim();
                    if s.starts_with(REDIRECT) {
                        let (_, kw) = s.split_at(REDIRECT.len());
                        keyword = kw.to_string();
                    } else {
                        return Some(content);
                    }
                }
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
