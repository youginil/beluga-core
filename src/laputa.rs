use crate::error::{LaputaError, LaputaResult};
use crate::raw::RawDict;
use crate::utils::*;
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::Path;
use std::rc::Rc;

const LEAF_NODE_SIZE: usize = 32 * 1024;
const INDEX_NODE_SIZE: usize = 32 * 1024;
const EXT_WORD: &str = "lpw";
const EXT_RESOURCE: &str = "lpr";

pub fn parse_file_type(file: &str) -> LaputaResult<LapFileType> {
    let ext = file.split(".").last();
    match ext {
        Some(EXT_WORD) => Ok(LapFileType::Word),
        Some(EXT_RESOURCE) => Ok(LapFileType::Resource),
        _ => Err(LaputaError::InvalidName),
    }
}

fn compress(buf: &[u8]) -> Vec<u8> {
    let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
    e.write_all(buf).expect("DeflateEncoder: Fail to write");
    return e.finish().expect("DeflateEncoder: Fail to finish");
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Metadata {
    spec: u8,
    pub version: String,
    pub word_number: u64,
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
            word_number: 0,
            author: String::from(""),
            email: String::from(""),
            create_time: String::from(""),
            comment: String::from(""),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Word {
    key: String,
    value: Vec<u8>,
    lower_key: String,
}

impl Word {
    fn new(key: String) -> Self {
        let lk = key.to_lowercase();
        Self {
            key,
            value: Vec::new(),
            lower_key: lk.to_string(),
        }
    }

    fn with_value(key: String, value: Vec<u8>) -> Self {
        let lk = key.to_lowercase();
        Self {
            key,
            value,
            lower_key: lk.to_string(),
        }
    }

    fn size(&self) -> usize {
        4 + 4 + self.key.bytes().len() + self.value.len()
    }

    fn compare(&self, other: &Word) -> i8 {
        let mut chs = other.lower_key.chars();
        for char in self.lower_key.chars() {
            if let Some(ch) = chs.next() {
                if char > ch {
                    return 1;
                } else if char < ch {
                    return -1;
                }
            } else {
                return 1;
            }
        }
        if let Some(_) = chs.next() {
            return -1;
        } else {
            return 0;
        }
    }

    fn to_vec(&mut self) -> Vec<u8> {
        let mut k = self.key.as_bytes().to_vec();
        let mut buf: Vec<u8> = Vec::new();
        let mut key_size = u32_to_u8v(k.len() as u32);
        buf.append(&mut key_size);
        let mut value_size = u32_to_u8v(self.value.len() as u32);
        buf.append(&mut k);
        buf.append(&mut value_size);
        buf.append(&mut self.value);
        return buf;
    }
}

type NodeRef = Rc<RefCell<Node>>;

fn create_node_ref() -> NodeRef {
    return Rc::new(RefCell::new(Node::new()));
}

fn create_node_ref_with_data(words: Vec<Word>, children: Vec<NodeRef>) -> NodeRef {
    return Rc::new(RefCell::new(Node::with_data(words, children)));
}

fn create_node_ref_from_bytes(
    bytes: &[u8],
    offset: usize,
    size: usize,
    leaves: Rc<RefCell<Vec<NodeRef>>>,
) -> NodeRef {
    let slice = &bytes[offset..(offset + size)];
    let mut decode = DeflateDecoder::new(slice);
    let mut data: Vec<u8> = Vec::new();
    decode.read_to_end(&mut data).unwrap();
    let wc = u8v_to_u32(&data[0..4]);
    let mut words: Vec<Word> = Vec::new();
    let mut pos: usize = 4;
    for _ in 0..wc {
        let word_length = u8v_to_u32(&data[pos..(pos + 4)]) as usize;
        pos += 4;
        let b = data[pos..(pos + word_length)].to_vec();
        pos += word_length;
        let key = String::from_utf8(b).unwrap();
        let value_length = u8v_to_u32(&data[pos..(pos + 4)]) as usize;
        pos += 4;
        let b = data[pos..(pos + value_length)].to_vec();
        pos += value_length;
        words.push(Word::with_value(key, b));
    }
    println!("Node [{} ~ {}]", words[0].key, words[words.len() - 1].key);
    let node_ref = create_node_ref_with_data(words, Vec::new());
    let mut children: Vec<NodeRef> = Vec::new();
    let cc = u8v_to_u32(&data[pos..(pos + 4)]);
    pos += 4;
    if cc > 1 {
        for _ in 0..cc {
            let offset = u8v_to_u64(&data[pos..(pos + 8)]) as usize;
            pos += 8;
            let size = u8v_to_u32(&data[pos..(pos + 4)]) as usize;
            pos += 4;
            let leaves_cloned = Rc::clone(&leaves);
            let child = create_node_ref_from_bytes(bytes, offset, size, leaves_cloned);
            child.borrow_mut().parent = Some(Rc::clone(&node_ref));
            children.push(child);
        }
    } else {
        leaves.borrow_mut().push(Rc::clone(&node_ref));
    }
    node_ref.borrow_mut().children = children;
    return node_ref;
}

struct Node {
    words: Vec<Word>,
    children: Vec<NodeRef>,
    parent: Option<NodeRef>,
    offset: u64,
    compressed_size: u32,
}

impl Node {
    fn new() -> Self {
        Self {
            words: Vec::new(),
            children: Vec::new(),
            parent: None,
            offset: 0,
            compressed_size: 0,
        }
    }

    fn with_data(words: Vec<Word>, children: Vec<NodeRef>) -> Self {
        Self {
            words,
            children,
            parent: None,
            offset: 0,
            compressed_size: 0,
        }
    }

    fn size(&self) -> usize {
        let mut size: usize = 8;
        for i in 0..self.words.len() {
            size += self.words[i].size();
        }
        if self.children.len() == 0 {
            size += 12;
        } else {
            size += 12 * self.children.len();
        }
        return size;
    }

    fn is_leaf(&self) -> bool {
        self.children.len() == 0
    }

    fn index_of(&self, word: &Word) -> (usize, i8) {
        let mut hi = self.words.len() - 1;
        let mut li = 0;
        loop {
            let mi = (hi + li) / 2;
            let mut c = self.words[mi].compare(word);
            if c > 0 {
                hi = mi;
            } else if c < 0 {
                li = mi;
            } else {
                return (mi, c);
            }
            if hi == li {
                return (hi, c);
            } else if hi - li == 1 {
                if mi == li {
                    return (hi, self.words[hi].compare(word));
                } else {
                    c = self.words[li].compare(word);
                    if c >= 0 {
                        return (li, c);
                    } else {
                        return (hi, 1);
                    }
                }
            }
        }
    }

    fn child_index(&self, child: &NodeRef) -> usize {
        for i in 0..self.children.len() {
            if Rc::ptr_eq(&self.children[i], &child) {
                return i;
            }
        }
        panic!("Not include children");
    }

    fn insert(&mut self, word: Word, index: usize) {
        if index == self.words.len() {
            self.words.push(word);
        } else {
            self.words.splice(index..index, [word]);
        }
    }

    fn add_child(&mut self, node: NodeRef, index: usize) {
        let last_child_index = self.children.len();
        if index == last_child_index {
            self.children.push(node);
        } else if last_child_index > 0 && index < last_child_index {
            self.children.splice(index..index, [node]);
        } else {
            panic!(
                "Invalid child index. index: {}, last child index: {}",
                index, last_child_index
            );
        }
    }

    fn to_vec(&mut self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        if self.words.len() + 1 > 2u64.pow(32) as usize {
            panic!("Node has too many words");
        }
        let mut wc = u32_to_u8v(self.words.len() as u32);
        buf.append(&mut wc);
        for i in 0..self.words.len() {
            let mut word_buf = self.words[i].to_vec();
            buf.append(&mut word_buf);
        }
        if self.children.len() == 0 {
            let mut cc_buf = u32_to_u8v(1);
            buf.append(&mut cc_buf);
        } else {
            let mut cc_buf = u32_to_u8v(self.children.len() as u32);
            buf.append(&mut cc_buf);
            for i in 0..self.children.len() {
                let child = self.children[i].borrow();
                let child_offset = child.offset;
                let mut co_buf = u64_to_u8v(child_offset);
                buf.append(&mut co_buf);
                let mut child_size_buf = u32_to_u8v(child.compressed_size);
                buf.append(&mut child_size_buf);
            }
        }
        return buf;
    }

    fn print(&self, indent: usize) {
        println!(
            "{:5} {:>10} Words {:>10} Children {:>10} Bytes [{} ~ {}]",
            "+".repeat(indent),
            self.words.len(),
            self.children.len(),
            self.size(),
            self.words[0].key,
            self.words[self.words.len() - 1].key,
        );
        for i in 0..self.children.len() {
            self.children[i].borrow().print(indent + 1);
        }
    }
}

#[derive(Clone, Copy)]
pub enum LapFileType {
    Word,
    Resource,
}

pub struct Laputa {
    metadata: Metadata,
    file_type: LapFileType,
    root: NodeRef,
    node_count: usize,
    leaves: Rc<RefCell<Vec<NodeRef>>>, // only for parse
}

impl Laputa {
    pub fn new(metadata: Metadata, file_type: LapFileType) -> Self {
        let root = Rc::new(RefCell::new(Node::new()));
        Self {
            metadata,
            file_type,
            root,
            node_count: 1,
            leaves: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn from_file(filepath: &str) -> LaputaResult<Self> {
        let ext = match parse_file_type(filepath) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };
        let mut file = match File::open(filepath) {
            Ok(r) => r,
            Err(_) => return Err(LaputaError::InvalidDictionary),
        };
        let mut buf: Vec<u8> = vec![0; 4];
        if let Ok(size) = file.read(&mut buf) {
            if size != buf.len() {
                return Err(LaputaError::InvalidDictionary);
            }
        } else {
            return Err(LaputaError::InvalidDictionary);
        }
        let metadata_length = u8v_to_u32(&buf[..]);
        buf = vec![0; metadata_length as usize];
        if let Ok(size) = file.read(&mut buf) {
            if size != buf.len() {
                return Err(LaputaError::InvalidDictionary);
            }
        } else {
            return Err(LaputaError::InvalidDictionary);
        }
        let metadata = match serde_json::from_slice(&buf[..]) {
            Ok(r) => r,
            Err(_) => {
                return Err(LaputaError::InvalidDictionary);
            }
        };
        let mut po = Self::new(metadata, ext);
        // root node
        if let Err(_) = file.seek(SeekFrom::End(-8)) {
            return Err(LaputaError::InvalidDictionary);
        }
        buf = vec![0; 8];
        if let Ok(size) = file.read(&mut buf) {
            if size != buf.len() {
                return Err(LaputaError::InvalidDictionary);
            }
        } else {
            return Err(LaputaError::InvalidDictionary);
        }
        let root_offset = u8v_to_u64(&buf[..]) as usize;
        let file_size = match file.metadata() {
            Ok(m) => m.len() as usize,
            Err(_) => return Err(LaputaError::InvalidDictionary),
        };
        if let Err(_) = file.seek(SeekFrom::Start(0)) {
            return Err(LaputaError::InvalidDictionary);
        }
        buf = vec![0; file_size];
        if let Ok(size) = file.read(&mut buf) {
            if size != buf.len() {
                return Err(LaputaError::InvalidDictionary);
            }
        } else {
            return Err(LaputaError::InvalidDictionary);
        }
        let leaves = Rc::clone(&po.leaves);
        po.root =
            create_node_ref_from_bytes(&buf[..], root_offset, file_size - 8 - root_offset, leaves);
        return Ok(po);
    }

    pub fn input_word(&mut self, name: String, value: Vec<u8>) {
        self.metadata.word_number += 1;
        if self.root.borrow().words.len() == 0 {
            self.root
                .borrow_mut()
                .insert(Word::with_value(name, value), 0);
            return;
        }
        let mut node_ref = Rc::clone(&self.root);
        let word = Word::with_value(name, value);
        loop {
            let tmp_node_ref = node_ref.clone();
            let node = tmp_node_ref.borrow();
            if node.is_leaf() {
                break;
            } else {
                let (idx, cmp) = node.index_of(&word);
                if cmp >= 0 {
                    node_ref = Rc::clone(&node.children[idx]);
                } else {
                    node_ref = Rc::clone(&node.children[idx + 1]);
                }
            }
        }
        {
            let mut node = node_ref.borrow_mut();
            let (idx, cmp) = node.index_of(&word);
            if cmp >= 0 {
                node.insert(word, idx);
            } else {
                node.insert(word, idx + 1);
            }
        }
        let mut div_node_ref = Rc::clone(&node_ref);
        loop {
            let tmp_div_node_ref = Rc::clone(&div_node_ref);
            let mut div_node = tmp_div_node_ref.borrow_mut();
            if div_node.is_leaf() {
                if div_node.size() > LEAF_NODE_SIZE {
                    // println!(
                    //     ">>> Divide leaf node [{} - {}]",
                    //     div_node.words[0].key,
                    //     div_node.words[div_node.words.len() - 1].key
                    // );
                    let div_index = div_node.words.len() / 2;
                    let words = div_node.words.drain(div_index..).collect();
                    let new_leaf_ref = create_node_ref_with_data(words, Vec::new());
                    let mut new_leaf = new_leaf_ref.borrow_mut();
                    let new_parent_key = new_leaf.words[0].key.clone();
                    match &div_node.parent {
                        Some(parent) => {
                            let mut pa = parent.borrow_mut();
                            // println!(
                            //     "> Deliver to parent [{} - {}]",
                            //     pa.words[0].key,
                            //     pa.words[pa.words.len() - 1].key
                            // );
                            let node_index = pa.child_index(&div_node_ref);
                            pa.insert(Word::new(new_parent_key), node_index);
                            pa.add_child(Rc::clone(&new_leaf_ref), node_index + 1);
                            new_leaf.parent = Some(Rc::clone(&parent));
                            div_node_ref = Rc::clone(&parent);
                            self.node_count += 1;
                        }
                        None => {
                            let parent = create_node_ref();
                            let mut pa = parent.borrow_mut();
                            pa.insert(Word::new(new_parent_key), 0);
                            // println!("> Create parent [{}]", pa.words[0].key,);
                            pa.add_child(Rc::clone(&div_node_ref), 0);
                            pa.add_child(Rc::clone(&new_leaf_ref), 1);
                            div_node.parent = Some(Rc::clone(&parent));
                            new_leaf.parent = Some(Rc::clone(&parent));
                            self.root = Rc::clone(&parent);
                            self.node_count += 2;
                            return;
                        }
                    }
                } else {
                    break;
                }
            } else if div_node.size() > INDEX_NODE_SIZE {
                // println!(
                //     ">>> Divide index node [{} - {}]",
                //     div_node.words[0].key,
                //     div_node.words[div_node.words.len() - 1].key
                // );
                let div_index = div_node.words.len() / 2 + 1;
                let mut words: Vec<Word> = div_node.words.drain(div_index..).collect();
                let pword = div_node.words.pop().unwrap();
                let children: Vec<NodeRef> = div_node.children.drain(div_index..).collect();
                let new_index_ref = create_node_ref();
                let mut new_index = new_index_ref.borrow_mut();
                new_index.words.append(&mut words);
                for i in 0..children.len() {
                    children[i].borrow_mut().parent = Some(Rc::clone(&new_index_ref));
                    new_index.add_child(Rc::clone(&children[i]), i);
                }
                match &div_node.parent {
                    Some(parent) => {
                        let mut pa = parent.borrow_mut();
                        // println!(
                        //     "> Deliver to parent [{} - {}]",
                        //     pa.words[0].key,
                        //     pa.words[pa.words.len() - 1].key
                        // );
                        let node_index = pa.child_index(&div_node_ref);
                        pa.insert(pword, node_index);
                        pa.add_child(Rc::clone(&new_index_ref), node_index + 1);
                        new_index.parent = Some(Rc::clone(&parent));
                        div_node_ref = Rc::clone(&parent);
                        self.node_count += 1;
                    }
                    None => {
                        let parent = create_node_ref();
                        let mut pa = parent.borrow_mut();
                        pa.insert(pword, 0);
                        // println!("> Create parent [{}]", pa.words[0].key);
                        pa.add_child(Rc::clone(&div_node_ref), 0);
                        pa.add_child(Rc::clone(&new_index_ref), 1);
                        div_node.parent = Some(Rc::clone(&parent));
                        new_index.parent = Some(Rc::clone(&parent));
                        self.root = Rc::clone(&parent);
                        self.node_count += 2;
                        return;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn print(&self) {
        println!("Dictionary Structure");
        self.root.borrow().print(1);
    }

    pub fn save(&mut self, dest: &str) {
        self.print();
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
        // node
        let mut node_ref = Rc::clone(&self.root);
        loop {
            let tmp_node_ref = Rc::clone(&node_ref);
            let tmp_node = tmp_node_ref.borrow();
            if tmp_node.is_leaf() {
                break;
            } else {
                let last_index = tmp_node.children.len() - 1;
                node_ref = Rc::clone(&tmp_node.children[last_index]);
            }
        }
        let mut offset: u64 = file.stream_position().expect("Fail to get stream position");
        let mut leaf_offset: u64 = 0;
        let mut leaf_size: u32 = 0;
        loop {
            let tmp_node_ref = Rc::clone(&node_ref);
            let mut tmp_node = tmp_node_ref.borrow_mut();
            if !tmp_node.is_leaf() {
                let mut children_saved = true;
                for i in (0..tmp_node.children.len()).rev() {
                    let tmp_child_node_ref = Rc::clone(&tmp_node.children[i]);
                    let tmp_child_node = tmp_child_node_ref.borrow();
                    if tmp_child_node.offset == 0 {
                        children_saved = false;
                        node_ref = Rc::clone(&tmp_child_node_ref);
                        break;
                    }
                }
                if !children_saved {
                    continue;
                }
            }
            let mut node_buf = tmp_node.to_vec();
            if tmp_node.is_leaf() {
                let mut leaf_offset_buf = u64_to_u8v(leaf_offset);
                node_buf.append(&mut leaf_offset_buf);
                let mut leaf_size_buf = u32_to_u8v(leaf_size);
                node_buf.append(&mut leaf_size_buf);
            }
            tmp_node.offset = offset;
            let buf = compress(&node_buf);
            tmp_node.compressed_size = buf.len() as u32;
            offset += buf.len() as u64;
            if tmp_node.is_leaf() {
                leaf_offset = tmp_node.offset;
                leaf_size = buf.len() as u32;
            }
            file.write_all(&buf).expect("Failt to write");
            match &tmp_node.parent {
                Some(p) => {
                    node_ref = Rc::clone(&p);
                }
                None => break,
            }
        }
        let offset_buf = u64_to_u8v(self.root.borrow().offset);
        file.write_all(&offset_buf).expect("Fail to write");
        println!("Done\n{} - {:.2}M", dest, (offset as f64) / 1024.0 / 1024.0);
    }

    pub fn to_raw(&self, dest: &str) {
        let mut raw = RawDict::new(dest);
        let leaves = self.leaves.borrow();
        let is_text = match self.file_type {
            LapFileType::Word => true,
            LapFileType::Resource => false,
        };
        for i in 0..leaves.len() {
            let words = &leaves[i].borrow().words;
            for word in words {
                raw.insert(word.key.as_str(), &word.value[..], is_text);
            }
        }
        raw.flush(is_text);
    }
}
