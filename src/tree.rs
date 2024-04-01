use crate::error::Result;
use crate::utils::{u32_to_u8v, u64_to_u8v, Scanner};
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    io::{Read, SeekFrom, Write},
    ptr::NonNull,
};
use tokio::io::AsyncWriteExt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::{debug, info, instrument};

fn compress(buf: &[u8]) -> Vec<u8> {
    let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
    e.write_all(buf).expect("DeflateEncoder: Fail to write");
    return e.finish().expect("DeflateEncoder: Fail to finish");
}

fn create_non_null<T>(value: Box<T>) -> NonNull<T> {
    NonNull::from(Box::leak(value))
}

pub trait Serializable {
    fn size(&self) -> usize;
    fn bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
}

pub trait Smoothable {
    fn smooth(&self) -> Self;
}

#[derive(Debug, Clone)]
pub struct Record<K, V> {
    pub key: K,
    pub value: Option<V>,
}

impl<K: Serializable, V: Serializable> Record<K, V> {
    fn new(key: K) -> Self {
        Self { key, value: None }
    }

    fn with_value(key: K, value: V) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }

    fn size(&self) -> usize {
        let mut size = self.key.size() + 4/* key length */;
        if let Some(v) = &self.value {
            size += v.size() + 4/* value length */;
        }
        size
    }

    fn bytes(&self) -> Vec<u8> {
        let mut data: Vec<u8> = vec![];
        let mut size_bytes = u32_to_u8v(self.key.size() as u32);
        data.append(&mut size_bytes);
        let mut key_bytes = self.key.bytes();
        data.append(&mut key_bytes);
        if let Some(v) = &self.value {
            size_bytes = u32_to_u8v(v.size() as u32);
            data.append(&mut size_bytes);
            let mut value_bytes = v.bytes();
            data.append(&mut value_bytes);
        }
        data
    }
}

#[derive(Debug, Clone)]
pub struct Node<K, V> {
    pub is_leaf: bool,
    pub records: Vec<Record<K, V>>,
    pub children: Vec<NonNull<Node<K, V>>>,
    parent: Option<NonNull<Node<K, V>>>,
    offset: u64,
    zip_size: u32,
}

unsafe impl<K, V> Send for Node<K, V> {}
unsafe impl<K, V> Sync for Node<K, V> {}

impl<
        K: PartialOrd + Ord + Serializable + Smoothable + Display + Debug + Clone,
        V: Serializable,
    > Node<K, V>
{
    pub fn new(is_leaf: bool) -> Self {
        Self {
            is_leaf,
            records: vec![],
            children: vec![],
            parent: None,
            offset: 0,
            zip_size: 0,
        }
    }

    pub fn new_ptr(is_leaf: bool) -> NonNull<Self> {
        let node = Box::new(Self::new(is_leaf));
        NonNull::from(Box::leak(node))
    }

    pub fn from_bytes(data: Vec<u8>) -> (Box<Self>, Vec<(u64, u32)>) {
        let mut scanner = Scanner::new(data);
        let is_leaf = scanner.read_u8() == 0;
        let rec_num = scanner.read_u32();
        let mut records: Vec<Record<K, V>> = vec![];
        for _ in 0..rec_num {
            let key_len = scanner.read_u32() as usize;
            let b = scanner.read(key_len);
            let key = K::from_bytes(&b);
            let rec = if is_leaf {
                let value_length = scanner.read_u32() as usize;
                let b = scanner.read(value_length);
                let value = V::from_bytes(&b);
                Record::with_value(key, value)
            } else {
                Record::new(key)
            };
            records.push(rec)
        }
        let mut node = Box::new(Node::new(is_leaf));
        node.records = records;
        let mut children: Vec<(u64, u32)> = vec![];
        let cc = if is_leaf { 1 } else { rec_num + 1 };
        for _ in 0..cc {
            let offset = scanner.read_u64();
            let size = scanner.read_u32();
            children.push((offset, size));
        }
        (node, children)
    }

    #[instrument(skip(self))]
    pub fn index_of(&self, key: &K) -> (usize, Ordering) {
        info!("{} NODE", if self.is_leaf { "LEAF" } else { "INDEX" });
        let key = key.smooth();
        let mut hi = self.records.len() - 1;
        let mut li = 0;
        let ret: (usize, Ordering);
        loop {
            let mi = (hi + li) / 2;
            debug!(
                "{}[{}] {}[{}] {}[{}]",
                li, &self.records[li].key, mi, &self.records[mi].key, hi, &self.records[hi].key
            );
            let mi_key = &self.records[mi].key.clone();
            let cr = if self.is_leaf {
                key.cmp(&mi_key.smooth())
            } else {
                key.cmp(mi_key)
            };
            if hi == li {
                ret = (hi, cr);
                break;
            }
            if hi - li == 1 {
                if mi == li {
                    if cr.is_le() {
                        ret = (li, cr);
                        break;
                    }
                    let hi_key = &self.records[hi].key;
                    let cr = if self.is_leaf {
                        key.cmp(&hi_key.smooth())
                    } else {
                        key.cmp(hi_key)
                    };
                    ret = (hi, cr);
                    break;
                }
                ret = (li, cr);
                break;
            }
            if cr.is_lt() {
                hi = mi;
            } else if cr.is_gt() {
                li = mi;
            } else {
                ret = (mi, cr);
                break;
            }
        }
        info!("index: {}, Ordering: {:?}", ret.0, ret.1);
        ret
    }

    fn size(&self) -> usize {
        let mut size: usize = 1/* is leaf */ + 4/* record number */;
        for i in 0..self.records.len() {
            size += self.records[i].size();
        }
        if self.is_leaf {
            size += 8/* next sibling offset */ + 4/* next sibling size */;
        } else {
            size += (8/* child offset */ + 4/* child size */) * self.children.len();
        }
        return size;
    }

    fn child_index_of(&self, child: NonNull<Node<K, V>>) -> Option<usize> {
        for (i, chd) in self.children.iter().enumerate() {
            if child == *chd {
                return Some(i);
            }
        }
        None
    }

    fn bytes(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        if self.records.len() + 1 > 2u64.pow(32) as usize {
            panic!("Node is too large");
        }
        if self.is_leaf {
            buf.append(&mut vec![0u8]);
        } else {
            buf.append(&mut vec![1u8]);
        }
        let mut wc = u32_to_u8v(self.records.len() as u32);
        buf.append(&mut wc);
        for i in 0..self.records.len() {
            let mut rec_buf = self.records[i].bytes();
            buf.append(&mut rec_buf);
        }
        for i in 0..self.children.len() {
            let child = unsafe { self.children[i].as_ref() };
            let child_offset = child.offset;
            let mut co_buf = u64_to_u8v(child_offset);
            buf.append(&mut co_buf);
            let mut child_size_buf = u32_to_u8v(child.zip_size);
            buf.append(&mut child_size_buf);
        }
        buf
    }

    fn print(&self, level: usize) {
        let flag = if self.is_leaf { "LEAF" } else { "INDEX" };
        println!(
            "{:5} ({:10}, {:5}) {:5} {:5} [{} ~ {}]",
            "+".repeat(level),
            self.offset,
            self.zip_size,
            flag,
            self.records.len(),
            self.records[0].key,
            self.records.last().unwrap().key
        );
        for child in &self.children {
            unsafe { child.as_ref().print(level + 1) };
        }
    }
}

async fn parse_node<
    K: PartialOrd + Ord + Serializable + Smoothable + Clone + Display + Debug,
    V: Serializable,
>(
    file: &mut File,
    offset: u64,
    size: u32,
    leaves: &mut Vec<NonNull<Node<K, V>>>,
    level: usize,
) -> Result<(NonNull<Node<K, V>>, usize)> {
    if size == 0 {
        return Ok((Node::new_ptr(true), 1));
    }
    file.seek(SeekFrom::Start(offset)).await?;
    let mut bytes = vec![0; size as usize];
    file.read_exact(&mut bytes).await?;
    let mut decode = DeflateDecoder::new(&bytes[..]);
    let mut data: Vec<u8> = vec![];
    decode.read_to_end(&mut data).unwrap();
    let (mut node, children) = Node::<K, V>::from_bytes(data);
    node.offset = offset;
    node.zip_size = size;
    node.print(level);
    let is_leaf = node.is_leaf;
    let mut node_ptr = create_non_null(node);
    let mut node_num = 1;
    if is_leaf {
        leaves.push(node_ptr);
    } else {
        for child in children {
            if child.1 == 0 {
                break;
            }
            let (mut child_node_ptr, child_node_num) =
                Box::pin(parse_node(file, child.0, child.1, leaves, level + 1)).await?;
            let child_node = unsafe { child_node_ptr.as_mut() };
            unsafe { node_ptr.as_mut().children.push(child_node_ptr) };
            child_node.parent = Some(node_ptr);
            node_num += child_node_num;
        }
    }
    Ok((node_ptr, node_num))
}

pub struct Tree<K, V> {
    root: NonNull<Node<K, V>>,
    leaves: NonNull<Vec<NonNull<Node<K, V>>>>,
    node_num: usize,
    index_size_limit: usize,
    leaf_size_limit: usize,
}

unsafe impl<K, V> Send for Tree<K, V> {}
unsafe impl<K, V> Sync for Tree<K, V> {}

impl<
        K: PartialOrd + Ord + Serializable + Smoothable + Clone + Display + Debug,
        V: Serializable,
    > Tree<K, V>
{
    pub fn new(index_size_limit: usize, leaf_size_limit: usize) -> Self {
        let root = Node::new_ptr(true);
        let leaves: Box<Vec<NonNull<Node<K, V>>>> = Box::new(vec![root]);
        let leaves_ptr = NonNull::from(Box::leak(leaves));
        Self {
            root,
            leaves: leaves_ptr,
            node_num: 1,
            index_size_limit,
            leaf_size_limit,
        }
    }

    pub async fn from_file(
        file: &mut File,
        root_offset: u64,
        root_size: u32,
        index_size_limit: usize,
        leaf_size_limit: usize,
    ) -> Result<Self> {
        let mut leaves = Box::<Vec<NonNull<Node<K, V>>>>::new(vec![]);
        let (root, node_num) = parse_node(file, root_offset, root_size, &mut leaves, 1).await?;
        let leaves_ptr = NonNull::from(Box::leak(leaves));
        Ok(Self {
            root,
            leaves: leaves_ptr,
            node_num,
            index_size_limit,
            leaf_size_limit,
        })
    }

    #[allow(dead_code)]
    pub fn print(&self) {
        unsafe { self.root.as_ref().print(1) };
    }

    pub fn insert(&mut self, key: K, value: V) {
        let root = unsafe { self.root.as_mut() };
        if root.records.len() == 0 {
            root.records.push(Record::with_value(key, value));
            return;
        }
        let mut node_ptr = self.root;
        loop {
            let node = unsafe { node_ptr.as_ref() };
            if node.is_leaf {
                break;
            }
            let (idx, cr) = node.index_of(&key);
            let child_idx = if cr.is_le() { idx } else { idx + 1 };
            node_ptr = node.children[child_idx];
        }
        {
            let leaf_node = unsafe { node_ptr.as_mut() };
            let (idx, cr) = leaf_node.index_of(&key);
            let rec = Record::with_value(key, value);
            if cr.is_le() {
                leaf_node.records.insert(idx, rec);
            } else {
                leaf_node.records.insert(idx + 1, rec);
            }
        }
        let mut div_node_ptr = node_ptr;
        loop {
            let div_node = unsafe { div_node_ptr.as_mut() };
            if div_node.is_leaf {
                if div_node.records.len() > 1 && div_node.size() > self.leaf_size_limit {
                    self.node_num += 1;
                    let div_idx = div_node.records.len() / 2;
                    let right_records = div_node.records.drain(div_idx..).collect();
                    let mut new_node_ptr = Node::new_ptr(true);
                    let new_node = unsafe { new_node_ptr.as_mut() };
                    new_node.records = right_records;
                    unsafe { self.leaves.as_mut().push(new_node_ptr) };
                    let new_parent_key = div_node.records[div_idx - 1].key.smooth();
                    if let Some(mut parent) = div_node.parent {
                        let pnode = unsafe { parent.as_mut() };
                        new_node.parent = div_node.parent;
                        let child_idx = pnode.child_index_of(div_node_ptr).unwrap();
                        pnode.records.insert(child_idx, Record::new(new_parent_key));
                        pnode.children.insert(child_idx + 1, new_node_ptr);
                        div_node_ptr = parent;
                    } else {
                        let mut pnode_ptr = Node::new_ptr(false);
                        div_node.parent = Some(pnode_ptr);
                        new_node.parent = Some(pnode_ptr);
                        let pnode = unsafe { pnode_ptr.as_mut() };
                        pnode.records.push(Record::new(new_parent_key));
                        pnode.children.push(div_node_ptr);
                        pnode.children.push(new_node_ptr);
                        self.root = pnode_ptr;
                        break;
                    }
                } else {
                    break;
                }
            } else if div_node.size() > self.index_size_limit && div_node.records.len() >= 3 {
                self.node_num += 1;
                let div_idx = div_node.records.len() / 2 + 1;
                let right_records = div_node.records.drain(div_idx..).collect();
                let precord = div_node.records.pop().unwrap();
                let mut new_node_ptr = Node::new_ptr(false);
                let new_node = unsafe { new_node_ptr.as_mut() };
                new_node.records = right_records;
                let mut right_children: Vec<NonNull<Node<K, V>>> =
                    div_node.children.drain(div_idx..).collect();
                for i in 0..right_children.len() {
                    unsafe { right_children[i].as_mut().parent = Some(new_node_ptr) };
                    new_node.children.push(right_children[i]);
                }
                if let Some(mut parent) = div_node.parent {
                    let pnode = unsafe { parent.as_mut() };
                    new_node.parent = div_node.parent.clone();
                    let child_idx = pnode.child_index_of(div_node_ptr).unwrap();
                    pnode.records.insert(child_idx, precord);
                    pnode.children.insert(child_idx + 1, new_node_ptr);
                    div_node_ptr = parent;
                } else {
                    let mut pnode_ptr = Node::new_ptr(false);
                    div_node.parent = Some(pnode_ptr);
                    new_node.parent = Some(pnode_ptr);
                    let pnode = unsafe { pnode_ptr.as_mut() };
                    pnode.records.push(precord);
                    pnode.children.push(div_node_ptr);
                    pnode.children.push(new_node_ptr);
                    self.root = pnode_ptr;
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub async fn write_to(&self, file: &mut File) -> Result<(u64, u32)> {
        if unsafe { self.root.as_ref().records.len() } == 0 {
            return Ok((0, 0));
        }
        let mut node_ptr = self.root;
        loop {
            let tmp_node = unsafe { node_ptr.as_ref() };
            if tmp_node.is_leaf {
                break;
            }
            let last_index = tmp_node.children.len() - 1;
            node_ptr = tmp_node.children[last_index];
        }
        let mut offset = file.stream_position().await?;
        let mut leaf_offset: u64 = 0;
        let mut leaf_size: u32 = 0;
        let mut saved_num = 0;
        loop {
            let tmp_node = unsafe { node_ptr.as_mut() };
            if !tmp_node.is_leaf {
                let mut children_all_saved = true;
                for i in (0..tmp_node.children.len()).rev() {
                    let tmp_child_node_ptr = tmp_node.children[i];
                    let tmp_child_node = unsafe { tmp_child_node_ptr.as_ref() };
                    if tmp_child_node.offset == 0 {
                        children_all_saved = false;
                        node_ptr = tmp_child_node_ptr;
                        break;
                    }
                }
                if !children_all_saved {
                    continue;
                }
            }
            let mut node_buf = tmp_node.bytes();
            if tmp_node.is_leaf {
                let mut leaf_offset_buf = u64_to_u8v(leaf_offset);
                node_buf.append(&mut leaf_offset_buf);
                let mut leaf_size_buf = u32_to_u8v(leaf_size);
                node_buf.append(&mut leaf_size_buf);
            }
            tmp_node.offset = offset;
            let buf = compress(&node_buf);
            tmp_node.zip_size = buf.len() as u32;
            offset += buf.len() as u64;
            if tmp_node.is_leaf {
                leaf_offset = tmp_node.offset;
                leaf_size = buf.len() as u32;
            }
            file.write(&buf).await?;
            saved_num += 1;
            print!(
                "\r{} / {} {:.2}%",
                saved_num,
                self.node_num,
                (saved_num as f64) / (self.node_num as f64) * 100.0
            );
            std::io::stdout().flush().unwrap();
            match tmp_node.parent {
                Some(p) => {
                    node_ptr = p;
                }
                None => break,
            }
        }
        print!("\n");
        file.flush().await?;
        let root_node = unsafe { self.root.as_ref() };
        Ok((root_node.offset, root_node.zip_size))
    }

    #[allow(dead_code)]
    pub fn record_num(&self) -> usize {
        let mut size: usize = 0;
        for leaf in unsafe { self.leaves.as_ref() } {
            size += unsafe { leaf.as_ref().records.len() };
        }
        size
    }

    pub fn traverse<F>(&self, mut cb: F)
    where
        F: FnMut(&K, &V),
    {
        for leaf in unsafe { self.leaves.as_ref() } {
            for rec in unsafe { &leaf.as_ref().records } {
                cb(&rec.key, &rec.value.as_ref().unwrap());
            }
        }
    }
}
