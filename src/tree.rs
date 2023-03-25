use crate::utils::{file_read, file_seek, u32_to_u8v, u64_to_u8v, Scanner};
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    rc::Rc,
};
use tracing::{debug, info, instrument};

fn compress(buf: &[u8]) -> Vec<u8> {
    let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
    e.write_all(buf).expect("DeflateEncoder: Fail to write");
    return e.finish().expect("DeflateEncoder: Fail to finish");
}

pub trait Serializable {
    fn size(&self) -> usize;
    fn bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
}

pub trait Smoothable {
    fn smooth(&self) -> Self;
}

type NodeRef<K, V> = Rc<RefCell<Node<K, V>>>;

fn create_node_ref<K, V>(node: Node<K, V>) -> NodeRef<K, V> {
    Rc::new(RefCell::new(node))
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Node<K, V> {
    pub is_leaf: bool,
    pub records: Vec<Record<K, V>>,
    children: Vec<NodeRef<K, V>>,
    parent: Option<NodeRef<K, V>>,
    offset: u64,
    zip_size: u32,
}

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

    pub fn from_bytes(data: Vec<u8>) -> (Self, Vec<(u64, u32)>) {
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
        let mut node = Node::new(is_leaf);
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
            } else if hi - li == 1 {
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
                } else {
                    ret = (li, cr);
                    break;
                }
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

    fn child_index_of(&self, child: &NodeRef<K, V>) -> Option<usize> {
        for (i, chd) in self.children.iter().enumerate() {
            if Rc::ptr_eq(&child, chd) {
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
            let child = self.children[i].borrow();
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
            child.borrow().print(level + 1);
        }
    }
}

fn parse_node<
    K: PartialOrd + Ord + Serializable + Smoothable + Clone + Display + Debug,
    V: Serializable,
>(
    file: &mut File,
    offset: u64,
    size: u32,
    leaves: &mut Vec<NodeRef<K, V>>,
    level: usize,
) -> (NodeRef<K, V>, usize) {
    if size == 0 {
        return (create_node_ref(Node::new(true)), 1);
    }
    file_seek(file, SeekFrom::Start(offset)).unwrap();
    let bytes = file_read(file, size as usize).unwrap();
    let mut decode = DeflateDecoder::new(&bytes[..]);
    let mut data: Vec<u8> = vec![];
    decode.read_to_end(&mut data).unwrap();
    let (node, children) = Node::<K, V>::from_bytes(data);
    let node_ref = create_node_ref(node);
    node_ref.borrow_mut().offset = offset;
    node_ref.borrow_mut().zip_size = size;
    node_ref.borrow().print(level);
    let mut node_num = 1;
    if node_ref.borrow().is_leaf {
        leaves.push(node_ref.clone());
    } else {
        for child in children {
            if child.1 == 0 {
                break;
            }
            let (child_node_ref, child_node_num) =
                parse_node(file, child.0, child.1, leaves, level + 1);
            child_node_ref.borrow_mut().parent = Some(node_ref.clone());
            node_ref.borrow_mut().children.push(child_node_ref);
            node_num += child_node_num;
        }
    }
    (node_ref, node_num)
}

pub struct Tree<K, V> {
    root: NodeRef<K, V>,
    leaves: Vec<NodeRef<K, V>>,
    node_num: usize,
    index_size_limit: usize,
    leaf_size_limit: usize,
}

impl<
        K: PartialOrd + Ord + Serializable + Smoothable + Clone + Display + Debug,
        V: Serializable,
    > Tree<K, V>
{
    pub fn new(index_size_limit: usize, leaf_size_limit: usize) -> Self {
        let root = create_node_ref(Node::new(true));
        let leaf = root.clone();
        Self {
            root,
            leaves: vec![leaf],
            node_num: 1,
            index_size_limit,
            leaf_size_limit,
        }
    }

    pub fn from_file(
        file: &mut File,
        root_offset: u64,
        root_size: u32,
        index_size_limit: usize,
        leaf_size_limit: usize,
    ) -> Self {
        let mut leaves: Vec<NodeRef<K, V>> = vec![];
        let (root, node_num) = parse_node(file, root_offset, root_size, &mut leaves, 1);
        Self {
            root,
            leaves,
            node_num,
            index_size_limit,
            leaf_size_limit,
        }
    }

    #[allow(dead_code)]
    pub fn print(&self) {
        self.root.borrow().print(1);
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.root.borrow().records.len() == 0 {
            self.root
                .borrow_mut()
                .records
                .push(Record::with_value(key, value));
            return;
        }
        let mut node_ref = self.root.clone();
        loop {
            let tmp_node_ref = node_ref.clone();
            let node = tmp_node_ref.borrow();
            if node.is_leaf {
                break;
            } else {
                let (idx, cr) = node.index_of(&key);
                let child_idx = if cr.is_le() { idx } else { idx + 1 };
                node_ref = node.children[child_idx].clone();
            }
        }
        {
            let mut leaf_node = node_ref.borrow_mut();
            let (idx, cr) = leaf_node.index_of(&key);
            let rec = Record::with_value(key, value);
            if cr.is_le() {
                leaf_node.records.insert(idx, rec);
            } else {
                leaf_node.records.insert(idx + 1, rec);
            }
        }
        let mut div_node_ref = node_ref.clone();
        loop {
            let tmp_node_ref = div_node_ref.clone();
            let mut div_node = tmp_node_ref.borrow_mut();
            if div_node.is_leaf {
                if div_node.records.len() > 1 && div_node.size() > self.leaf_size_limit {
                    self.node_num += 1;
                    let div_idx = div_node.records.len() / 2;
                    let right_records = div_node.records.drain(div_idx..).collect();
                    let mut new_node = Node::new(true);
                    new_node.records = right_records;
                    let new_node_ref = create_node_ref(new_node);
                    self.leaves.push(new_node_ref.clone());
                    let mut new_node = new_node_ref.borrow_mut();
                    let new_parent_key = div_node.records[div_idx - 1].key.smooth();
                    if let Some(parent) = &div_node.parent {
                        let mut pnode = parent.borrow_mut();
                        new_node.parent = div_node.parent.clone();
                        let child_idx = pnode.child_index_of(&div_node_ref).unwrap();
                        pnode.records.insert(child_idx, Record::new(new_parent_key));
                        pnode.children.insert(child_idx + 1, new_node_ref.clone());
                        div_node_ref = parent.clone();
                    } else {
                        let pnode_ref = create_node_ref(Node::new(false));
                        div_node.parent = Some(pnode_ref.clone());
                        new_node.parent = Some(pnode_ref.clone());
                        let mut pnode = pnode_ref.borrow_mut();
                        pnode.records.push(Record::new(new_parent_key));
                        pnode.children.push(div_node_ref.clone());
                        pnode.children.push(new_node_ref.clone());
                        self.root = pnode_ref.clone();
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
                let mut new_node = Node::new(false);
                new_node.records = right_records;
                let right_children: Vec<NodeRef<K, V>> =
                    div_node.children.drain(div_idx..).collect();
                let new_node_ref = create_node_ref(new_node);
                let mut new_node = new_node_ref.borrow_mut();
                for i in 0..right_children.len() {
                    right_children[i].borrow_mut().parent = Some(new_node_ref.clone());
                    new_node.children.push(right_children[i].clone());
                }
                if let Some(parent) = &div_node.parent {
                    let mut pnode = parent.borrow_mut();
                    new_node.parent = div_node.parent.clone();
                    let child_idx = pnode.child_index_of(&div_node_ref).unwrap();
                    pnode.records.insert(child_idx, precord);
                    pnode.children.insert(child_idx + 1, new_node_ref.clone());
                    div_node_ref = parent.clone();
                } else {
                    let pnode_ref = create_node_ref(Node::new(false));
                    div_node.parent = Some(pnode_ref.clone());
                    new_node.parent = Some(pnode_ref.clone());
                    let mut pnode = pnode_ref.borrow_mut();
                    pnode.records.push(precord);
                    pnode.children.push(div_node_ref.clone());
                    pnode.children.push(new_node_ref.clone());
                    self.root = pnode_ref.clone();
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub fn write_to(&self, file: &mut File) -> (u64, u32) {
        if self.root.borrow().records.len() == 0 {
            return (0, 0);
        }
        let mut node_ref = self.root.clone();
        loop {
            let tmp_node_ref = node_ref.clone();
            let tmp_node = tmp_node_ref.borrow();
            if tmp_node.is_leaf {
                break;
            } else {
                let last_index = tmp_node.children.len() - 1;
                node_ref = Rc::clone(&tmp_node.children[last_index]);
            }
        }
        let mut offset = file.stream_position().expect("Fail to get stream position");
        let mut leaf_offset: u64 = 0;
        let mut leaf_size: u32 = 0;
        let mut saved_num = 0;
        loop {
            let tmp_node_ref = node_ref.clone();
            let mut tmp_node = tmp_node_ref.borrow_mut();
            if !tmp_node.is_leaf {
                let mut children_all_saved = true;
                for i in (0..tmp_node.children.len()).rev() {
                    let tmp_child_node_ref = tmp_node.children[i].clone();
                    let tmp_child_node = tmp_child_node_ref.borrow();
                    if tmp_child_node.offset == 0 {
                        children_all_saved = false;
                        node_ref = tmp_child_node_ref.clone();
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
            file.write(&buf).expect("Failt to write");
            saved_num += 1;
            print!(
                "\r{} / {} {:.2}%",
                saved_num,
                self.node_num,
                (saved_num as f64) / (self.node_num as f64) * 100.0
            );
            std::io::stdout().flush().unwrap();
            match &tmp_node.parent {
                Some(p) => {
                    node_ref = p.clone();
                }
                None => break,
            }
        }
        print!("\n");
        file.sync_all().unwrap();
        let root_node = self.root.borrow();
        (root_node.offset, root_node.zip_size)
    }

    #[allow(dead_code)]
    pub fn record_num(&self) -> usize {
        let mut size: usize = 0;
        for leaf in &self.leaves {
            size += leaf.borrow().records.len();
        }
        size
    }

    pub fn traverse<F>(&self, mut cb: F)
    where
        F: FnMut(&K, &V),
    {
        for leaf in &self.leaves {
            for rec in &leaf.borrow().records {
                cb(&rec.key, &rec.value.as_ref().unwrap());
            }
        }
    }
}
