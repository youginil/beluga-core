use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    string::FromUtf8Error,
};

use crate::error::{LaputaError, LaputaResult};

pub fn u8v_to_u64(v: &[u8]) -> u64 {
    if v.len() != 8 {
        panic!("Invalid vector size");
    }
    let mut r: u64 = 0;
    for i in 0..8 {
        r |= (v[i] as u64) << (7 - i) * 8;
    }
    r
}

pub fn u64_to_u8v(v: u64) -> Vec<u8> {
    let mut r: Vec<u8> = Vec::new();
    for i in (0..8).rev() {
        let elem = (v >> (i * 8)) as u8;
        r.push(elem);
    }
    return r;
}

pub fn u8v_to_u32(v: &[u8]) -> u32 {
    if v.len() != 4 {
        panic!("Invalid vector size");
    }
    let mut r: u32 = 0;
    for i in 0..4 {
        r |= (v[i] as u32) << (3 - i) * 8;
    }
    r
}

pub fn u32_to_u8v(v: u32) -> Vec<u8> {
    let mut r: Vec<u8> = Vec::new();
    for i in (0..4).rev() {
        let elem = (v >> (i * 8)) as u8;
        r.push(elem);
    }
    return r;
}

pub struct Scanner {
    buf: Vec<u8>,
    pos: usize,
}

impl Scanner {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn forward(&mut self, n: usize) {
        self.pos += n;
    }

    pub fn read(&mut self, n: usize) -> Vec<u8> {
        self.buf[self.pos..self.pos + n].to_vec()
    }

    pub fn read_u64(&mut self) -> u64 {
        let r = u8v_to_u64(&self.buf[self.pos..self.pos + 8]);
        self.forward(8);
        r
    }

    pub fn read_u32(&mut self) -> u32 {
        let r = u8v_to_u32(&self.buf[self.pos..self.pos + 4]);
        self.forward(4);
        r
    }

    pub fn read_string(&mut self, n: usize) -> Result<String, FromUtf8Error> {
        let s = String::from_utf8(self.buf[self.pos..self.pos + n].to_vec());
        self.pos += n;
        s
    }
}

pub fn file_read(file: &mut File, n: usize) -> LaputaResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(n);
    if let Ok(size) = file.read(&mut buf) {
        if size != buf.len() {
            return Err(LaputaError::InvalidDictFile);
        }
    } else {
        return Err(LaputaError::InvalidDictFile);
    }
    Ok(buf)
}

pub fn file_seek(file: &mut File, pos: SeekFrom) -> LaputaResult<()> {
    if let Err(_) = file.seek(pos) {
        return Err(LaputaError::InvalidDictFile);
    }
    Ok(())
}

pub fn file_metadata(file: &File) -> LaputaResult<std::fs::Metadata> {
    if let Ok(m) = file.metadata() {
        return Ok(m);
    } else {
        return Err(LaputaError::InvalidDictFile);
    }
}

pub fn file_open(filepath: &str) -> LaputaResult<File> {
    match File::open(filepath) {
        Ok(r) => Ok(r),
        Err(_) => return Err(LaputaError::InvalidDictFile),
    }
}
