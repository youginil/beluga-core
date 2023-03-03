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

pub fn u8v_to_u16(v: &[u8]) -> u16 {
    if v.len() != 2 {
        panic!("Invalid vector size");
    }
    ((v[0] as u16) << 8) | (v[1] as u16)
}

// pub fn u16_to_u8v(v: u16) -> Vec<u8> {
//     let mut r = Vec::new();
//     r.push((v >> 8) as u8);
//     r.push(v as u8);
//     return r;
// }

pub enum Endianness {
    #[allow(dead_code)]
    Big,
    Little,
}

pub fn u8v_to_u16v(v: &[u8], endian: Endianness) -> Vec<u16> {
    if v.len() % 2 != 0 {
        panic!("Invalid vector size");
    }
    let l = v.len() / 2;
    let mut r: Vec<u16> = vec![0; l];
    for i in 0..l {
        match endian {
            Endianness::Big => {
                r[i] |= (v[i * 2] as u16) << 8;
                r[i] |= v[i * 2 + 1] as u16;
            }
            Endianness::Little => {
                r[i] |= v[i * 2] as u16;
                r[i] |= (v[i * 2 + 1] as u16) << 8;
            }
        }
    }
    r
}
