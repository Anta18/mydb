use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Result};

/// NodeType indicates leaf or internal node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

/// Common header fields for both node types
pub struct NodeHeader {
    pub node_type: NodeType, // 1 byte
    pub key_count: u16,      // 2 bytes
    pub parent: u64,         // 8 bytes (optional)
}

impl NodeHeader {
    pub const SIZE: usize = 1 + 2 + 8;

    pub fn serialize(&self, buf: &mut [u8]) {
        buf[0] = self.node_type as u8;
        (&mut buf[1..3])
            .write_u16::<LittleEndian>(self.key_count)
            .unwrap();
        (&mut buf[3..11])
            .write_u64::<LittleEndian>(self.parent)
            .unwrap();
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        let node_type = match buf[0] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid node type",
                ));
            }
        };
        let mut rdr = Cursor::new(&buf[1..3]);
        let key_count = rdr.read_u16::<LittleEndian>()?;
        let mut rdr2 = Cursor::new(&buf[3..11]);
        let parent = rdr2.read_u64::<LittleEndian>()?;
        Ok(NodeHeader {
            node_type,
            key_count,
            parent,
        })
    }
}

/// Serializer for internal node pages
pub struct InternalNodeSerializer {
    pub order: usize,
}

impl InternalNodeSerializer {
    /// Serialize internal node: header + keys + children pointers
    /// Layout: [header][keys...][children...]
    pub fn serialize(
        &self,
        header: &NodeHeader,
        keys: &[u64],     // length = key_count
        children: &[u64], // length = key_count + 1
        page_size: usize,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; page_size];
        header.serialize(&mut buf[0..NodeHeader::SIZE]);
        let mut pos = NodeHeader::SIZE;
        // write keys
        for &key in keys.iter() {
            (&mut buf[pos..pos + 8])
                .write_u64::<LittleEndian>(key)
                .unwrap();
            pos += 8;
        }
        // write children pointers
        for &child in children.iter() {
            (&mut buf[pos..pos + 8])
                .write_u64::<LittleEndian>(child)
                .unwrap();
            pos += 8;
        }
        buf
    }

    /// Deserialize internal node from raw page bytes
    pub fn deserialize(&self, buf: &[u8]) -> Result<(NodeHeader, Vec<u64>, Vec<u64>)> {
        let header = NodeHeader::deserialize(&buf[0..NodeHeader::SIZE])?;
        assert_eq!(header.node_type, NodeType::Internal);
        let mut pos = NodeHeader::SIZE;
        let mut keys = Vec::with_capacity(header.key_count as usize);
        for _ in 0..header.key_count {
            let key = (&buf[pos..pos + 8]).read_u64::<LittleEndian>()?;
            keys.push(key);
            pos += 8;
        }
        let child_count = (header.key_count as usize) + 1;
        let mut children = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            let child = (&buf[pos..pos + 8]).read_u64::<LittleEndian>()?;
            children.push(child);
            pos += 8;
        }
        Ok((header, keys, children))
    }
}

/// Serializer for leaf node pages
pub struct LeafNodeSerializer {
    pub order: usize,
}

impl LeafNodeSerializer {
    /// Serialize leaf node: header + keys + rids + next pointer
    /// Layout: [header][keys...][rids...][next_leaf]
    pub fn serialize(
        &self,
        header: &NodeHeader,
        keys: &[u64],        // length = key_count
        rids: &[(u64, u16)], // length = key_count
        next_leaf: u64,
        page_size: usize,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; page_size];
        header.serialize(&mut buf[0..NodeHeader::SIZE]);
        let mut pos = NodeHeader::SIZE;
        // write keys
        for &key in keys.iter() {
            (&mut buf[pos..pos + 8])
                .write_u64::<LittleEndian>(key)
                .unwrap();
            pos += 8;
        }
        // write rids: page_no (8) + slot_no (2)
        for &(page_no, slot_no) in rids.iter() {
            (&mut buf[pos..pos + 8])
                .write_u64::<LittleEndian>(page_no)
                .unwrap();
            pos += 8;
            (&mut buf[pos..pos + 2])
                .write_u16::<LittleEndian>(slot_no)
                .unwrap();
            pos += 2;
        }
        // write next_leaf pointer
        (&mut buf[pos..pos + 8])
            .write_u64::<LittleEndian>(next_leaf)
            .unwrap();
        buf
    }

    /// Deserialize leaf node from raw page bytes
    pub fn deserialize(&self, buf: &[u8]) -> Result<(NodeHeader, Vec<u64>, Vec<(u64, u16)>, u64)> {
        let header = NodeHeader::deserialize(&buf[0..NodeHeader::SIZE])?;
        assert_eq!(header.node_type, NodeType::Leaf);
        let mut pos = NodeHeader::SIZE;
        let mut keys = Vec::with_capacity(header.key_count as usize);
        for _ in 0..header.key_count {
            let key = (&buf[pos..pos + 8]).read_u64::<LittleEndian>()?;
            keys.push(key);
            pos += 8;
        }
        let mut rids = Vec::with_capacity(header.key_count as usize);
        for _ in 0..header.key_count {
            let page_no = (&buf[pos..pos + 8]).read_u64::<LittleEndian>()?;
            pos += 8;
            let slot_no = (&buf[pos..pos + 2]).read_u16::<LittleEndian>()?;
            pos += 2;
            rids.push((page_no, slot_no));
        }
        let next_leaf = (&buf[pos..pos + 8]).read_u64::<LittleEndian>()?;
        Ok((header, keys, rids, next_leaf))
    }
}
