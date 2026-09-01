//! Wire protocol for udpcast.
//!
//! This is a faithful port of `udpc-protoc.h`: all integers are
//! big-endian (network byte order) on the wire, and message sizes and
//! field layouts match the C structs byte-for-byte (C padding bytes are
//! simply not sent by this implementation, which old/new C peers parse
//! fine because they only read the declared fields).

use std::net::Ipv4Addr;

// ---------------------------------------------------------------------------
// Opcodes
// ---------------------------------------------------------------------------

/// Receiver to sender
pub const CMD_OK: u16 = 0;
pub const CMD_RETRANSMIT: u16 = 1;
pub const CMD_GO: u16 = 2;
pub const CMD_CONNECT_REQ: u16 = 3;
pub const CMD_DISCONNECT: u16 = 4;
pub const CMD_UNUSED: u16 = 5;

/// Sender to receiver
pub const CMD_REQACK: u16 = 6;
pub const CMD_CONNECT_REPLY: u16 = 7;
pub const CMD_DATA: u16 = 8;
pub const CMD_FEC: u16 = 9;
pub const CMD_HELLO_NEW: u16 = 10;
pub const CMD_HELLO_STREAMING: u16 = 11;

/// Obsolete opcode value of the old (2005-era) hello.
pub const CMD_HELLO: u16 = 0x0500;

/// A hello is recognized by either of these opcodes.
pub fn is_hello(opcode: u16) -> bool {
    opcode == CMD_HELLO_NEW || opcode == CMD_HELLO || opcode == CMD_HELLO_STREAMING
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

pub const CAP_NEW_GEN: u32 = 0x0001;
/// Forward error correction. C 2012 defines this bit (`udpc-protoc.h`,
/// under BB_FEATURE_UDPCAST_FEC) but never raises it: the C receiver turns
/// FEC on by the arrival of CMD_FEC packets and only tests CAP_NEW_GEN /
/// CAP_ASYNC from the advertised word. This port sets the bit in the
/// sender's HELLO / CONNECT_REPLY while `-F` is in use (`senddata::
/// sender_capabilities`); the bit is therefore informational for old
/// peers, which ignore it, so advertising it is safe for C receivers.
pub const CAP_FEC: u32 = 0x0004;
pub const CAP_BIG_ENDIAN: u32 = 0x0008;
pub const CAP_LITTLE_ENDIAN: u32 = 0x0010;
pub const CAP_ASYNC: u32 = 0x0020;
/// Extension (not in the C reference): receiver asks the sender to repeat
/// its CONNECT_REPLY, so a receiver whose one-shot reply was lost can join a
/// transfer in progress and re-request the slices it missed (see the
/// sender's late-join re-reply in senddata.rs and the sniffer in
/// receiver.rs). Old senders simply ignore this bit.
pub const CAP_LATE_JOIN: u32 = 0x0040;

/// What the sender tells receivers it supports.
pub const SENDER_CAPABILITIES: u32 = CAP_NEW_GEN | CAP_BIG_ENDIAN;
/// What a receiver declares in CONNECT_REQ.
pub const RECEIVER_CAPABILITIES: u32 = CAP_NEW_GEN | CAP_BIG_ENDIAN | CAP_LATE_JOIN;

pub const MAX_BLOCK_SIZE: u16 = 1456;
pub const MAX_SLICE_SIZE: u16 = 1024;
pub const MAX_CLIENTS: usize = 1024;

/// Receiver port = base, sender port = base + 1.
pub const fn receiver_port(base: u16) -> u16 {
    base
}
pub const fn sender_port(base: u16) -> u16 {
    base + 1
}

// ---------------------------------------------------------------------------
// Message payloads
//
// Every message starts with a 4-byte `msgHeader` { u16 opCode; i16 reserved }.
// The structs below are the *payload after the header* (or the full message
// where noted), in exactly the C field order.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct OkMsg {
    pub slice_no: i32,
}
/// `sizeof(struct ok)` including header = 8.
pub const OK_MSG_SIZE: usize = 8;
impl OkMsg {
    pub fn pack(self) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[0..2].copy_from_slice(&CMD_OK.to_be_bytes());
        b[2..4].copy_from_slice(&0i16.to_be_bytes());
        b[4..8].copy_from_slice(&self.slice_no.to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        Self {
            slice_no: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
        }
    }
}

/// retransmit: { u16 op; i16 res; i32 sliceNo; i32 rxmit; u8 map[128] }
#[derive(Clone, Copy, Debug)]
pub struct Retransmit {
    pub slice_no: i32,
    pub rxmit: i32,
    /// Missing-block bitmap, MAX_SLICE_SIZE/8 = 128 bytes.
    pub map: [u8; 128],
}
/// `sizeof(struct retransmit)` = 140.
pub const RETRANSMIT_SIZE: usize = 4 + 4 + 4 + 128;
impl Retransmit {
    pub fn pack(self) -> [u8; 140] {
        let mut b = [0u8; 140];
        b[0..2].copy_from_slice(&CMD_RETRANSMIT.to_be_bytes());
        b[4..8].copy_from_slice(&self.slice_no.to_be_bytes());
        b[8..12].copy_from_slice(&self.rxmit.to_be_bytes());
        b[12..140].copy_from_slice(&self.map);
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        let mut m = [0u8; 128];
        m.copy_from_slice(&buf[12..140]);
        Self {
            slice_no: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
            rxmit: i32::from_be_bytes(buf[8..12].try_into().unwrap()),
            map: m,
        }
    }
}

/// connectReq: { u16 op; i16 res; i32 capabilities; u32 rcvbuf }
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConnectReq {
    pub capabilities: u32,
    pub rcvbuf: u32,
}
/// `sizeof(struct connectReq)` = 12.
pub const CONNECT_REQ_SIZE: usize = 12;
impl ConnectReq {
    pub fn pack(self) -> [u8; 12] {
        let mut b = [0u8; 12];
        b[0..2].copy_from_slice(&CMD_CONNECT_REQ.to_be_bytes());
        b[4..8].copy_from_slice(&self.capabilities.to_be_bytes());
        b[8..12].copy_from_slice(&self.rcvbuf.to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        Self {
            capabilities: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            rcvbuf: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
        }
    }
}

/// connectReply: { u16 op; i16 res; i32 clNr; i32 blockSize; i32 capabilities; u8 mcast[16] }
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConnectReply {
    pub cl_nr: i32,
    pub block_size: i32,
    pub capabilities: u32,
    pub mcast: [u8; 16],
}
/// `sizeof(struct connectReply)` = 32.
pub const CONNECT_REPLY_SIZE: usize = 32;
impl ConnectReply {
    pub fn pack(self) -> [u8; 32] {
        let mut b = [0u8; 32];
        b[0..2].copy_from_slice(&CMD_CONNECT_REPLY.to_be_bytes());
        b[4..8].copy_from_slice(&self.cl_nr.to_be_bytes());
        b[8..12].copy_from_slice(&self.block_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.capabilities.to_be_bytes());
        b[16..].copy_from_slice(&self.mcast);
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        let mut mcast = [0u8; 16];
        mcast.copy_from_slice(&buf[16..32]);
        Self {
            cl_nr: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
            block_size: i32::from_be_bytes(buf[8..12].try_into().unwrap()),
            capabilities: u32::from_be_bytes(buf[12..16].try_into().unwrap()),
            mcast,
        }
    }
}

/// hello: { u16 op; i16 res; i32 capabilities; u8 mcastAddr[16]; i16 blockSize }
#[derive(Clone, Copy, Debug)]
pub struct Hello {
    pub capabilities: u32,
    pub mcast: [u8; 16],
    pub block_size: i32,
}
/// Wire size = 28, matching C struct hello (4-byte header, 4-byte
/// capabilities, 16-byte mcastAddr, 2-byte blockSize, 2-byte padding).
pub const HELLO_SIZE: usize = 28;
impl Hello {
    pub fn pack(self, opcode: u16) -> [u8; 28] {
        let mut b = [0u8; 28];
        b[0..2].copy_from_slice(&opcode.to_be_bytes());
        b[4..8].copy_from_slice(&self.capabilities.to_be_bytes());
        b[8..24].copy_from_slice(&self.mcast);
        b[24..26].copy_from_slice(&(self.block_size as i16).to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        let mut mcast = [0u8; 16];
        mcast.copy_from_slice(&buf[8..24]);
        Self {
            capabilities: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            mcast,
            block_size: i16::from_be_bytes(buf[24..26].try_into().unwrap()) as i32,
        }
    }
}

/// go: { u16 op; i16 res }
pub fn pack_go() -> [u8; 4] {
    let mut b = [0u8; 4];
    b[0..2].copy_from_slice(&CMD_GO.to_be_bytes());
    b
}

/// disconnect: { u16 op; i16 res }
pub fn pack_disconnect() -> [u8; 4] {
    let mut b = [0u8; 4];
    b[0..2].copy_from_slice(&CMD_DISCONNECT.to_be_bytes());
    b
}

// ---------------------------------------------------------------------------
// Data-plane messages (sender -> receiver), all 16 bytes
// ---------------------------------------------------------------------------

/// dataBlock: { u16 op; i16 res; i32 sliceNo; u16 blockNo; u16 res2; i32 bytes }
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DataBlock {
    pub slice_no: i32,
    pub block_no: u16,
    pub bytes: i32,
}
pub const DATA_BLOCK_SIZE: usize = 16;
impl DataBlock {
    pub fn pack(self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..2].copy_from_slice(&CMD_DATA.to_be_bytes());
        b[4..8].copy_from_slice(&self.slice_no.to_be_bytes());
        b[8..10].copy_from_slice(&self.block_no.to_be_bytes());
        b[12..16].copy_from_slice(&self.bytes.to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        Self {
            slice_no: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
            block_no: u16::from_be_bytes(buf[8..10].try_into().unwrap()),
            bytes: i32::from_be_bytes(buf[12..16].try_into().unwrap()),
        }
    }
}

/// fecBlock: { u16 op; i16 stripes; i32 sliceNo; u16 blockNo; u16 res2; i32 bytes }
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FecBlock {
    pub stripes: i32,
    pub slice_no: i32,
    pub block_no: u16,
    pub bytes: i32,
}
pub const FEC_BLOCK_SIZE: usize = 16;
impl FecBlock {
    pub fn pack(self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..2].copy_from_slice(&CMD_FEC.to_be_bytes());
        b[2..4].copy_from_slice(&(self.stripes as i16).to_be_bytes());
        b[4..8].copy_from_slice(&self.slice_no.to_be_bytes());
        b[8..10].copy_from_slice(&self.block_no.to_be_bytes());
        b[12..16].copy_from_slice(&self.bytes.to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        Self {
            stripes: i16::from_be_bytes(buf[2..4].try_into().unwrap()) as i32,
            slice_no: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
            block_no: u16::from_be_bytes(buf[8..10].try_into().unwrap()),
            bytes: i32::from_be_bytes(buf[12..16].try_into().unwrap()),
        }
    }
}

/// reqack: { u16 op; i16 res; i32 sliceNo; i32 bytes; i32 rxmit }
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Reqack {
    pub slice_no: i32,
    pub bytes: i32,
    pub rxmit: i32,
}
pub const REQACK_SIZE: usize = 16;
impl Reqack {
    pub fn pack(self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..2].copy_from_slice(&CMD_REQACK.to_be_bytes());
        b[4..8].copy_from_slice(&self.slice_no.to_be_bytes());
        b[8..12].copy_from_slice(&self.bytes.to_be_bytes());
        b[12..16].copy_from_slice(&self.rxmit.to_be_bytes());
        b
    }
    pub fn unpack(buf: &[u8]) -> Self {
        Self {
            slice_no: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
            bytes: i32::from_be_bytes(buf[8..12].try_into().unwrap()),
            rxmit: i32::from_be_bytes(buf[12..16].try_into().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: embed an IPv4 address in a 16-byte mcast field
// ---------------------------------------------------------------------------

/// Store a v4 address in the 16-byte mcastAddr field the way the C code does:
/// `copyToMessage` memcpy's `sin_addr` (4 bytes) to the start of the field.
pub fn ip4_into_16(addr: Ipv4Addr) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&addr.octets());
    b
}

pub fn ip4_from_16(b: &[u8; 16]) -> Ipv4Addr {
    Ipv4Addr::new(b[0], b[1], b[2], b[3])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_layout_matches_c_struct() {
        // C struct hello on the wire:
        //  hdr(4) | capabilities=9 | mcastAddr[16] | blockSize=1456 | pad(2)
        let h = Hello {
            capabilities: 9,
            mcast: ip4_into_16(Ipv4Addr::new(224, 0, 0, 1)),
            block_size: 1456,
        };
        let packed = h.pack(CMD_HELLO_NEW);
        assert_eq!(packed.len(), 28);
        // offset 0: op=10 -> 00 0a
        assert_eq!(&packed[0..4], &[0, 10, 0, 0]);
        // offset 4: capabilities = 9 -> 00 00 00 09
        assert_eq!(&packed[4..8], &[0, 0, 0, 9]);
        // mcast field: sin_addr copied to the start of mcastAddr
        assert_eq!(&packed[8..12], &[224, 0, 0, 1]);
        // blockSize 1456 -> 05 b0 at offset 24
        assert_eq!(&packed[24..26], &[5, 176]);
        // round trip
        let back = Hello::unpack(&packed);
        assert_eq!(back.capabilities, 9);
        assert_eq!(back.block_size, 1456);
        assert_eq!(ip4_from_16(&back.mcast), Ipv4Addr::new(224, 0, 0, 1));
    }

    #[test]
    fn retransmit_size_is_140() {
        let m = Retransmit {
            slice_no: 3,
            rxmit: 7,
            map: [0xab; 128],
        };
        let packed = m.pack();
        assert_eq!(packed.len(), 140);
        assert_eq!(&packed[0..2], &1u16.to_be_bytes());
        assert_eq!(&packed[4..8], &3i32.to_be_bytes());
        assert_eq!(&packed[8..12], &7i32.to_be_bytes());
        let back = Retransmit::unpack(&packed);
        assert_eq!(back.slice_no, 3);
        assert_eq!(back.rxmit, 7);
        assert_eq!(back.map, [0xab; 128]);
    }

    #[test]
    fn data_block_roundtrip() {
        let d = DataBlock {
            slice_no: 42,
            block_no: 7,
            bytes: 1456,
        };
        let b = d.pack();
        assert_eq!(b.len(), 16);
        assert_eq!(b[1], CMD_DATA as u8);
        let back = DataBlock::unpack(&b);
        assert_eq!(back, d);
    }

    #[test]
    fn connect_req_reply_roundtrip() {
        let q = ConnectReq {
            capabilities: RECEIVER_CAPABILITIES,
            rcvbuf: 131072,
        };
        let qb = q.pack();
        assert_eq!(qb.len(), 12);
        assert_eq!(ConnectReq::unpack(&qb), q);

        let r = ConnectReply {
            cl_nr: 3,
            block_size: 1456,
            capabilities: SENDER_CAPABILITIES,
            mcast: ip4_into_16(Ipv4Addr::new(232, 1, 2, 3)),
        };
        let rb = r.pack();
        assert_eq!(rb.len(), 32);
        assert_eq!(ConnectReply::unpack(&rb), r);
    }

    #[test]
    fn reqack_roundtrip() {
        let r = Reqack {
            slice_no: 11,
            bytes: 5600,
            rxmit: 5,
        };
        let b = r.pack();
        assert_eq!(b.len(), 16);
        assert_eq!(Reqack::unpack(&b), r);
    }

    #[test]
    fn fec_block_roundtrip() {
        let f = FecBlock {
            stripes: 10,
            slice_no: 2,
            block_no: 13,
            bytes: 999,
        };
        let b = f.pack();
        assert_eq!(b.len(), 16);
        assert_eq!(FecBlock::unpack(&b), f);
    }
}
