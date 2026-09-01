use std::net::{Ipv4Addr, SocketAddrV4};

pub const MAX_CLIENTS: usize = 1024;

#[derive(Clone, Debug)]
struct ClientDesc {
    addr: SocketAddrV4,
    used: bool,
    capabilities: u32,
    rcvbuf: u32,
    /// Set once the participant has sent any OK/RETRANSMIT after the
    /// transfer started. Used for late-join: the sender keeps re-sending
    /// the CONNECT_REPLY to registered participants that never answered
    /// (their one-shot reply was lost), so they can catch up.
    ever_answered: bool,
    /// The (slice_no, rxmit_id) of the participant's first OK/RETRANSMIT on
    /// that slice, if any. The drop budget (--retries-until-drop) is
    /// measured from that moment rather than from the slice start, so a late
    /// joiner's missed-start rounds do not eat its budget. The slice number
    /// makes the stamp per-slice: stamps from an earlier slice are ignored.
    first_answered_round: Option<(i32, i32)>,
}

pub struct ParticipantsDb {
    nr_participants: usize,
    client_table: Vec<ClientDesc>,
}

impl ParticipantsDb {
    pub fn new() -> Self {
        Self {
            nr_participants: 0,
            client_table: (0..MAX_CLIENTS)
                .map(|_| ClientDesc {
                    addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                    used: false,
                    capabilities: 0,
                    rcvbuf: 0,
                    ever_answered: false,
                    first_answered_round: None,
                })
                .collect(),
        }
    }

    pub fn is_participant_valid(&self, i: usize) -> bool {
        self.client_table.get(i).map_or(false, |c| c.used)
    }

    pub fn remove_participant(&mut self, i: usize) -> i32 {
        if let Some(client) = self.client_table.get_mut(i) {
            if client.used {
                eprintln!("Disconnecting #{} ({})", i, client.addr);
                client.used = false;
                self.nr_participants -= 1;
            }
        }
        0
    }

    pub fn lookup_participant(&self, addr: &SocketAddrV4) -> i32 {
        for (i, client) in self.client_table.iter().enumerate() {
            if client.used && client.addr == *addr {
                return i as i32;
            }
        }
        -1
    }

    pub fn nr_participants(&self) -> usize {
        self.nr_participants
    }

    pub fn add_participant(
        &mut self,
        addr: &SocketAddrV4,
        capabilities: u32,
        rcvbuf: u32,
        pointopoint: bool,
    ) -> i32 {
        let existing = self.lookup_participant(addr);
        if existing >= 0 {
            return existing;
        }

        for (i, client) in self.client_table.iter_mut().enumerate() {
            if !client.used {
                client.addr = *addr;
                client.used = true;
                client.capabilities = capabilities;
                client.rcvbuf = rcvbuf;
                self.nr_participants += 1;
                eprintln!(
                    "New connection from {}  (# {}) {:08x}",
                    addr, i, capabilities
                );
                return i as i32;
            } else if pointopoint {
                return -1;
            }
        }
        -1
    }

    pub fn get_participant_capabilities(&self, i: usize) -> u32 {
        self.client_table.get(i).map_or(0, |c| c.capabilities)
    }

    /// Records that participant `i` answered slice `slice_no` (if any) in
    /// round `round`. Stamps `first_answered_round` on the first answer for
    /// each slice.
    pub fn mark_answered(&mut self, i: usize, slice_no: Option<i32>, round: i32) {
        if let Some(client) = self.client_table.get_mut(i) {
            if !client.ever_answered {
                client.ever_answered = true;
            }
            if let Some(sn) = slice_no {
                if client.first_answered_round.map_or(true, |(s, _)| s != sn) {
                    if crate::util::dbg_on() {
                        crate::util::flprintf(&format!(
                            "DBG {:.3} sender: cl={} first answer for slice {} at round {}\n",
                            crate::util::dbg_stamp(),
                            i,
                            sn,
                            round
                        ));
                    }
                    client.first_answered_round = Some((sn, round));
                }
            }
        }
    }

    pub fn ever_answered(&self, i: usize) -> bool {
        self.client_table.get(i).map_or(false, |c| c.ever_answered)
    }

    pub fn first_answered_round(&self, i: usize) -> Option<(i32, i32)> {
        self.client_table
            .get(i)
            .map(|c| c.first_answered_round)
            .flatten()
    }

    pub fn get_participant_rcvbuf(&self, i: usize) -> u32 {
        self.client_table.get(i).map_or(0, |c| c.rcvbuf)
    }

    pub fn get_participant_ip(&self, i: usize) -> Option<&SocketAddrV4> {
        self.client_table.get(i).map(|c| &c.addr)
    }

    pub fn print_not_set(&self, d: &[u8]) {
        let mut first = true;
        eprint!("[");
        for (i, client) in self.client_table.iter().enumerate() {
            if client.used && !bit_is_set(i, d) {
                if !first {
                    eprint!(",");
                }
                first = false;
                eprint!("{}", i);
            }
        }
        eprint!("]");
    }

    pub fn print_set(&self, d: &[u8]) {
        let mut first = true;
        eprint!("[");
        for (i, client) in self.client_table.iter().enumerate() {
            if client.used && bit_is_set(i, d) {
                if !first {
                    eprint!(",");
                }
                first = false;
                eprint!("{}", i);
            }
        }
        eprint!("]");
    }
}

pub fn bit_is_set(bit: usize, map: &[u8]) -> bool {
    let byte_idx = bit / 8;
    let bit_idx = bit % 8;
    if byte_idx >= map.len() {
        return false;
    }
    (map[byte_idx] & (1 << bit_idx)) != 0
}

pub fn set_bit(bit: usize, map: &mut [u8]) {
    let byte_idx = bit / 8;
    let bit_idx = bit % 8;
    if byte_idx < map.len() {
        map[byte_idx] |= 1 << bit_idx;
    }
}

pub fn clear_bit(bit: usize, map: &mut [u8]) {
    let byte_idx = bit / 8;
    let bit_idx = bit % 8;
    if byte_idx < map.len() {
        map[byte_idx] &= !(1 << bit_idx);
    }
}
