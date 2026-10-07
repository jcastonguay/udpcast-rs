# udpcast-rust

A vibe coded Rust re-implementation of [udpcast](http://udpcast.linux.lu/),
the single-sender / many-receiver UDP multicasting tool for file transfer
and live streaming.

The C original (2012 snapshot) was used strictly as a protocol reference.
The wire format, message flow and semantics are compatible with it, so a
Rust sender can talk to a C receiver and vice versa. Extensions that change
behavior are capability-gated and never affect C peers.

## Features

- **One sender, any number of receivers**, over unicast, subnet broadcast or
  multicast control channels; data over unicast (point-to-point) or multicast.
- **Reliable delivery**: per-slice retransmission protocol (REQACK / RETR /
  OK), retransmission budget (`--retries-until-drop`), participant drop
  detection for dead or killed receivers.
- **Adaptive slices**: slice size grows as the transfer stabilizes
  (`--min-slice-size` … `--max-slice-size`), like the original.
- **Rate control**: `--max-bitrate` and `--auto-bitrate` governors
  (C `rate` equivalent).
- **Forward error correction** (`-F [stripes]x[redundancy]/[stripesize]`):
  the sender appends Reed-Solomon parity blocks to every slice, so a
  receiver can reconstruct up to `redundancy` lost blocks per stripe without
  a retransmission round. This port also advertises `CAP_FEC` in the
  sender's `HELLO`/`CONNECT_REPLY` while `-F` is in use — the 2012 C sender
  defines the bit but never raises it (its receivers turn FEC on by the
  arrival of `CMD_FEC` packets), so the extra bit is ignored by old peers
  and is safe. `-L` prints the exact license text of the FEC code (GPL for
  udpcast plus the BSD license of the Reed-Solomon code) and exits, byte-for
  byte like C's `-L`.
- **Late-join self-healing** (Rust extension, `CAP_LATE_JOIN = 0x0040`):
  if a receiver misses its one-shot `CONNECT_REPLY` entirely, it sniffs the
  data channel during the start phase, learns which slices were already sent,
  and re-requests them once its (retried) reply reaches the sender — the
  sender keeps un-acked slices in its ring and re-sends them, so the receiver
  still obtains a complete file instead of the sender stalling for its full
  retransmit budget.
- **Robustness details** (all safe for C peers):
  - the sender retransmits `CONNECT_REPLY` while the first slice is not yet
    acked by every participant,
  - a receiver that completes a slice sends a proactive `OK` (the sender's
    `OK` handling is idempotent),
  - the receiver answers `OK` to a `REQACK` for an already-delivered slice
    instead of re-requesting it (C semantics),
  - each missing block is retransmitted twice per round,
  - stale retransmission requests are still applied to a pending slice
    (they can only add blocks, never cause loss),
  - a participant's drop budget starts at the round of its first answer for
    the slice in question, so a late joiner is not dropped for rounds it
    missed before joining,
  - on give-up the receiver retransmits `DISCONNECT` so the sender does not
    stall on a truly dead participant,
  - the receiver never reports an incomplete file as complete: the
    zero-byte end marker only terminates the transfer when every earlier
    slice was delivered.

## Building

```sh
cargo build --release
# binaries:
#   target/release/udp-sender
#   target/release/udp-receiver
```

Dependencies are intentionally small: `clap`, `nix`, `socket2`, `libc`.

## Usage

Receiver (in a terminal of its own):

```sh
udp-receiver -i eth0 -P 9000 -f received.bin
```

Sender:

```sh
udp-sender -i eth0 -P 9000 -f file.bin
```

Useful options (see `--help`):

```
-m   data multicast address        (default: derived from interface)
-M   rendezvous (control) multicast address
-B   subnet-broadcast control channel (instead of -M)
-1   force point-to-point mode
-r   max bitrate, e.g. -r 50m
-A   autorate (adaptive rate control)
-b   block size (default 1456)
-F   FEC: [stripes]x[redundancy]/[stripesize], e.g. -F 8x2/128
-L   print the FEC code license (like C's `-L`) and exit
--min-slice-size / --default-slice-size / --max-slice-size
-R   --retries-until-drop         (default 200)
-T/-s --start-timeout
--receive-timeout (receiver)
-l   log file                     -k   --nokbd, --no-progress (headless use)
-p   pipe to command instead of -f
```

Example: one sender, three receivers on 10.0.0.0/24:

```sh
# receiver side (run N times, one per machine)
udp-receiver -i eth0 -f file.bin
# sender side
udp-sender -i eth0 -f file.bin
```

## Running as a service

Unlike the C original, `udp-sender` does not daemonize itself: `-D` / `--daemon-mode`
(fork + setsid + pid file + `-K/--kill`) was deliberately not ported. Run it under
a process supervisor instead — systemd works out of the box:

```ini
# /etc/systemd/system/udp-sender.service
[Unit]
Description=udpcast file sender
After=network-online.target
Wants=network-online.target

[Service]
# --no-progress + -k: what -DD used to force automatically in daemon mode
ExecStart=/usr/local/bin/udp-sender -i eth0 -P 9000 -f /srv/transfer/file.bin -k --no-progress
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Notes:

- Each invocation sends the file once and exits; `Restart=on-failure` (or
  `always`) reproduces the C `-D` "keep serving transfers until killed" behavior —
  a receiver that joins later starts the next run.
- `systemctl stop` sends SIGTERM; there is no pid file to clean up, systemd
  tracks the process itself.
- For a live stream (`-Z/--streaming`) use `Restart=always` so the service
  comes back up after every stop.

## Testing

The test suite uses unprivileged user + network namespaces to build a real
private LAN (one bridge, one veth per receiver) with `tc netem` loss
injection — no root needed beyond the privileges the tests declare.

```sh
# 3 receivers, 1 MB, 30% packet loss on every link
LOSS=30 ./test_lan.sh 3 1048576

# kill receiver #2 mid-transfer, the sender must drop it and the
# remaining receivers must complete
LOSS=30 KILL_EARLY=2 ./test_lan.sh 3 20971520

# blackhole the data channel for 6 s during start: forces a receiver to
# miss its CONNECT_REPLY and exercise the late-join path
LOSS=30 TC_HOOK=/path/to/blackhole.sh ./test_lan.sh 3 20971520

CAPTURE=1   # keep per-host pcap captures
UDPC_DEBUG=1  # verbose DBG logging
```

`test_matrix.sh` sweeps scenarios in bulk; `test_integration.sh` covers basic
single-pair transfers.

## Layout

```
src/
  bin/sender_main.rs     CLI entry, sender
  bin/receiver_main.rs   CLI entry, receiver
  protocol.rs            packet layout + capability bits
  negotiate.rs           pre-start negotiation (connect/hello/reply)
  senddata.rs            sender data-phase state machine
  receivedata.rs         receiver data-phase state machine
  fifo.rs / produconsum.rs  thread-safe producer/consumer ring
  participants.rs        participant table (C db equivalent)
  rate.rs                max/auto bitrate governors
  fec.rs                 Reed-Solomon (advertised via CAP_FEC while -F is in use)
  socklib.rs             sockets, mcast, interface handling
  statistics.rs / console.rs / diskio.rs / process.rs / util.rs
tests/multi_receiver.rs  in-process protocol round-trip tests
```

## License

GPL-2.0 (see `LICENSE`).

The wire protocol is that of udpcast, © 2001–2012 Alain Knaff
(http://udpcast.linux.lu/), GPL-2. The forward-error-correction module
descends from the Rizzo/Karn/Morelos-Zaragoza work (BSD); it is fully
functional here (enable it with `-F`) and the sender advertises it with
`CAP_FEC` — a deliberate extension, since the 2012 C sender never raised
that bit. `udp-sender -L` prints the exact combined license text the C
binaries print and exits.
