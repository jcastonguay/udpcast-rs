#!/bin/bash
# One sender + N receivers on a private bridged LAN.
#
# The LAN is built inside unprivileged user+network namespaces: the sender runs
# on a bridge (10.0.0.1/24) and each receiver runs in its own network namespace
# behind a veth (10.0.0.2..N/24). This is what makes it a genuine multi-receiver
# test -- every receiver has its own IP, so the sender sees N distinct
# participants, exactly like on a real LAN.
#
# Usage: ./test_lan.sh [num_receivers] [size] [extra sender args...]
#
# Environment:
#   NRX       number of receivers              (default 3)
#   SIZE      payload size in bytes            (default 1048576)
#   PORT      port base                        (default 9000)
#   RDV       rendezvous address; empty means use the subnet broadcast
#             control channel instead of multicast (default 239.1.1.1)
#   DATA      data multicast address           (default 239.1.1.2)
#   LOSS      percent of datagrams netem drops on every link (default 0)
#   STAGGER   seconds between two receiver launches (default 0.3)
#   PRESTART  seconds between "all receivers up" and the sender start
#   MINWAIT   --min-wait passed to the sender    (default 0)
#   KILL_EARLY  1-based index of a receiver that is SIGKILLed in the middle of
#               the transfer (the remaining ones must still finish cleanly)
#   TC_HOOK   script executed inside the parent netns once the LAN is up
#               ($1 = work dir); use it to reshape the links mid-run
#   BIN       directory with udp-sender / udp-receiver
set -u

BIN=${BIN:-$(cd "$(dirname "$0")" && pwd)/target/debug}
NRX=${NRX:-3}
SIZE=${SIZE:-1048576}
PORT=${PORT:-9000}
RDV=${RDV-239.1.1.1}
DATA=${DATA:-239.1.1.2}
LOSS=${LOSS:-0}
STAGGER=${STAGGER:-0.3}
MINWAIT=${MINWAIT:-0}
W=${W:-/tmp/udpc_lan}

# Positional: [num_receivers] [size] [extra sender args...]
if [ $# -ge 1 ]; then NRX=$1; shift; fi
if [ $# -ge 1 ]; then SIZE=$1; shift; fi
SEND_ARGS="${SEND_ARGS:-$*}"

# Wait for every receiver to register before starting: that is the documented
# way of driving a multi-receiver transfer without a keyboard. Options passed
# by hand (SEND_ARGS / extra args) win over these defaults, so they are
# prepended rather than appended.
AUTO_ARGS=""
case "$SEND_ARGS" in
    *-C*|*--min-receivers*) ;;
    *) AUTO_ARGS="-C $NRX" ;;
esac
case "$SEND_ARGS" in
    *-w*|*--min-wait*) ;;
    *) [ "${MINWAIT:-0}" != 0 ] && AUTO_ARGS="$AUTO_ARGS -w $MINWAIT" ;;
esac
case "$SEND_ARGS" in
    *-R*|*--retries*) ;;
    # A silent participant is dropped only after --retries-until-drop REQACK
    # rounds, and those rounds take >= 1s each from the tenth retry onwards
    # (same ramp as C). Only the dropout scenario needs that to happen well
    # before the receivers' own --receive-timeout, so shorten the budget there
    # and keep the C default (200) everywhere else.
    *) [ -n "${KILL_EARLY:-}" ] && AUTO_ARGS="$AUTO_ARGS -R ${RETRIES:-20}" ;;
esac
SEND_ARGS="$AUTO_ARGS $SEND_ARGS"

# Surviving receivers must outlive the drop detection.
RCV_TIMEOUT="${RCV_TIMEOUT:-$( [ -n "${KILL_EARLY:-}" ] && echo 180 || echo 60 )}"

rm -rf "$W"; mkdir -p "$W"

cat > "$W/child.sh" <<'CHILD'
#!/bin/bash
# $1 = host octet (2..), $W shared dir, plus udpcast args in $2..
i=$1; shift
W=$1; shift
echo $$ > "$W/pid$i"
n=0
while [ ! -f "$W/plug$i" ]; do sleep 0.05; n=$((n+1)); [ $n -gt 400 ] && exit 9; done
exec >> "$W/rx$i.log" 2>&1
ip link set lo up
ip link set eth0 up
ip addr add "10.0.0.$i/24" brd 10.0.0.255 dev eth0
ip route add 224.0.0.0/4 dev eth0
[ -n "$LOSS" ] && [ "$LOSS" != 0 ] && tc qdisc add dev eth0 root netem loss ${LOSS}%
echo "=== receiver #$i ($(hostname)) started $(date +%T) ==="
out="$W/dst$i"
# CAPTURE=1 also records from the receiver's point of view, so that datagrams
# lost on the way (or never sent) can be told apart from lost answers.
if [ "${CAPTURE:-0}" = 1 ]; then
    tcpdump -Z root -i eth0 -U -w "$W/rxdump$i.pcap" \
        'udp port 9000 or udp port 9001' > "$W/tcpdump$i.log" 2>&1 &
    tdpid=$!
fi
"$BIN/udp-receiver" -i eth0 -P "$PORT" ${RDV:+-M "$RDV"} -f "$out" \
    --nokbd --no-progress --start-timeout 60 --receive-timeout "${RCV_TIMEOUT:-60}" &
rxpid=$!
if [ -n "${KILL_EARLY:-}" ] && [ "$((i - 1))" = "$KILL_EARLY" ]; then
    # Die in the middle of the transfer: the sender has to notice that this
    # participant stopped answering and drop it, so that everybody else can
    # finish. Poll until a quarter of the file has arrived.
    thr=$(( ${SIZE:-1048576} / 4 ))
    n=0
    while [ $n -lt 600 ]; do
        sz=$(stat -c%s "$out" 2>/dev/null || echo 0)
        [ "${sz:-0}" -ge "$thr" ] && break
        sleep 0.1
        n=$((n + 1))
    done
    echo "killing myself (pid $rxpid) at ${sz:-?} bytes of $thr"
    kill -9 "$rxpid" 2>/dev/null
fi
wait "$rxpid"
echo "RECEIVER_RC=$?"
sleep 0.5
[ -n "${tdpid:-}" ] && kill "$tdpid" 2>/dev/null
CHILD
chmod +x "$W/child.sh"

cat > "$W/parent.sh" <<'PARENT'
#!/bin/bash
set -u
W=$1; shift
NRX=$1; shift
SIZE=$1; shift
BIN=$1; shift
PORT=$1; shift
RDV=$1; shift
DATA=$1; shift
SEND_ARGS=$1

export BIN W PORT RDV DATA LOSS KILL_EARLY SIZE RCV_TIMEOUT
ip link set lo up
ip addr add 127.0.0.1/8 dev lo 2>/dev/null
ip link add br0 type bridge
# A Linux bridge adopts the lowest MAC address among its ports, and re-computes
# it whenever a port joins or leaves. When a receiver dies its namespace (and
# with it the veth that is a bridge port) disappears; if that veth happened to
# hold the lowest address, br0's own address changes mid-transfer and every ARP
# entry for 10.0.0.1 in the surviving namespaces goes stale. Their unicast
# answers are then unknown-unicast-flooded and never reach the sender's stack,
# which looks exactly like a dead return channel. Pin the bridge address so no
# port can ever take it over.
ip link set br0 address 02:00:00:00:00:01
ip addr add 10.0.0.1/24 brd 10.0.0.255 dev br0
ip link set br0 up
ip route add 224.0.0.0/4 dev br0

head -c "$SIZE" /dev/urandom > "$W/src.bin"
sha256sum "$W/src.bin" | awk '{print $1}' > "$W/src.sha"

for i in $(seq 2 $((NRX + 1))); do
    unshare --net --user --map-root-user bash "$W/child.sh" "$i" "$W" &
    sleep "$STAGGER"
    CP=$(cat "$W/pid$i" 2>/dev/null) || { echo "child $i never started"; exit 8; }
    ip link add "h$i" type veth peer name eth0
    # Deterministic port addresses (see the note on the bridge address above).
    ip link set "h$i" address "$(printf '02:00:00:00:01:%02x' "$i")"
    ip link set eth0 address "$(printf '02:00:00:00:02:%02x' "$i")"
    ip link set eth0 netns "$CP"
    ip link set "h$i" master br0
    ip link set "h$i" up
    [ -n "$LOSS" ] && [ "$LOSS" != 0 ] && tc qdisc add dev "h$i" root netem loss ${LOSS}%
    touch "$W/plug$i"
done

# CAPTURE=1 records the whole exchange on the bridge (needs CAP_NET_RAW, which
# the parent namespace has) for offline analysis of a failing run.
if [ "${CAPTURE:-0}" = 1 ]; then
    tcpdump -Z root -i br0 -U -w "$W/dump.pcap" 'udp port 9000 or udp port 9001' \
        > "$W/tcpdump.log" 2>&1 &
    TCPID=$!
    export TCPID
fi

# Give receivers a moment to join the multicast group.
# An optional hook script runs in this netns in the background; it can for
# example flip the netem loss to 100% for a few seconds to force the loss of
# the one-shot CONNECT_REPLYs (late-join test).
if [ -n "${TC_HOOK:-}" ]; then
    ( timeout 320 bash "$TC_HOOK" "$W" || true ) &
fi
sleep "${PRESTART:-1.5}"
echo "=== sender started $(date +%T) (args: $SEND_ARGS) ==="
timeout 300 "$BIN/udp-sender" -f "$W/src.bin" -i br0 -P "$PORT" \
    ${RDV:+-M "$RDV"} -m "$DATA" --nokbd --no-progress $SEND_ARGS > "$W/tx.log" 2>&1
echo "SENDER_RC=$?" > "$W/tx.rc"
sleep 0.5
[ -n "${TCPID:-}" ] && kill "$TCPID" 2>/dev/null
wait
PARENT

export LOSS STAGGER KILL_EARLY TC_HOOK
timeout 600 unshare --net --user --map-root-user --keep-caps \
    bash "$W/parent.sh" "$W" "$NRX" "$SIZE" "$BIN" "$PORT" "$RDV" "$DATA" "$SEND_ARGS"

echo "=================== RESULTS ==================="
echo "--- sender ---"
tail -6 "$W/tx.log" 2>/dev/null
echo "sender rc: $(cat "$W/tx.rc" 2>/dev/null)"
if [ -n "$RDV" ]; then CTRL="mcast rdv $RDV"; else CTRL="broadcast"; fi
echo "control channel: $CTRL, loss: ${LOSS}%, receivers: $NRX"
SRCSHA=$(cat "$W/src.sha")
FAIL=0
DROPPED_SEEN=0
grep -q "Dropping client" "$W/tx.log" 2>/dev/null && DROPPED_SEEN=1
for i in $(seq 2 $((NRX + 1))); do
    n=$((i - 1))
    if [ -n "${KILL_EARLY:-}" ] && [ "$n" = "$KILL_EARLY" ]; then
        echo "receiver #$n: killed on purpose ($(stat -c%s "$W/dst$i" 2>/dev/null || echo 0) bytes before death)"
        continue
    fi
    if [ ! -f "$W/dst$i" ]; then
        echo "receiver #$n: NO OUTPUT FILE"
        tail -4 "$W/rx$i.log" 2>/dev/null
        FAIL=$((FAIL + 1)); continue
    fi
    got=$(sha256sum "$W/dst$i" | awk '{print $1}')
    if [ "$got" = "$SRCSHA" ]; then
        echo "receiver #$n: OK ($(stat -c%s "$W/dst$i") bytes)"
    else
        echo "receiver #$n: *** CORRUPTION *** want=${SRCSHA:0:16}.. got=${got:0:16}.."
        echo "   size want=$SIZE got=$(stat -c%s "$W/dst$i")"
        tail -4 "$W/rx$i.log" 2>/dev/null
        FAIL=$((FAIL + 1))
    fi
done
if [ -n "${KILL_EARLY:-}" ]; then
    if [ "$DROPPED_SEEN" = 1 ]; then
        echo "sender dropped the dead participant as expected"
    else
        echo "*** sender never reported dropping the killed receiver ***"
        FAIL=$((FAIL + 1))
    fi
fi
echo "==============================================="
[ $FAIL -eq 0 ] && echo "LAN TEST PASSED ($NRX receivers)" || echo "LAN TEST FAILED ($FAIL/$NRX)"
exit $FAIL
