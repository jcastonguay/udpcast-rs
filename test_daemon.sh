#!/bin/bash
# -D / --pid-file behaviour: a daemonised sender keeps serving one transfer
# after another, and SIGTERM takes it down without leaving the pid file behind.
set -u
cd "$(dirname "$0")"

BIN=${BIN:-$(pwd)/target/debug}
PORT=${PORT:-9300}
RDV=239.9.9.1
DATA=239.9.9.2
W=$(mktemp -d /tmp/udpc_daemon.XXXX)
PIDFILE=$W/sender.pid

head -c 1048576 /dev/urandom > "$W/src.bin"
sha=$(sha256sum "$W/src.bin" | awk '{print $1}')

"$BIN/udp-sender" -f "$W/src.bin" -i lo -P "$PORT" -M "$RDV" -m "$DATA" \
    -DD --pid-file "$PIDFILE" -C 1 --no-progress > "$W/tx.log" 2>&1 &
launcher=$!

for _ in $(seq 50); do [ -f "$PIDFILE" ] && break; sleep 0.1; done
if [ ! -f "$PIDFILE" ]; then
    echo "FAIL: no pid file written"; cat "$W/tx.log"; kill $launcher 2>/dev/null; exit 1
fi
daemon_pid=$(cat "$PIDFILE")
# The launcher exits immediately; the detached process must be alive.
wait "$launcher" 2>/dev/null
rc_launcher=$?
if ! kill -0 "$daemon_pid" 2>/dev/null; then
    echo "FAIL: daemonised sender (pid $daemon_pid) is not running"; cat "$W/tx.log"; exit 1
fi

run_receiver() { # $1 = suffix
    timeout 60 "$BIN/udp-receiver" -i lo -P "$PORT" -M "$RDV" -f "$W/dst$1" \
        --nokbd --no-progress --start-timeout 30 > "$W/rx$1.log" 2>&1
    rc=$?
    got=$(sha256sum "$W/dst$1" 2>/dev/null | awk '{print $1}')
    if [ $rc -ne 0 ] || [ "$got" != "$sha" ]; then
        echo "FAIL: transfer #$1 rc=$rc got=${got:0:16}.. want=${sha:0:16}.."
        tail -3 "$W/rx$1.log"
        kill "$daemon_pid" 2>/dev/null
        exit 1
    fi
    echo "transfer #$1 OK"
}

run_receiver 1
if ! kill -0 "$daemon_pid" 2>/dev/null; then
    echo "FAIL: sender did not stay around for the next transfer (-D)"; exit 1
fi
run_receiver 2

kill -TERM "$daemon_pid"
sleep 0.5
if [ -f "$PIDFILE" ]; then
    echo "FAIL: SIGTERM left $PIDFILE behind"; exit 1
fi
if kill -0 "$daemon_pid" 2>/dev/null; then
    echo "FAIL: SIGTERM did not stop the sender"; kill -9 "$daemon_pid"; exit 1
fi
echo "pid file removed on SIGTERM, sender stopped"
echo "DAEMON TEST PASSED (rc_launcher=$rc_launcher)"
rm -rf "$W"
