#!/bin/bash
# Integration test: sender -> receiver over loopback, verify byte-for-byte.
set -u
BIN=${BIN:-target/debug}
PORT=${PORT:-9100}
FEC=${FEC:-}
EXTRA_SENDER=${EXTRA_SENDER:-}
AUTOSTART=${AUTOSTART:-1000}
DIR=/tmp/udpc_test
mkdir -p "$DIR"

run_one() {
    local size=$1
    local label=$2
    local src="$DIR/src_${size}.bin"
    local dst="$DIR/dst_${size}.bin"
    rm -f "$dst"
    if [ ! -f "$src" ]; then
        head -c "$size" /dev/urandom > "$src"
    fi

    "$BIN/udp-receiver" -f "$dst" -P "$PORT" --nokbd --no-progress \
        --start-timeout 30 > "$DIR/rx.log" 2>&1 &
    local rxpid=$!
    sleep 0.3

    local start=$(date +%s.%N)
    timeout 60 "$BIN/udp-sender" -f "$src" -P "$PORT" --nokbd --no-progress $FEC $EXTRA_SENDER \
        --autostart "$AUTOSTART" > "$DIR/tx.log" 2>&1
    local txrc=$?
    local end=$(date +%s.%N)

    # give receiver time to finish writing
    for i in $(seq 1 100); do
        if ! kill -0 $rxpid 2>/dev/null; then break; fi
        sleep 0.1
    done
    kill $rxpid 2>/dev/null
    wait $rxpid 2>/dev/null

    local elapsed=$(echo "$end $start" | awk '{printf "%.2f", $1-$2}')
    if [ $txrc -ne 0 ]; then
        echo "FAIL $label size=$size: sender rc=$txrc"
        tail -3 "$DIR/tx.log"; return 1
    fi
    if cmp -s "$src" "$dst"; then
        echo "PASS $label size=$size time=${elapsed}s"
    else
        echo "FAIL $label size=$size: MISMATCH"
        tail -3 "$DIR/tx.log"; tail -3 "$DIR/rx.log"
        return 1
    fi
}

FAILS=0
for spec in "102400 100KB" "1048576 1MB" "8388608 8MB" "33554432 32MB"; do
    set -- $spec
    run_one "$1" "$2" || FAILS=$((FAILS+1))
done
exit $FAILS
