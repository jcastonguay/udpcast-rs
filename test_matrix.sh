#!/bin/bash
# Multi-receiver regression matrix for the Rust port.
#
# Runs test_lan.sh once per scenario. Every scenario is a real one-sender /
# N-receiver transfer on a private bridge inside user+net namespaces, and each
# receiver's file must come out with the same sha256 as the source.
#
#   ./test_matrix.sh              # full matrix (~5 min)
#   ./test_matrix.sh quick        # short version: no big files, no loss runs
#   BIN=/path/to/bins ./test_matrix.sh   # A/B a different build
set -u

cd "$(dirname "$0")"

MODE=${1:-full}

# name | receivers | size | loss% | rdv ("" = broadcast control) | extra sender args
#       | stagger seconds between receiver launches
case "$MODE" in
quick)
	SCENARIOS="
base-1rx|1|262144|0|239.1.1.1||
base-3rx|3|1048576|0|239.1.1.1||
broadcast-ctrl|3|262144|0|||
nopointopoint|1|262144|0|239.1.1.1|-2|
staggered|3|1048576|0|239.1.1.1|-w 2|1.5
"
	;;
*)
	SCENARIOS="
base-1rx|1|262144|0|239.1.1.1||
base-3rx|3|1048576|0|239.1.1.1||
base-5rx|5|4194304|0|239.1.1.1||
base-8rx|8|8388608|0|239.1.1.1||
broadcast-ctrl|3|262144|0|||
nopointopoint|1|262144|0|239.1.1.1|-2|
staggered|4|2097152|0|239.1.1.1|-w 2|1.5
loss-2pct|3|1048576|2|239.1.1.1||
loss-10pct|4|2097152|10|239.1.1.1||
fec-3rx|3|2097152|5|239.1.1.1|-F 8x2/128|
slow-receivers|3|4194304|0|239.1.1.1|-r 20M|
big-32mb|3|33554432|0|239.1.1.1||
"
	;;
esac

declare -a NAMES RESULTS
FAIL=0
while IFS='|' read -r name nrx size loss rdv extra stagger; do
	[ -z "${name// /}" ] && continue
	echo "################ scenario: $name (rx=$nrx size=$size loss=$loss rdv='${rdv:-broadcast}' args='$extra')"
	start=$(date +%s)
	out=$(NRX="$nrx" LOSS="$loss" RDV="$rdv" STAGGER="${stagger:-${STAGGER:-0.3}}" \
		W="/tmp/udpc_matrix_$name" timeout 600 bash test_lan.sh "$nrx" "$size" $extra 2>&1)
	rc=$?
	el=$(($(date +%s) - start))
	summary=$(echo "$out" | grep -E "^(LAN TEST|receiver #[0-9]+: (\*\*\*|NO ))" | head -8)
	if [ $rc -eq 0 ]; then
		RESULTS+=("PASS  $name (${el}s)")
	else
		RESULTS+=("FAIL  $name (${el}s)")
		echo "$summary"
		FAIL=$((FAIL + 1))
	fi
done <<<"$SCENARIOS"

echo
echo "=================== MATRIX ($MODE) ==================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "======================================================"
[ $FAIL -eq 0 ] && echo "ALL SCENARIOS PASSED" || echo "$FAIL SCENARIO(S) FAILED"
exit $FAIL
