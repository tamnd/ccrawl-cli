#!/usr/bin/env bash
#
# Put the same ccrawl on every machine in the fleet, with the units it runs
# under.
#
# The reason this exists is the state the fleet was found in: server3 was on
# 0.5.0 from July, server1 and server2 were on dev builds from two different
# afternoons, and nothing was under a unit at all. A fleet where the three
# machines are running three binaries is a fleet where a rate difference between
# them means nothing, so the first thing this does is build one binary and the
# last thing it does is print the version off all three.
#
# It installs and enables. It does not start anything, because starting a crawl
# is a decision about somebody else's bandwidth and belongs in the runbook next
# to the sentence about what to watch. Pass --start to say yes to it here.
#
#   deploy/install.sh                                  # all three, domains
#   deploy/install.sh --servers server1 --kind urls    # one machine, url list
#   deploy/install.sh --start                          # and turn it on
#
set -euo pipefail

SERVERS=(server1 server2 server3)
KIND=domains
DO_START=0
DO_BUILD=1
BIN=/tmp/ccrawl-fleet-build

usage() {
	sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
	exit "${1:-0}"
}

while [ $# -gt 0 ]; do
	case "$1" in
	--servers)
		IFS=', ' read -r -a SERVERS <<<"$2"
		shift 2
		;;
	--kind)
		KIND=$2
		shift 2
		;;
	--start) DO_START=1; shift ;;
	--no-build) DO_BUILD=0; shift ;;
	-h | --help) usage 0 ;;
	*)
		echo "install.sh: $1 is not an option I know" >&2
		usage 1
		;;
	esac
done

case "$KIND" in
domains | urls) ;;
*)
	echo "install.sh: --kind is domains or urls, not $KIND" >&2
	exit 1
	;;
esac

here=$(cd "$(dirname "$0")/.." && pwd)
cd "$here"

# The version has to come off the tree that is being deployed, and it has to be
# stamped into the binary, or the check at the end is just three copies of the
# word dev.
version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
commit=$(git rev-parse --short HEAD 2>/dev/null || echo none)
date=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [ "$DO_BUILD" = 1 ]; then
	echo "building $version for linux/amd64"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
		-ldflags "-s -w \
			-X github.com/tamnd/ccrawl-cli/cli.Version=$version \
			-X github.com/tamnd/ccrawl-cli/cli.Commit=$commit \
			-X github.com/tamnd/ccrawl-cli/cli.Date=$date" \
		-o "$BIN" ./cmd/ccrawl
fi
[ -f "$BIN" ] || { echo "install.sh: no binary at $BIN" >&2; exit 1; }

# sha256sum on Linux, shasum on a Mac. The point of computing it here is that
# the remote checks it before the binary is moved into place, so a truncated
# copy over a flaky link fails the install instead of becoming the fleet.
if command -v sha256sum >/dev/null; then
	sum=$(sha256sum "$BIN" | cut -d' ' -f1)
else
	sum=$(shasum -a 256 "$BIN" | cut -d' ' -f1)
fi
echo "sha256 $sum"

shard=0
for s in "${SERVERS[@]}"; do
	echo
	echo "===== $s, shard $shard of ${#SERVERS[@]}, $KIND ====="

	scp -q "$BIN" "$s:/tmp/ccrawl.new"
	scp -q deploy/systemd/ccrawl-recrawl@.service \
		deploy/systemd/ccrawl-publish@.service \
		deploy/systemd/ccrawl-recrawl.target \
		"$s:/tmp/"
	scp -q "deploy/env/recrawl-$KIND.env.example" "$s:/tmp/recrawl-$KIND.env.example"

	ssh "$s" "
		set -euo pipefail
		got=\$(sha256sum /tmp/ccrawl.new | cut -d' ' -f1)
		if [ \"\$got\" != '$sum' ]; then
			echo \"the copy arrived as \$got, not $sum\" >&2
			rm -f /tmp/ccrawl.new
			exit 1
		fi
		chmod 0755 /tmp/ccrawl.new
		# Moved rather than copied over, so a running crawl keeps the inode it
		# started with and picks the new binary up when it is next restarted.
		mv /tmp/ccrawl.new /usr/local/bin/ccrawl

		mkdir -p /etc/ccrawl /var/lib/ccrawl/$KIND/captures
		mv /tmp/ccrawl-recrawl@.service /tmp/ccrawl-publish@.service \
			/tmp/ccrawl-recrawl.target /etc/systemd/system/

		# The env file holds this machine's shard number and its tuning, so it
		# is written once and left alone. A deploy that reset it would silently
		# undo whatever the last person measured.
		#
		# CCRAWL_SERVER is the name this script was given and not the machine's
		# own hostname. It ends up in every shard file name and in the ledger
		# path, and these boxes call themselves things like vmi3391933, so
		# taking the hostname puts a number nobody recognises on the hub and
		# leaves the fleet with no way to say which file came from where.
		if [ ! -f /etc/ccrawl/recrawl-$KIND.env ]; then
			sed -e 's/^CCRAWL_SHARD=.*/CCRAWL_SHARD=$shard/' \
			    -e 's/^CCRAWL_SHARDS=.*/CCRAWL_SHARDS=${#SERVERS[@]}/' \
			    -e 's/^CCRAWL_SERVER=.*/CCRAWL_SERVER=$s/' \
			    /tmp/recrawl-$KIND.env.example > /etc/ccrawl/recrawl-$KIND.env
			echo 'wrote /etc/ccrawl/recrawl-$KIND.env'
		else
			echo 'kept /etc/ccrawl/recrawl-$KIND.env as it was'
		fi
		rm -f /tmp/recrawl-$KIND.env.example
		chmod 0644 /etc/ccrawl/recrawl-$KIND.env

		# The token lives in its own file that only the publisher reads. It is
		# created empty rather than filled, because a secret does not belong in
		# a repo or in an argument list, and an empty one fails loudly at the
		# first commit rather than quietly at the hundredth.
		if [ ! -f /etc/ccrawl/hf.env ]; then
			echo 'HF_TOKEN=' > /etc/ccrawl/hf.env
			echo 'put the token in /etc/ccrawl/hf.env before starting the publisher'
		fi
		chmod 0600 /etc/ccrawl/hf.env

		# Memory is the one setting that cannot be the same everywhere, since
		# these boxes have 5, 11 and 23 GB and the resident set scales with
		# workers times writers. MemoryHigh throttles rather than kills, which
		# on a crawler is the right end: a run that slows down is one that
		# resumes, and a run that is OOM killed repeats its batch.
		total_kb=\$(awk '/MemTotal/{print \$2}' /proc/meminfo)
		high_mb=\$(( total_kb / 1024 * 70 / 100 ))
		mkdir -p /etc/systemd/system/ccrawl-recrawl@.service.d
		printf '# written by deploy/install.sh from this machine RAM\n[Service]\nMemoryHigh=%sM\n' \
			\"\$high_mb\" > /etc/systemd/system/ccrawl-recrawl@.service.d/memory.conf

		systemctl daemon-reload
		systemctl enable ccrawl-recrawl.target >/dev/null 2>&1
		systemctl enable ccrawl-recrawl@$KIND.service ccrawl-publish@$KIND.service >/dev/null 2>&1
		echo \"installed \$(/usr/local/bin/ccrawl version --short), MemoryHigh \${high_mb}M, \$(df -h /var/lib/ccrawl | awk 'NR==2{print \$4}') free\"
	"

	if [ "$DO_START" = 1 ]; then
		ssh "$s" "systemctl restart ccrawl-recrawl@$KIND.service ccrawl-publish@$KIND.service && systemctl is-active ccrawl-recrawl@$KIND.service ccrawl-publish@$KIND.service"
	fi
	shard=$((shard + 1))
done

echo
echo "===== version across the fleet ====="
for s in "${SERVERS[@]}"; do
	printf '%-10s %s\n' "$s" "$(ssh "$s" '/usr/local/bin/ccrawl version --short')"
done
