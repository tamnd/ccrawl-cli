#!/usr/bin/env bash
#
# coverage.sh runs the tests with coverage and fails if a package has slipped
# below its floor. The floors live in scripts/coverage-floors.txt, one package
# per line, and they only ever go up: a package that has climbed well above its
# floor is reported so the floor can be raised, and --update raises them all.
#
# The point is not the number. It is that the number cannot fall: a change that
# deletes a test, or adds a large untested command, has to say so in the diff
# rather than quietly lowering the bar.
#
#   ./scripts/coverage.sh             # run the tests, then check the floors
#   ./scripts/coverage.sh --update    # raise the floors to what the tests reach
#   COVERAGE_OUT=cov.out ./scripts/coverage.sh   # check a profile you already have

set -euo pipefail

cd "$(dirname "$0")/.."

FLOORS="scripts/coverage-floors.txt"
PROFILE="${COVERAGE_OUT:-}"
UPDATE=0
[ "${1:-}" = "--update" ] && UPDATE=1

if [ -z "$PROFILE" ]; then
  PROFILE="$(mktemp -t ccrawl-cov)"
  trap 'rm -f "$PROFILE"' EXIT
  echo "running tests with coverage ..."
  go test -count=1 -coverprofile="$PROFILE" ./... > /dev/null
fi

# percent_of prints the coverage of one package as a plain number. go tool cover
# reports per function, so the package total is recomputed from the profile: the
# covered statements over the total, which is what `go test -cover` prints.
percent_of() {
  local pkg="$1"
  awk -v pkg="$pkg" '
    NR == 1 { next }                       # mode: line
    {
      file = $1
      sub(/:.*/, "", file)
      # The package is the file path minus the file name.
      p = file
      sub(/\/[^\/]*$/, "", p)
      if (p != pkg) next
      total += $2
      if ($3 > 0) covered += $2
    }
    END {
      if (total == 0) { print "none"; exit }
      printf "%.1f", covered * 100 / total
    }
  ' "$PROFILE"
}

fail=0
updated=""

while IFS=$'\t' read -r pkg floor; do
  case "$pkg" in ''|'#'*) updated+="$pkg"$'\n'; continue ;; esac
  got="$(percent_of "$pkg")"
  if [ "$got" = "none" ]; then
    printf '  %-46s no statements in the profile\n' "$pkg"
    echo "COVERAGE: $pkg is in the floors file but not in the profile; did the tests run?"
    fail=1
    updated+="$pkg"$'\t'"$floor"$'\n'
    continue
  fi

  if awk -v g="$got" -v f="$floor" 'BEGIN { exit !(g < f) }'; then
    printf '  %-46s %5s%%  BELOW the %s%% floor\n' "$pkg" "$got" "$floor"
    fail=1
    updated+="$pkg"$'\t'"$floor"$'\n'
    continue
  fi

  # The new floor is the whole percent below what the tests reach, so ordinary
  # churn does not trip it while a real regression does.
  new="$(awk -v g="$got" 'BEGIN { printf "%d", int(g) }')"
  if [ "$UPDATE" = 1 ] && awk -v n="$new" -v f="$floor" 'BEGIN { exit !(n > f) }'; then
    printf '  %-46s %5s%%  floor raised %s -> %s\n' "$pkg" "$got" "$floor" "$new"
    updated+="$pkg"$'\t'"$new"$'\n'
    continue
  fi

  slack="$(awk -v g="$got" -v f="$floor" 'BEGIN { printf "%.1f", g - f }')"
  note=""
  if awk -v s="$slack" 'BEGIN { exit !(s >= 5) }'; then
    note="  (${slack} points of slack, consider --update)"
  fi
  printf '  %-46s %5s%%  floor %s%%%s\n' "$pkg" "$got" "$floor" "$note"
  updated+="$pkg"$'\t'"$floor"$'\n'
done < "$FLOORS"

if [ "$UPDATE" = 1 ]; then
  printf '%s' "$updated" > "$FLOORS"
  echo "wrote $FLOORS"
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "coverage fell below a floor, see the BELOW lines above"
  echo "add tests, or if the drop is deliberate say why in the PR and lower the floor by hand"
  exit 1
fi
echo "every package is at or above its floor"
