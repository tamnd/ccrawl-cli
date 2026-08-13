#!/usr/bin/env bash
#
# docs-drift.sh compares `ccrawl --help` against the reference docs and fails if
# they disagree. It catches the two ways the docs rot: a command ships and never
# gets written down, and a flag gets documented that the binary does not have.
#
# It reads only docs/content/reference/, not the guides. Guides are prose and are
# allowed to talk about planned work; the reference is a contract.
#
#   ./scripts/docs-drift.sh                          # build, then check
#   CCRAWL=./bin/ccrawl ./scripts/docs-drift.sh      # check a binary you already have

set -euo pipefail

cd "$(dirname "$0")/.."

REF_DIR="docs/content/reference"
CLI_DOC="$REF_DIR/cli.md"
MD_DOC="$REF_DIR/markdown.md"
REQ_DOC="$REF_DIR/requirements.md"

# Cobra generates these; they are not ccrawl's surface and are not documented.
SKIP_COMMANDS=" help completion "

# Flags the reference names on purpose without any command having them. Keep this
# list short and say why each entry is here.
#   --nonexistent-flag: the exit-codes page uses it to show what an unknown flag does
SKIP_FLAGS=" --nonexistent-flag "

CCRAWL="${CCRAWL:-}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if [ -z "$CCRAWL" ]; then
  echo "building ccrawl ..."
  CGO_ENABLED=0 go build -o "$TMP/ccrawl" ./cmd/ccrawl
  CCRAWL="$TMP/ccrawl"
fi

fail=0
problem() { printf 'DRIFT: %s\n' "$1"; fail=1; }

# help_of prints the help for a command path ("" is the root), stripped of colour
# escapes and cached, since the flag check asks for the same help many times.
help_of() {
  local path="$1" key
  key="$TMP/help.$(printf '%s' "${path:-root}" | tr ' /' '__')"
  if [ ! -f "$key" ]; then
    # shellcheck disable=SC2086
    "$CCRAWL" $path --help 2>&1 | sed -e 's/'$'\033''\[[0-9;]*[a-zA-Z]//g' > "$key" || true
  fi
  cat "$key"
}

# subcommands_of lists the immediate subcommands of a command path. The kit help
# layout puts them in indented blocks between the COMMANDS and FLAGS headings,
# one per line with the name first.
subcommands_of() {
  help_of "$1" | awk '
    /^[[:space:]]+(READ |WRITE )?COMMANDS/ { inblock = 1; next }
    /^[[:space:]]+(FLAGS|USAGE|EXAMPLES)/  { inblock = 0 }
    inblock && /^[[:space:]]{2,}[a-z][a-z0-9-]*([[:space:]]|$)/ {
      gsub(/^[[:space:]]+/, "")
      print $1
    }
  ' | sort -u
}

listed() {
  case "$2" in *" $1 "*) return 0 ;; esac
  return 1
}

# scope_for maps a markdown heading to the command path its flags belong to. A
# heading that names a real command scopes to it; a prose heading (Global flags,
# Commands, Tuning) keeps the document's default scope.
scope_for() {
  local heading="$1" fallback="$2"
  heading="$(printf '%s' "$heading" | tr -d '`')"
  case "$heading" in
    *[!a-z\ 0-9-]*) printf '%s' "$fallback"; return ;;
  esac
  # shellcheck disable=SC2086
  if "$CCRAWL" $heading --help >/dev/null 2>&1; then
    expand_scope "$heading"
  else
    printf '%s' "$fallback"
  fi
}

# expand_scope turns a command path into the list of paths a flag under that
# heading may belong to: the command itself plus its subcommands. A section
# headed `## news` documents the flags of `news search`, not of the group.
expand_scope() {
  local path="$1" sub
  printf '%s' "$path"
  for sub in $(subcommands_of "$path"); do
    listed "$sub" "$SKIP_COMMANDS" && continue
    printf '\n%s %s' "$path" "$sub"
  done
}

# has_flag reports whether any of the candidate command paths accepts the flag.
# Candidates are newline separated so a doc that covers two sibling commands (the
# markdown pipelines) can check against both.
has_flag() {
  local tok="$1" paths="$2" p
  while IFS= read -r p; do
    [ -n "$p" ] || p=""
    if help_of "$p" | grep -qE -- "(^|[^a-z-])$tok([^a-z-]|\$)"; then
      return 0
    fi
  done <<EOF
$paths
EOF
  return 1
}

echo
echo "== every command is in the reference =="

top="$(subcommands_of "")"
for cmd in $top; do
  listed "$cmd" "$SKIP_COMMANDS" && continue
  if ! grep -qE "(^|[^a-z-])\`?$cmd\`?([^a-z-]|\$)" "$CLI_DOC"; then
    problem "command '$cmd' is not mentioned in $CLI_DOC"
    continue
  fi
  for sub in $(subcommands_of "$cmd"); do
    listed "$sub" "$SKIP_COMMANDS" && continue
    if ! grep -qrF -- "$cmd $sub" "$REF_DIR"; then
      problem "subcommand '$cmd $sub' is not mentioned anywhere in $REF_DIR"
    fi
  done
done
printf '  checked %s top-level commands\n' "$(printf '%s\n' "$top" | wc -l | tr -d ' ')"

echo
echo "== every documented flag exists on the binary =="

# Walk a reference page, tracking which command each heading scopes to, and check
# every --flag token under it: prose, tables, and examples alike. A flag passes if
# the scoped command's own help lists it, which covers the global flags too.
check_flags_in() {
  local doc="$1" fallback="$2"
  local path="$fallback" line heading tok checked=0

  while IFS= read -r line; do
    case "$line" in
      '## '*)  path=$(scope_for "${line#\#\# }" "$fallback"); continue ;;
      # A third level heading is a subsection of the command above it, so a prose
      # one keeps that command's scope rather than falling back to the whole page.
      '### '*) path=$(scope_for "${line#\#\#\# }" "$path"); continue ;;
    esac
    for tok in $(printf '%s\n' "$line" | grep -oE -- '--[a-z][a-z0-9-]*' || true); do
      listed "$tok" "$SKIP_FLAGS" && continue
      checked=$((checked + 1))
      if ! has_flag "$tok" "$path"; then
        # $path may hold a group and its subcommands; name the group in the message.
        local where="${path%%$'\n'*}"
        problem "$doc documents '$tok' under '${where:-ccrawl}', which does not accept it"
      fi
    done
  done < "$doc"
  printf '  checked %d flag mentions in %s\n' "$checked" "$doc"
}

check_flags_in "$CLI_DOC" ""
# The markdown page covers both pipelines at once, so a flag only has to exist on
# one of them.
check_flags_in "$MD_DOC" "$(printf 'markdown export\nmarkdown refetch')"
# The requirements page is organised by dependency rather than by command, so its
# headings name no command to scope against. The fallback is every command it
# talks about, and a flag passes if any one of them has it.
check_flags_in "$REQ_DOC" "$(printf '\ncolumnar sql\ncrawl fetch\ncrawl run\ndb sql\ndownload\nhost get\nhost enrich\nindex build\nurls publish\ndomains publish\nmarkdown export\nmarkdown refetch\npublish verify\napi\nserve')"

# A release notes page nobody links to is a page nobody reads. This is not the
# binary drifting from the docs, it is one doc drifting from another, but it is
# the same kind of miss and it happens at the same moment, so it rides along here
# rather than in a job of its own. v0.10.1 shipped with its notes written and
# unlinked, which is what put this check in.
check_release_notes_listed() {
  local index="docs/content/release-notes/_index.md" f slug checked=0
  for f in docs/content/release-notes/*.md; do
    slug="$(basename "$f" .md)"
    [ "$slug" = "_index" ] && continue
    checked=$((checked + 1))
    if ! grep -qF "/release-notes/$slug/" "$index"; then
      problem "$index does not link $f, so the release notes page will not list it"
    fi
  done
  printf '  checked %d release notes pages against %s\n' "$checked" "$index"
}

echo
check_release_notes_listed

echo
if [ "$fail" -ne 0 ]; then
  echo "the docs and the binary disagree, see the DRIFT lines above"
  exit 1
fi
echo "reference matches the binary"
