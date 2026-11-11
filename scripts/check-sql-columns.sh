#!/usr/bin/env bash
#
# Refuse SQL that selects or returns `*` from a table.
#
# sqlx keeps each prepared statement per connection, and Postgres fixes a
# statement's result shape when it is prepared. A statement reading `*` from a
# table that a migration then extends is refused on every connection that
# prepared it before ("cached plan must not change result type") — in a
# rolling upgrade, every connection of every engine still on the old build.
# Name the columns instead: crates/storage/src/repositories/columns.rs has the
# lists the repositories use.
#
# Allowed: `COUNT(*)` and the like (not a select list), `SELECT * FROM
# UNNEST(...)` and `SELECT * FROM (subquery)` (their shape is the statement's
# own, the subquery naming its columns), comments, and a line
# marked `sql-columns: allow` (on it or the line before) — for a test that
# reproduces the failure on purpose.
#
#   scripts/check-sql-columns.sh [root]     # default: this repository

set -euo pipefail
root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

files=()
while IFS= read -r -d '' f; do files+=("$f"); done < <(
  find "$root/crates" "$root/services" -path '*/src/*' -name '*.rs' \
    -not -path '*/target/*' -print0
)

perl -0777 -ne '
  while (/\b(?:SELECT(?:\s+DISTINCT)?|RETURNING)\s+(?:\w+\.)?\*(?!\s*\))|,\s*\w+\.\*(?=\s*(?:,|FROM\b|$))/gim) {
    my ($start, $end) = ($-[0], $+[0]);
    my $line_start = rindex($_, "\n", $start) + 1;
    my $line_end = index($_, "\n", $start);
    $line_end = length($_) if $line_end < 0;
    my $line = substr($_, $line_start, $line_end - $line_start);
    next if $line =~ m{^\s*//};
    my $prev_start = $line_start > 0 ? rindex($_, "\n", $line_start - 2) + 1 : 0;
    my $prev = $line_start > 0 ? substr($_, $prev_start, $line_start - 1 - $prev_start) : "";
    next if $line =~ /sql-columns: allow/ || $prev =~ /sql-columns: allow/;
    next if substr($_, $end, 40) =~ /^\s*FROM\s+(?:UNNEST\s*)?\(/i;
    my $n = (substr($_, 0, $start) =~ tr/\n//) + 1;
    $line =~ s/^\s+//;
    print "$ARGV:$n: $line\n";
    $found = 1;
  }
  END { exit($found ? 1 : 0) }
' "${files[@]}" && exit 0

echo >&2
echo "SQL above selects or returns * — name the columns (crates/storage/src/repositories/columns.rs)." >&2
exit 1
