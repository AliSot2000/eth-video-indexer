#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

for entry in "$SCRIPT_DIR"/*
do
  if [[ $entry =~ .db ]]; then
    echo "Compressing $entry"
    sqlite3 "$entry" VACUUM;
  fi
done

echo "All databases compressed"