#!/usr/bin/env python3

import os
import re
import sys


if len(sys.argv) != 2:
    raise SystemExit("expected the TiUP edit-config file")

path = sys.argv[1]
value = os.environ.get("PAUTO_VALUE")
if value not in ("true", "false"):
    raise SystemExit("PAUTO_VALUE must be true or false")

with open(path, encoding="utf-8") as config:
    lines = config.readlines()

key = "pessimistic-txn.pessimistic-auto-commit"
server_index = next(
    (index for index, line in enumerate(lines) if line.strip() == "server_configs:"),
    None,
)
if server_index is None:
    raise SystemExit("server_configs was not found")

server_indent = len(lines[server_index]) - len(lines[server_index].lstrip())
tidb_index = None
for index in range(server_index + 1, len(lines)):
    line = lines[index]
    indent = len(line) - len(line.lstrip())
    if line.strip() and indent <= server_indent:
        break
    if line.strip() == "tidb:":
        tidb_index = index
        break
if tidb_index is None:
    raise SystemExit("server_configs.tidb was not found")

tidb_indent = len(lines[tidb_index]) - len(lines[tidb_index].lstrip())
key_indent = " " * (tidb_indent + 4)
replacement = f"{key_indent}{key}: {value}\n"
for index in range(tidb_index + 1, len(lines)):
    line = lines[index]
    indent = len(line) - len(line.lstrip())
    if line.strip() and indent <= tidb_indent:
        lines.insert(tidb_index + 1, replacement)
        break
    if re.match(rf"^\s+{re.escape(key)}\s*:", line):
        lines[index] = replacement
        break
else:
    lines.append(replacement)

with open(path, "w", encoding="utf-8") as config:
    config.writelines(lines)
