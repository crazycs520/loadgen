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
replacement = f"        {key}: {value}\n"
for index, line in enumerate(lines):
    if re.match(rf"^\s+{re.escape(key)}\s*:", line):
        lines[index] = replacement
        break
else:
    for index, line in enumerate(lines):
        if re.match(r"^\s{{4}}tidb:\s*$", line):
            lines.insert(index + 1, replacement)
            break
    else:
        raise SystemExit("server_configs.tidb was not found")

with open(path, "w", encoding="utf-8") as config:
    config.writelines(lines)
