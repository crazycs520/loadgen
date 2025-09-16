#!/bin/bash

COUNT=3
TABLES=("customer" "district" "history" "new_order" "order_line" "orders" "stock" "warehouse")
RULES=("leader1" "leader2" "leader3" "leader4")

for ((i=0; i<${COUNT}; i++)); do
  for t in ${TABLES[@]};do
    idx=$(( i % ${#RULES[@]} ))
    echo "ALTER TABLE $t PARTITION p$i PLACEMENT POLICY=${RULES[$idx]};"
  done
done
