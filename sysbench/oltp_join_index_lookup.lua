#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- OLTP Join Index Lookup benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

local join_index_lookup_template =
  "SELECT t1.c, t2.c FROM sbtest%u as t1 join sbtest%u as t2 WHERE t1.k=t2.k AND t1.k = ?"

local function next_table_num(table_num, total_tables)
  local next_num = table_num + 1
  if next_num > total_tables then
    next_num = 1
  end
  return next_num
end

local function build_query(table_num, total_tables)
  return string.format(
    join_index_lookup_template,
    table_num,
    next_table_num(table_num, total_tables)
  )
end

local function prepare_join_index_lookups()
  local sql_type = sysbench.sql.type
  local total_tables = sysbench.opt.tables

  for table_num = 1, total_tables do
    local query = build_query(table_num, total_tables)

    stmt[table_num].join_index_lookups = con:prepare(query)
    param[table_num].join_index_lookups = {}
    param[table_num].join_index_lookups[1] =
      stmt[table_num].join_index_lookups:bind_create(sql_type.INT)
    stmt[table_num].join_index_lookups:bind_param(unpack(param[table_num].join_index_lookups))
  end
end

local function execute_join_index_lookups()
  local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)

  param[table_num].join_index_lookups[1]:set(
    sysbench.rand.default(1, sysbench.opt.table_size)
  )
  stmt[table_num].join_index_lookups:execute()
end

function prepare_statements()
  prepare_join_index_lookups()
end

function event()
  execute_join_index_lookups()
end

