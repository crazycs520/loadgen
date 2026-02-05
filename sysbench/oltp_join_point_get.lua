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
-- OLTP Join Point Get benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

local join_point_get_template =
  "SELECT t1.c FROM sbtest%u as t1 join sbtest%u as t2 WHERE t1.id=t2.id AND t1.id = ?"

local function next_table_num(table_num, total_tables)
  local next_num = table_num + 1
  if next_num > total_tables then
    next_num = 1
  end
  return next_num
end

local function build_query(table_num, total_tables)
  return string.format(
    join_point_get_template,
    table_num,
    next_table_num(table_num, total_tables)
  )
end

local function prepare_join_point_gets()
  local sql_type = sysbench.sql.type
  local total_tables = sysbench.opt.tables

  for table_num = 1, total_tables do
    local query = build_query(table_num, total_tables)

    stmt[table_num].join_point_gets = con:prepare(query)
    param[table_num].join_point_gets = {}
    param[table_num].join_point_gets[1] =
      stmt[table_num].join_point_gets:bind_create(sql_type.INT)
    stmt[table_num].join_point_gets:bind_param(unpack(param[table_num].join_point_gets))
  end
end

local function execute_join_point_gets()
  local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)

  for _ = 1, sysbench.opt.point_selects do
    param[table_num].join_point_gets[1]:set(
      sysbench.rand.default(1, sysbench.opt.table_size)
    )
    stmt[table_num].join_point_gets:execute()
  end
end

function prepare_statements()
  -- Use 1 query per event, rather than sysbench.opt.point_selects which
  -- defaults to 10 in other OLTP scripts.
  sysbench.opt.point_selects = 1

  prepare_join_point_gets()
end

function event()
  execute_join_point_gets()
end
