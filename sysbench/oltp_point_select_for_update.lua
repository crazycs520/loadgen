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
-- OLTP Point Select FOR UPDATE benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

-- SQL template:
--   begin;
--   SELECT c FROM sbtest%u WHERE id = ? FOR UPDATE;
--   commit;
local point_select_for_update_template =
  "SELECT c FROM sbtest%u WHERE id = ? FOR UPDATE"

local function prepare_point_selects_for_update()
  local sql_type = sysbench.sql.type
  local total_tables = sysbench.opt.tables

  for table_num = 1, total_tables do
    local query = string.format(point_select_for_update_template, table_num)

    stmt[table_num].point_select_for_update = con:prepare(query)
    param[table_num].point_select_for_update = {}
    param[table_num].point_select_for_update[1] =
      stmt[table_num].point_select_for_update:bind_create(sql_type.INT)
    stmt[table_num].point_select_for_update:bind_param(
      unpack(param[table_num].point_select_for_update)
    )
  end
end

local function execute_point_selects_for_update()
  local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)

  param[table_num].point_select_for_update[1]:set(
    sysbench.rand.default(1, sysbench.opt.table_size)
  )
  stmt[table_num].point_select_for_update:execute()
end

function prepare_statements()
  prepare_point_selects_for_update()
  prepare_begin()
  prepare_commit()
end

function event()
  begin()
  execute_point_selects_for_update()
  commit()
end

