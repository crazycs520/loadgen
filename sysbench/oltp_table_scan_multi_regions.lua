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
-- OLTP Table Scan Multi Regions benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

sysbench.cmdline.options.range_size[2] = 2

local range_count = 10

local function get_table_num()
  return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_id()
  return sysbench.rand.default(1, sysbench.opt.table_size)
end

local function build_query()
  local conditions = {}

  for _ = 1, range_count do
    table.insert(conditions, "(id >= ? AND id< ?)")
  end

  return "SELECT k FROM sbtest%u WHERE " .. table.concat(conditions, " or ")
end

local query_template = build_query()

local function prepare_table_scan_multi_regions()
  local sql_type = sysbench.sql.type

  for table_num = 1, sysbench.opt.tables do
    stmt[table_num].table_scan_multi_regions = con:prepare(
      string.format(query_template, table_num)
    )
    param[table_num].table_scan_multi_regions = {}

    for bind_index = 1, range_count * 2 do
      param[table_num].table_scan_multi_regions[bind_index] =
        stmt[table_num].table_scan_multi_regions:bind_create(sql_type.INT)
    end

    stmt[table_num].table_scan_multi_regions:bind_param(
      unpack(param[table_num].table_scan_multi_regions)
    )
  end
end

local function execute_table_scan_multi_regions()
  local table_num = get_table_num()
  local bind_index
  local id

  for _ = 1, sysbench.opt.table_scan_ranges do
    bind_index = 1

    for _ = 1, range_count do
      id = get_id()
      param[table_num].table_scan_multi_regions[bind_index]:set(id)
      param[table_num].table_scan_multi_regions[bind_index + 1]:set(
        id + sysbench.opt.range_size
      )
      bind_index = bind_index + 2
    end

    stmt[table_num].table_scan_multi_regions:execute()
  end
end

function prepare_statements()
  prepare_table_scan_multi_regions()
end

function event()
  execute_table_scan_multi_regions()
end
