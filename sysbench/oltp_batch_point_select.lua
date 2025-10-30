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
-- along with this program; if not, write to the
-- Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- OLTP Batch Point Select benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

function prepare_statements()
   if sysbench.opt.batch_point_selects > 1 then
      prepare_begin()
      prepare_commit()
   end
   prepare_batch_point_selects()
end

function event()
   if sysbench.opt.batch_point_selects > 1 then
      begin()
   end

   execute_batch_point_selects()

   if sysbench.opt.batch_point_selects > 1 then
      commit()
   end
end
