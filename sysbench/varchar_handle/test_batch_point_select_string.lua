local function fail(msg)
  io.stderr:write(msg .. "\n")
  os.exit(1)
end

local function assert_eq(actual, expected, msg)
  if actual ~= expected then
    fail(string.format("%s: expected %s, got %s", msg, tostring(expected), tostring(actual)))
  end
end

local next_id = 0

sysbench = {
  cmdline = {
    command = "test",
    options = {},
  },
  opt = {
    tables = 1,
    batch_point_selects = 1,
    table_size = 100,
  },
  rand = {
    uniform = function(min_value, max_value)
      return min_value
    end,
    default = function(min_value, max_value)
      next_id = next_id + 1
      return next_id
    end,
  },
  sql = {
    type = {
      INT = "INT",
      VARCHAR = "VARCHAR",
      CHAR = "CHAR",
    },
  },
  hooks = {},
}

event = function() end

dofile("oltp_common.lua")

local created_stmt = nil

local stmt_stub = {}
stmt_stub.__index = stmt_stub

function stmt_stub:bind_create(btype, max_len)
  local param = {
    type = btype,
    max_len = max_len,
    values = {},
  }

  function param:set(value)
    table.insert(self.values, value)
  end

  return param
end

function stmt_stub:bind_param(...)
  self.bound_params = {...}
  return true
end

function stmt_stub:execute()
  self.execute_count = (self.execute_count or 0) + 1
  return true
end

con = {
  prepare = function(_, sql)
    created_stmt = setmetatable({ sql = sql }, stmt_stub)
    return created_stmt
  end,
}

stmt = { [1] = {} }
param = { [1] = {} }

prepare_batch_point_selects()

assert_eq(created_stmt.sql, "SELECT c FROM sbtest1 WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", "prepared SQL")
assert_eq(#param[1].batch_point_selects, 10, "bind count")

for i, bind in ipairs(param[1].batch_point_selects) do
  assert_eq(bind.type, sysbench.sql.type.VARCHAR, string.format("bind %d type", i))
  assert_eq(bind.max_len, 64, string.format("bind %d max_len", i))
end

execute_batch_point_selects()
assert_eq(created_stmt.execute_count, 1, "execute count")

for i, bind in ipairs(param[1].batch_point_selects) do
  assert_eq(type(bind.values[1]), "string", string.format("bind %d runtime type", i))
  assert_eq(bind.values[1], tostring(i), string.format("bind %d runtime value", i))
end

print("ok")
