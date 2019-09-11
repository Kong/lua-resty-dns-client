local indent = require("pl.text").indent
local decode = require("cjson.safe").decode
local readlines = require("pl.utils").readlines
local writefile = require("pl.utils").writefile
local pretty = require("pl.pretty").write
local split = require("pl.utils").split


local filename = arg[1]
if not filename then
  print("1st argument (filename required) is missing")
  os.exit(1)
end

local no_pid = -1
local lines = readlines(filename)
local files = setmetatable({}, {
  __index = function(self, pid)
    local new_file = {}
    print("found pid: ", pid)
    rawset(self, pid, new_file)
    if pid ~= no_pid then
      -- if there is a preface, copy it into the new file
      for i, v in ipairs(self[no_pid]) do
        new_file[i] = v
      end
    end
    return new_file
  end,
})

-- grab pid:
local PID_PATTERN = "%[%l+%] (%d+)#%d+:"  -- returns only the pid
-- split line:
local SPLIT_PATTERN = "^(.-:%d*: )(.+)$"  -- returns prefix + "[dns-client] ..."
-- grab json:
local JSON_PATTERN = "^(.-)({.+})(.*)$" -- returns prefix, JSON, postfix
-- grab "Tried" json:
local TRIED_PATTERN = "^(.-Tried: )(%[.+%])(.*)$" -- returns prefix, JSON, postfix


for i = 1, #lines do
  local line = lines[i] --("%6.0f "):format(i) .. lines[i]
  local pid = line:match(PID_PATTERN)
  if not pid then
    -- no PID found, inject in 'no_pid' table
    table.insert(files[no_pid], line)
  else
    local line_prefix, message = line:match(SPLIT_PATTERN)
    if not line_prefix then
      -- not a dns-client line, just insert
      table.insert(files[pid], line)
    else
      local pre_json, json, post_json = message:match(TRIED_PATTERN)
      if not pre_json then
        pre_json, json, post_json = message:match(JSON_PATTERN)
      end
      if not pre_json then
        -- no json to expand, just insert
        table.insert(files[pid], line)
      else
        -- json to expand
        local json_table = decode(json)
        if not json_table then
          json_table = decode("["..json.."]")
        end
        if not json_table then
          -- failed decoding json
          table.insert(files[pid], line)
        else
          -- pretty print json
          local pretty_json = indent(pretty(json_table), 4)
          if pretty_json:sub(-1,-1) == "\n" then pretty_json = pretty_json:sub(1,-2) end
          local lines = split(pretty_json, "\n", true)
          local file = files[pid]

          table.insert(file, line_prefix .. pre_json)
          for _, entry in ipairs(lines) do
            table.insert(file, line_prefix .. entry)
          end
          if #post_json > 0 then
            table.insert(file, line_prefix .. "    " .. post_json)
          end
        end
      end
    end
  end
end

for pid, file in pairs(files) do
  if pid == no_pid then pid = "unknown" end
  local name = filename .. "_pid-" .. pid .. ".log"
  print("writing: ", name)
  writefile(name, table.concat(file, "\n"))
end

print("\n")
