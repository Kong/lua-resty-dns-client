-- script to analyze logs.
-- If extensive debug is enabled (the long-comments) in the client, then
-- a lot of data will be logged as json.
-- this script will update a log file by expanding the json, it will insert
-- original line numbers for each line to track with the original log.
-- Optionally filters and token replacements can be made.
-- check the TODO comments in the code below.



-- The input log-file name, extension must be ".log", but that extension must
-- be omitted here
-- TODO: update
local filename = "log_error_2018-02-16"

-- The output filename, same as the input name, but with this suffix:
-- TODO: update
local output = filename .. "_out3.log"



filename = filename .. ".log"
local indent = require("pl.text").indent
local decode = require("cjson.safe").decode
local readfile = require("pl.utils").readfile
local readlines = require("pl.utils").readlines
local writefile = require("pl.utils").writefile
local pretty = require("pl.pretty").write


local file = assert(readfile(filename))
local tokens = {}
local list = {}



-- Look for, and list tokens.
-- In case the log has been adapted and tokens inserted for sensitive data
-- this allows to replace the tokens again with original-type data
-- and also prevent breaking the JSON serialization

-- TODO: update pattern to match a token
local token_match = "(%<%<.-%>%>)"


local i = 0
for token in file:gmatch(token_match) do
  if not tokens[token] then
    tokens[token] = 1
    table.insert(list, token)
  else
    tokens[token] = tokens[token] + 1
  end
end

-- display the tokens found
table.sort(list)
print("local substitutes = {")
for _, token in ipairs(list) do
  print('  ["'..token..'"] = nil,    -- ', tokens[token])
end
print("}")



-- Update tokens.
-- here we provide a list of replacement strings for the tokens found
-- TODO: update
local substitutes = {
--  ["<< PORT>>"] = "8080",    -- 1384
--  ["<<HOST ALIAS 1>>"] = nil,    -- 7
--  ["<<HOST ALIAS 2>>"] = "some.hostname",    -- 1619
--  ["<<HOSTNAME 1>>"] = "internal.hostname1",    -- 5773
--  ["<<IP ADDRESS 10>>"] = "10.10.10.10",    -- 206
--  ["<<IP ADDRESS 11>>"] = "10.10.10.11",    -- 206
--  ["<<IP ADDRESS 12>>"] = "10.10.10.12",    -- 3201
--  ["<<IP ADDRESS 13>>"] = "10.10.10.13",    -- 224
--  ["<<IP ADDRESS 1>>"] = "10.10.10.1",    -- 2
--  ["<<IP ADDRESS 2>>"] = "10.10.10.2",    -- 2986
--  ["<<IP ADDRESS 3>>"] = "10.10.10.3",    -- 810
--  ["<<IP ADDRESS 7>>"] = "10.10.10.7",    -- 258
--  ["<<IP ADDRESS 8>>"] = "10.10.10.8",    -- 210
--  ["<<IP ADDRESS 9>>"] = "10.10.10.9",    -- 252
--  ["<<UPSTREAM ALIAS 1>>"] = "external.name1",    -- 9606
--  ["<<UPSTREAM ALIAS 2>>"] = "external.name2",    -- 9979
--  ["<<UPSTREAM ALIAS 3.1>>"] = "external.name3-1",    -- 13655
--  ["<<UPSTREAM ALIAS 3>>"] = "external.name3",    -- 10602
--  ["<<UPSTREAM ALIAS 4>>"] = "external.name4",    -- 10273
--  ["<<UPSTREAM HOSTNAME 2>>"] = "internal.hostname2",    -- 2733
--  ["<<UPSTREAM HOSTNAME 4>>"] = "internal.hostname4",    -- 5927
--  ["<<UPSTREAM IP 2>>"] = "10.10.11.2",    -- 1380
--  ["<<UPSTREAM IP 4>>"] = "10.10.10.4",    -- 2998
--  ["<<UPSTREAM PORT 2>>"] = "8082",    -- 1348
--  ["<<UPSTREAM PORT 4>>"] = "8084",    -- 2750
--  ["<<UPSTREAM VIP 3.1>>"] = "internal.hostname3-1",    -- 322
--  ["<<UPSTREAM VIP 3.2>>"] = nil,    -- 38
--  ["<<UPSTREAM VIP 3.3>>"] = "internal.hostname3-3",    -- 385
--  ["<<UPSTREAM VIP 3.4>>"] = "internal.hostname3-4",    -- 394
--  ["<<UPSTREAM VIP 3.5>>"] = "internal.hostname3-5",    -- 343
--  ["<<UPSTREAM VIP 3.6>>"] = "internal.hostname3-6",    -- 6563
--  ["<<UPSTREAM VIP 3.7>>"] = "internal.hostname3-7",    -- 316
--  ["<<UPSTREAM VIP 3.8>>"] = "internal.hostname3-8",    -- 316
}

-- escape the tokens, and replace all occurences
for token, substitute in pairs(substitutes) do
  l = {}
  for i = 1, #token do
    --table.insert(l, "%")
    table.insert(l, token:sub(i,i))
  end
  token = table.concat(l)

  file, l = file:gsub(token, substitute)
  print(token, " : ", l)    -- print number of replacements
end


-- write output
writefile(output, file)
file = nil  -- save memory
local lines = readlines(output)  -- read again, as lines


-- provide a filter to match, only lines matching at least 1 of those filters
-- will be included
-- TODO: update
local matches = {
--  "external%.name4 ",
--  "external%.name4:",
--  "external%.name4$",
}

local out = {}
local out_n = 0
for i = 1, #lines do
  local line = ("%6.0f "):format(i) .. lines[i]
  local m = (#matches == 0)
  for _, patt in ipairs(matches) do
    if line:match(patt) then
      m = true
      break
    end
  end
  if m then
    local prefix, json = line:match("^(.-)(%{.+%})$")
    if not json then
      prefix, json = line:match("^(.-)(%[%{.+%}%])$")
    end
    if json then
      local t, err = assert(decode(json))
      if t then
        t = indent(pretty(t), 4)
        if t:sub(-1,-1) == "\n" then t = t:sub(1,-2) end
        lines[i] = prefix .. "\n" .. t
      end
    else
      lines[i] = line
    end
    out_n = out_n + 1
    out[out_n] = lines[i]
  end
end

writefile(output, table.concat(out, "\n"))
