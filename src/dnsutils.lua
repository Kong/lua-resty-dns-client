--------------------------------------------------------------------------
-- DNS utility module to parse the `/etc/hosts` and `/etc/resolv.conf` 
-- configuration files.
--
-- Uses LuaSocket if available for the `gettime` function. Or falls back on `os.time` if LuaSocket 
-- is unavailable.
-- 
-- _NOTE_: parsing the files is done using blocking i/o file operations, for non-blocking applications
-- parse only at startup.
--
-- @copyright Thijs Schreijer, Mashape Inc.
-- @license MIT
-- @usage

local _M = {}
local utils = require("pl.utils")
local tinsert = table.insert
local success, socket = pcall(require, "socket")
local gettime = success and socket.gettime or os.time
local is_windows = package.config:sub(1,1) == [[\]]

-- pattern that will only match data before a # or ; comment
-- returns nil if there is none before the # or ;
-- 2nd capture is the comment after the # or ;
local PATT_COMMENT = "^([^#;]+)[#;]*(.*)$"
-- Splits a string in IP and hostnames part, drops leading/trailing whitespace
local PATT_IP_HOST = "^%s*([%x%.%:]+)%s+(%S.-%S)%s*$"
-- hosts filename to use when omitted
local DEFAULT_HOSTS = "/etc/hosts"
if is_windows then
  DEFAULT_HOSTS = (os.getenv("SystemRoot") or "") .. [[\system32\drivers\etc\hosts]]
end

-- resolv.conf default filename
local DEFAULT_RESOLV_CONF = "/etc/resolv.conf"

--- Parses a hosts file.
-- Does not check for correctness of ip addresses nor hostnames. Might return `nil + error` if the file
-- cannot be read.
-- @param filename (optional) File to parse, defaults to "/etc/hosts" if omitted, or a table with the file contents in lines.
-- @return 1; reverse lookup table, ip addresses indexed by their canonical names and aliases
-- @return 2; list with all entries. Containing fields `ip` and `canonical`, and a list of aliasses
_M.parse_hosts = function(filename)
  local lines
  if type(filename) == "table" then
    lines = filename
  else
    local err
    lines, err = utils.readlines(filename or DEFAULT_HOSTS)
    if not lines then return lines, err end
  end
  local result = {}
  local reverse = {}
  for _,line in ipairs(lines) do 
    local data, comments = line:match(PATT_COMMENT)
    if data then
      local ip, hosts = data:match(PATT_IP_HOST)
      if ip and hosts then
        hosts = hosts:lower()
        local entry = { ip = ip }
        local key = "canonical"
        for host in hosts:gmatch("%S+") do
          entry[key] = host
          key = (tonumber(key) or 0) + 1
          reverse[host] = reverse[host] or ip -- do not overwrite, first one wins
        end
        tinsert(result, entry)
      end
    end
  end
  return reverse, result
end


local bool_options = { "debug", "rotate", "no-check-names", "inet6", 
                       "ip6-bytestring", "ip6-dotint", "no-ip6-dotint", 
                       "edns0", "single-request", "single-request-reopen",
                       "no-tld-query", "use-vc"}
for i, name in ipairs(bool_options) do bool_options[name] = name bool_options[i] = nil end

local num_options = { "ndots", "timeout", "attempts" }
for i, name in ipairs(num_options) do num_options[name] = name num_options[i] = nil end

-- Parses a single option.
-- @param target table in which to insert the option
-- @param details string containing the option details
-- @return modified target table
local parse_option = function(target, details)
  local option, n = details:match("^([^:]+)%:*(%d*)$")
  if bool_options[option] and n == "" then
    target[option] = true
    if option == "ip6-dotint" then target["no-ip6-dotint"] = nil end
    if option == "no-ip6-dotint" then target["ip6-dotint"] = nil end
  elseif num_options[option] and tonumber(n) then
    target[option] = tonumber(n)
  end
end

--- Parses a resolv.conf file.
-- Does not check for correctness of ip addresses nor hostnames, bad options will be ignored. Might 
-- return `nil + error` if the file cannot be read.
-- @param filename (optional) File to parse (defaults to "/etc/resolv.conf" if omitted) or a table with the file contents in lines.
-- @return table with fields `nameserver` (table), `domain` (string), `search` (table), `sortlist` (table) and `options` (table)
_M.parse_resolv_conf = function(filename)
  local lines
  if type(filename) == "table" then
    lines = filename
  else
    local err
    lines, err = utils.readlines(filename or DEFAULT_RESOLV_CONF)
    if not lines then return lines, err end
  end
  local result = {}
  for _,line in ipairs(lines) do 
    local data, comments = line:match(PATT_COMMENT)
    if data then
      local option, details = data:match("^%s*(%a+)%s+(.-)%s*$")
      if option == "nameserver" then
        result.nameserver = result.nameserver or {}
        tinsert(result.nameserver, details:lower())
      elseif option == "domain" then
        result.search = nil  -- mutually exclusive, last one wins
        result.domain = details:lower()
      elseif option == "search" then
        result.domain = nil  -- mutually exclusive, last one wins
        local search = {}
        result.search = search
        for host in details:gmatch("%S+") do
          tinsert(search, host:lower())
        end
      elseif option == "sortlist" then
        local list = {}
        result.sortlist = list
        for ips in details:gmatch("%S+") do
          tinsert(list, ips)
        end
      elseif option == "options" then
        result.options = result.options or {}
        parse_option(result.options, details)
      end
    end
  end
  return result
end

--- Will parse `LOCALDOMAIN` and `RES_OPTIONS` environment variables
-- and insert them into the given configuration table.
--
-- NOTE: if the input is `nil+error` it will return the input, to allow for pass-through error handling
-- @param config Options table, as parsed by `parse_resolv_conf()`, or an empty table to get only the environment options
-- @return modified table
-- @usage local dnsutils = require("dnsutils")
--
-- -- errors are passed through, so this;
-- local config, err = dnsutils.parse_resolf_conf()
-- if config then 
--   config, err = dnsutils.apply_env(config)
-- end
-- 
-- -- Is identical to;
-- local config, err = dnsutils.apply_env(dnsutils.parse_resolf_conf())
_M.apply_env = function(config, err)
  if not config then return config, err end -- allow for 'nil+error' pass-through
  local localdomain = os.getenv("LOCALDOMAIN") or ""
  if localdomain ~= "" then
    config.domain = nil  -- mutually exclusive, last one wins
    local search = {}
    config.search = search
    for host in localdomain:gmatch("%S+") do
      tinsert(search, host:lower())
    end
  end

  local options = os.getenv("RES_OPTIONS") or ""
  if options ~= "" then
    config.options = config.options or {}
    for option in options:gmatch("%S+") do
      parse_option(config.options, option)
    end
  end
  return config
end

if is_windows then
  -- there are no environment variables like this on Windows, so short-circuit it
  _M.parse_resolv.conf = function(...) return ... end
end


local cache_hosts  -- cached value
local cache_hostsr  -- cached value
local last_hosts = 0 -- timestamp
local ttl_hosts   -- time to live for cache

--- returns the `parse_hosts()` results, but cached.
-- Once `ttl` has been provided, only after it expires the file will be parsed again
-- @param ttl cache time-to-live in seconds (can be updated in following calls)
-- @return reverse and list tables, see `parse_hosts()`. NOTE: if cached, the _SAME_ tables will be returned, so do not modify them unless you know what you are doing
_M.gethosts = function(ttl)
  ttl_hosts = ttl or ttl_hosts
  local now = gettime()
  if (not ttl_hosts) or (last_hosts + ttl_hosts <= now) then
    cache_hosts = nil    -- expired
    cache_hostsr = nil    -- expired
  end

  if not cache_hosts then
    cache_hostsr, cache_hosts = _M.parse_hosts()
    last_hosts = now
  end
  
  return cache_hostsr, cache_hosts
end


local cache_resolv  -- cached value
local last_resolv = 0 -- timestamp
local ttl_resolv   -- time to live for cache

--- returns the `dnsutils.apply_env(dnsutils.parse_resolve_conf())` results, but cached.
-- Once `ttl` has been provided, only after it expires it will be parsed again
-- @param ttl cache time-to-live in seconds (can be updated in following calls)
-- @return configuration table, see `parse_resolve_conf()`. NOTE: if cached, the _SAME_ table will be returned, so do not modify them unless you know what you are doing
_M.getresolv = function(ttl)
  ttl_resolv = ttl or ttl_resolv
  local now = gettime()
  if (not ttl_resolv) or (last_resolv + ttl_resolv <= now) then
    cache_resolv = nil    -- expired
  end

  if not cache_resolv then
    last_resolv = now
    cache_resolv = _M.apply_env(_M.parse_resolv_conf())
  end
  
  return cache_resolv
end

return _M