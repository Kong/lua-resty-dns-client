--------------------------------------------------------------------------
-- DNS utility module. 
--
-- Parses the `/etc/hosts` and `/etc/resolv.conf` configuration files, caches them, 
-- and provides some utility functions. 
--
-- _NOTE_: parsing the files is done using blocking i/o file operations.
--
-- @copyright 2016 Mashape Inc.
-- @author Thijs Schreijer
-- @license Apache 2.0


local _M = {}
local utils = require("pl.utils")
local gsub = string.gsub
local tinsert = table.insert
local gettime = ngx.now

-- pattern that will only match data before a # or ; comment
-- returns nil if there is none before the # or ;
-- 2nd capture is the comment after the # or ;
local PATT_COMMENT = "^([^#;]+)[#;]*(.*)$"
-- Splits a string in IP and hostnames part, drops leading/trailing whitespace
local PATT_IP_HOST = "^%s*([%x%.%:]+)%s+(%S.-%S)%s*$"

local _DEFAULT_HOSTS = "/etc/hosts"              -- hosts filename to use when omitted
local _DEFAULT_RESOLV_CONF = "/etc/resolv.conf"  -- resolv.conf default filename

--- Default filename to parse for the `hosts` file.
-- @field DEFAULT_HOSTS Defaults to `/etc/hosts`
_M.DEFAULT_HOSTS = _DEFAULT_HOSTS

--- Default filename to parse for the `resolv.conf` file.
-- @field DEFAULT_RESOLV_CONF Defaults to `/etc/resolv.conf`
_M.DEFAULT_RESOLV_CONF = _DEFAULT_RESOLV_CONF

--- Parsing configuration files and variables
-- @section parsing

--- Parses a `hosts` file or table.
-- Does not check for correctness of ip addresses nor hostnames (hostnames will 
-- be forced to lowercase). Might return `nil + error` if the file cannot be read.
--
-- __NOTE__: All hostnames and aliases will be returned in lowercase.
-- @param filename (optional) Filename to parse, or a table with the file 
-- contents in lines (defaults to `'/etc/hosts'` if omitted)
-- @return 1; reverse lookup table, ip addresses (table with `ipv4` and `ipv6` 
-- fields) indexed by their canonical names and aliases
-- @return 2; list with all entries. Containing fields `ip`, `canonical` and `family`, 
-- and a list of aliasses
-- @usage local lookup, list = utils.parse_hosts({
--   "127.0.0.1   localhost",
--   "1.2.3.4     someserver",
--   "192.168.1.2 test.computer.com",
--   "192.168.1.3 ftp.COMPUTER.com alias1 alias2",
-- })
-- 
-- print(lookup["localhost"])         --> "127.0.0.1"
-- print(lookup["ftp.computer.com"])  --> "192.168.1.3" note: name in lowercase!
-- print(lookup["alias1"])            --> "192.168.1.3"
_M.parse_hosts = function(filename)
  local lines
  if type(filename) == "table" then
    lines = filename
  else
    local err
    lines, err = utils.readlines(filename or _M.DEFAULT_HOSTS)
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
        local family = ip:find(":",1, true) and "ipv6" or "ipv4"
        local entry = { ip = ip, family = family }
        local key = "canonical"
        for host in hosts:gmatch("%S+") do
          entry[key] = host
          key = (tonumber(key) or 0) + 1
          local rev = reverse[host]
          if not rev then
            rev = {}
            reverse[host] = rev
          end
          rev[family] = rev[family] or ip -- do not overwrite, first one wins
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

--- Parses a `resolv.conf` file or table.
-- Does not check for correctness of ip addresses nor hostnames, bad options 
-- will be ignored. Might return `nil + error` if the file cannot be read.
-- @param filename (optional) File to parse (defaults to `'/etc/resolv.conf'` if 
-- omitted) or a table with the file contents in lines.
-- @return a table with fields `nameserver` (table), `domain` (string), `search` (table), `sortlist` (table) and `options` (table)
-- @see apply_env
_M.parse_resolv_conf = function(filename)
  local lines
  if type(filename) == "table" then
    lines = filename
  else
    local err
    lines, err = utils.readlines(filename or _M.DEFAULT_RESOLV_CONF)
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

--- Will parse `LOCALDOMAIN` and `RES_OPTIONS` environment variables.
-- It will insert them into the given `resolv.conf` based configuration table.
--
-- __NOTE__: if the input is `nil+error` it will return the input, to allow for 
-- pass-through error handling
-- @param config Options table, as parsed by `parse_resolv_conf`, or an empty table to get only the environment options
-- @return modified table
-- @see parse_resolv_conf
-- @usage -- errors are passed through, so this;
-- local config, err = utils.parse_resolv_conf()
-- if config then 
--   config, err = utils.apply_env(config)
-- end
-- 
-- -- Is identical to;
-- local config, err = utils.apply_env(utils.parse_resolv_conf())
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

--- Caching configuration files and variables
-- @section caching

-- local caches
local cache_hosts  -- cached value
local cache_hostsr  -- cached value
local last_hosts = 0 -- timestamp
local ttl_hosts   -- time to live for cache

--- returns the `parse_hosts` results, but cached.
-- Once `ttl` has been provided, only after it expires the file will be parsed again.
--
-- __NOTE__: if cached, the _SAME_ tables will be returned, so do not modify them 
-- unless you know what you are doing!
-- @param ttl cache time-to-live in seconds (can be updated in following calls)
-- @return reverse and list tables, same as `parse_hosts`.
-- @see parse_hosts
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

--- returns the `apply_env` results, but cached.
-- Once `ttl` has been provided, only after it expires it will be parsed again.
--
-- __NOTE__: if cached, the _SAME_ table will be returned, so do not modify them 
-- unless you know what you are doing!
-- @param ttl cache time-to-live in seconds (can be updated in following calls)
-- @return configuration table, same as `parse_resolve_conf`.
-- @see parse_resolv_conf
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

--- Miscellaneous
-- @section miscellaneous

--- checks the hostname type; ipv4, ipv6, or name.
-- Type is determined by exclusion, not by validation. So if it returns `'ipv6'` then
-- it can only be an ipv6, but it is not necessarily a valid ipv6 address.
-- @param name the string to check (this may contain a port number)
-- @return string either; `'ipv4'`, `'ipv6'`, or `'name'`
-- @usage hostname_type("123.123.123.123")  -->  "ipv4"
-- hostname_type("127.0.0.1:8080")   -->  "ipv4"
-- hostname_type("::1")              -->  "ipv6"
-- hostname_type("[::1]:8000")       -->  "ipv6"
-- hostname_type("some::thing")      -->  "ipv6", but invalid...
_M.hostname_type = function(name)
  local remainder, colons = gsub(name, ":", "")
  if colons > 1 then return "ipv6" end
  if remainder:match("^[%d%.]+$") then return "ipv4" end
  return "name"
end

return _M