--------------------------------------------------------------------------
-- DNS client.
--
-- Works with OpenResty only. Requires the `resty.dns.resolver` module.
-- 
-- _NOTES_: 
-- 
-- 1. parsing the config files upon initialization uses blocking i/o, so use with
-- care. See `init()` for details.
-- 2. All returned records are directly from the cache. So do not modify them! If you
-- need to, copy them first.
-- 3. TTL for records is the TTL returned by the server at the time of fetching 
-- and won't be updated while the client serves the records from its cache.
-- 4. resolving IPv4 (A-type) and IPv6 (AAAA-type) addresses is explicitly supported. If
-- the hostname to be resolved is a valid IP address, it will be cached with a ttl of 
-- 10 years. So the user doesn't have to check for ip adresses.
--
-- See `./examples/` for examples and output returned.
--
-- @copyright 2016 Mashape Inc.
-- @author Thijs Schreijer
-- @license Apache 2.0

local utils = require("dns.utils")
local fileexists = require("pl.path").exists
local semaphore = require("ngx.semaphore").new

local resolver = require("resty.dns.resolver")
local time = ngx.now
local log = ngx.log
local log_WARN = ngx.WARN

-- resolver options
local config

-- recursion level before erroring out
local max_dns_recursion = 20
-- ttl (in seconds) for an empty dns result
local empty_ttl = 1

-- create module table
local _M = {}
-- copy resty based constants for record types
for k,v in pairs(resolver) do
  if type(k) == "string" and k:sub(1,5) == "TYPE_" then
    _M[k] = v
  end
end

-- ==============================================
--    In memory DNS cache
-- ==============================================

-- hostname cache indexed by "recordtype:hostname" returning address list.
-- Result is a list with entries. 
-- Keys only by "hostname" only contain the last succesfull lookup type 
-- for this name, see `resolve` function.
local cache = {}

-- lookup a single entry in the cache. Invalidates the entry if its beyond its ttl
-- @param qname name to lookup
-- @qtype type number, any of the TYPE_xxx constants
-- @peek just consult the cache, do not check ttl and expire, just touch it
local cachelookup = function(qname, qtype, peek)
  local now = time()
  local key = qtype..":"..qname
  local cached = cache[key]
  
  if cached then
    if (cached.expire < now) and (not peek) then
      -- the cached entry expired
      cache[key] = nil
      cached = nil
    else
      cached.touch = now
    end
  end
  
  return cached
end

-- inserts an entry in the cache
-- Note: if the ttl=0, then it is also stored to enable 'cache-only' lookups
-- params qname, qtype, qttl are IGNORED unless `entry` is an empty list
local cacheinsert = function(entry, qname, qtype)

  local ttl, key
  local e1 = entry[1]
  if e1 then
    key = e1.type..":"..e1.name
    
    -- determine minimum ttl of all answer records
    ttl = e1.ttl
    for i = 2, #entry do
      ttl = math.min(ttl, entry[i].ttl)
    end
  else
    -- list is empty, so no entries to grab data from
    ttl = empty_ttl
    key = qtype..":"..qname
  end
 
  -- set expire time
  local now = time()
  entry.touch = now
  entry.expire = now + ttl
  cache[key] = entry
end

-- Lookup the last succesful query type.
-- @param qname name to resolve
-- @return query/record type constant, or ˋnilˋ if not found
local function cachegetsuccess(qname)
  return cache[qname]
end

-- Sets the last succesful query type.
-- @qparam name resolved
-- @qtype query/record type to set, or ˋnilˋ to clear
-- @return ˋtrueˋ
local function cachesetsuccess(qname, qtype)
  cache[qname] = qtype
  return true
end

--- Cleanup the DNS client cache. Items will be checked on TTL only upon 
-- retrieval from the cache. So items inserted, but never used again will 
-- never be removed from the cache automatically. So unless you have a very 
-- restricted fixed set of hostnames you're resolving, you should occasionally 
-- purge the cache.
-- @param touched in seconds. Cleanup everything (also non-expired items) not touched in `touched` seconds. If omitted, only expired items (based on ttl) will be removed. 
-- @return number of entries deleted
_M.purge_cache = function(touched)
  local f
  if type(touched == nil) then 
    f = function(entry, now, count)  -- check ttl only
      if entry.expire < now then
        return nil, count + 1, true
      else
        return entry, count, false
      end
    end
  elseif type(touched) == "number" then
    f = function(entry, now, count)  -- check ttl and touch
      if (entry.expire < now) or (entry.touch + touched <= now) then
        return nil, count + 1, true
      else
        return entry, count, false
      end
    end
  else
    error("expected nil or number, got " ..type(touched), 2)
  end
  local now = time()
  local count = 0
  local deleted
  for key, entry in pairs(cache) do
    if type(entry) == "table" then
      cache[key], count, deleted = f(entry, now, count)
    else
      -- TODO: entry for record type, how to purge this???
    end
  end
  return count
end

-- ==============================================
--    Main DNS functions for lookup
-- ==============================================

local type_order = {
  _M.TYPE_A,
  _M.TYPE_AAAA,
  _M.TYPE_SRV,
  _M.TYPE_CNAME,
}
local pool_max_wait
local pool_max_retry

--- initialize resolver. When called multiple times, it will clear the cache.
-- Will parse hosts and resolv.conf files/tables.
-- If the `hosts` and `resolv_conf` fields are not provided, it will fall back on default
-- filenames (see the `dns.utils` module for details). To prevent any potential 
-- blocking i/o all together, manually fetch the contents of those files and 
-- provide them as tables. Or provide both fields as empty tables.
-- @param options Same table as the openresty dns resolver, with extra fields `hosts`, `resolv_conf` containing the filenames to parse.
-- @return true on success, nil+error otherwise
-- @usage -- initialize without any blocking i/o
-- local client = require("dns.client")
-- assert(client.init({
--          hosts = {}, 
--          resolv_conf = {},
--        })
-- )
_M.init = function(options, secondary)
  if options == _M then options = secondary end -- in case of colon notation call
  
  local resolv, hosts, err
  options = options or {}
  cache = {}  -- clear cache on re-initialization
  
  local hostsfile = options.hosts or utils.DEFAULT_HOSTS
  local resolvconffile = options.resolv_conf or utils.DEFAULT_RESOLV_CONF

  if ((type(hostsfile) == "string") and (fileexists(hostsfile)) or
     (type(hostsfile) == "table")) then
    hosts, err = utils.parse_hosts(hostsfile)  -- results will be all lowercase!
    if not hosts then return hosts, err end
  else
    log(log_WARN, "Hosts file not found: "..tostring(hostsfile))  
    hosts = {}
  end
  
  -- Populate the DNS cache with the hosts (and aliasses) from the hosts file.
  local ttl = 10*365*24*60*60  -- use ttl of 10 years for hostfile entries
  for name, address in pairs(hosts) do
    if address.ipv4 then 
      cacheinsert({{  -- NOTE: nested list! cache is a list of lists
          name = name,
          address = address.ipv4,
          type = _M.TYPE_A,
          class = 1,
          ttl = ttl,
        }})
    end
    if address.ipv6 then 
      cacheinsert({{  -- NOTE: nested list! cache is a list of lists
          name = name,
          address = address.ipv6,
          type = _M.TYPE_AAAA,
          class = 1,
          ttl = ttl,
        }})
    end
  end

  if ((type(resolvconffile) == "string") and (fileexists(resolvconffile)) or
     (type(resolvconffile) == "table")) then
    resolv, err = utils.apply_env(utils.parse_resolv_conf(resolvconffile))
    if not resolv then return resolv, err end
  else
    log(log_WARN, "Resolv.conf file not found: "..tostring(resolvconffile))  
    resolv = {}
  end

  if #(options.nameservers or {}) == 0 and resolv.nameserver then
    options.nameservers = {}
    -- some systems support port numbers in nameserver entries, so must parse those
    for i, address in ipairs(resolv.nameserver) do
      local ip, port = address:match("^([^:]+)%:*(%d*)$")
      port = tonumber(port)
      if port then
        options.nameservers[i] = { ip, port }
      else
        options.nameservers[i] = ip
      end
    end
  end
  assert(#(options.nameservers or {}) > 0, "Invalid configuration, no dns servers found")
  
  options.retrans = options.retrans or resolv.attempts or 5 -- 5 is openresty default
  
  if not options.timeout then
    if resolv.timeout then
      options.timeout = resolv.timeout * 1000
    else
      options.timeout = 2000  -- 2000 is openresty default
    end
  end
  
  -- options.no_recurse = -- not touching this one for now
  
  config = options -- store it in our module level global
  
  pool_max_retry = 1  -- do one retry, dns resolver is already doing 'retrans' number of retries on top
  pool_max_wait = options.timeout / 1000 * options.retrans 
  
  return true
end

local _queue = setmetatable({}, {__mode = "v"})
-- Performs a query, but only one at a time. While the query is waiting for a response, all
-- other queries for the same name+type combo will be yielded until the first one 
-- returns. All calls will then return the same response.
-- Reason; prevent a dog-pile effect when a dns record expires. Especially under load many dns 
-- queries would be fired at the dns server if we wouldn't do this.
-- The `max_wait` is how long a thread waits for another to complete the query, after the timeout it will
-- clear the token in the cache and retry (all others will become in line after this new one)
-- The `max_retry` is how often we wait for another query to complete, after this number it will return
-- an error. A retry will be performed when a) waiting for the other thread times out, or b) when the
-- query by the other thread returns an error.
-- The maximum delay would be `max_wait * max_retry`.
-- @return query result + nil + r, or nil + error + r
local function _synchronized_query(qname, r_opts, r, count)
  local key = qname..":"..r_opts.qtype
  local item = _queue[key]
  if not item then
    -- no lookup being done so far
    if not r then
      local err
      r, err = resolver:new(config)
      if not r then
        return r, err, nil
      end

    end
    item = {
      semaphore = semaphore(),
    }
    _queue[key] = item  -- insertion in _queue; this is where the synchronization starts
    item.result, item.err = r:query(qname, r_opts)
    -- query done, but by now many others might be waiting for our result.
    -- 1) stop new ones from adding to our lock/semaphore
    _queue[key] = nil
    -- 2) release all waiting threads
    item.semaphore:post(math.max(item.semaphore:count() * -1, 1))
    return item.result, item.err, r
  else
    -- lookup is on its way, wait for it
    local ok, err = item.semaphore:wait(pool_max_wait)
    if ok and item.result then
      -- we were released, and have a query result from the
      -- other thread, so all is well, return it
      return item.result, item.err, r
    else
      -- there was an error, either a semaphore timeout, or 
      -- a lookup error, so retry (retry actually means; do 
      -- our own lookup instead of waiting for another lookup).
      count = count or 1
      if count > pool_max_retry then
        return r, nil, "dns lookup pool exceeded retries ("..tostring(pool_max_retry).."): "..(item.error or err or "unknown")
      end
      _queue[key] = nil  -- don't block on the same thread again
      return _synchronized_query(qname, r_opts, r, count + 1)
    end
  end
end

local function check_ipv6(qname)
  local check = qname
  if check:sub(1,1) == ":" then check = "0"..check end
  if check:sub(-1,-1) == ":" then check = check.."0" end
  if check:find("::") then
    -- expand double colon
    local _, count = check:gsub(":","")
    local ins = ":"..string.rep("0:", 8 - count)
    check = check:gsub("::", ins, 1)  -- replace only 1 occurence!
  end
  if not check:match("^%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?$") then
    -- not a valid IPv6 address
    -- return a "server error" as a bad IPv4 would be looked up on 
    -- the server and return a server error as well, for consistency.
    return {
      errcode = 3,
      errstr = "name error",
    }
  end
  local record = {{
    address = qname,
    type = _M.TYPE_AAAA,
    class = 1,
    name = qname,
    ttl = 10 * 365 * 24 * 60 * 60 -- TTL = 10 years
  }}
  cacheinsert(record)
  return record
end

local function check_ipv4(qname)
  local record = {{
    address = qname,
    type = _M.TYPE_A,
    class = 1,
    name = qname,
    ttl = 10 * 365 * 24 * 60 * 60 -- TTL = 10 years
  }}
  cacheinsert(record)
  return record
end

-- will lookup in the cache, or alternatively query dns servers and populate the cache.
-- only looks up the requested type.
-- @return query result + nil + r, or r + nil + error
local function _lookup(qname, r_opts, dns_cache_only, r)
  local qtype = r_opts.qtype
  local record = cachelookup(qname, qtype, dns_cache_only)
  if record then
    return record                  -- cache hit
  elseif (qtype == _M.TYPE_AAAA) and qname:find(":") then
    return check_ipv6(qname)       -- IPv6 or invalid
  elseif (qtype == _M.TYPE_A) and qname:match("^%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?$") then
    return check_ipv4(qname)       -- IPv4 address
  elseif dns_cache_only then
    -- no active lookups allowed, so return error
    return {
      errcode = 4,                                         -- standard is "server failure"
      errstr = "server failure, cache only lookup failed", -- extended description
    }, nil, r
  else
    -- not found in our cache, so perform query on dns servers
    local answers, err
    answers, err, r = _synchronized_query(qname, r_opts, r)
    if not answers then return answers, err, r end
    
    -- check our answers and store them in the cache
    -- A, AAAA, SRV records may be accompanied by CNAME records
    -- store them all, leaving only the requested type in so we can return that set
    for i = #answers, 1, -1 do -- we're deleting entries, so reverse the traversal
      local answer = answers[i]
      if answer.type ~= qtype then
        cacheinsert({answer}) -- insert in cache before removing it
        table.remove(answers, i)
      end
    end

    -- now insert actual target record in cache
    cacheinsert(answers, qname, qtype)
    return answers, nil, r
  end
end

--- Resolve a name. 
-- If `r_opts.qtype` is given, then it will fetch that specific type only (if it's a `TYPE_CNAME` then 
-- it will return the cname record). If `r_opts.qtype` is not provided, then it will try to resolve
-- the name using the following record types, in the order listed;
-- 
-- 1. last succesful lookup type (if any), 
-- 2. A-record, 
-- 3. AAAA-record, 
-- 4. SRV-record,  --> will be returned, if found, will not be dereferenced
-- 5. CNAME-record --> will not be returned, but dereferenced, so its target will be returned
--
-- So requesting `mysrv.domain.com` (assuming to be an SRV record) will try to resolve
-- it (the first time) as A, then AAAA, then SRV, CNAME will not be tried. If succesful, a second lookup 
-- will now try SRV, A, AAAA, CNAME.
-- @function resolve
-- @param qname Name to resolve
-- @param r_opts Options table, see remark about the `qtype` field above
-- @param dns_cache_only Only check the cache, won't do server lookups (will not invalidate any ttl expired data and will possibly return expired data)
-- @param r (optional) dns resolver object to use
-- @return `list of records + nil + r`, or `nil + err + r`. The list can be empty if the name is present on the server, but has a different record type. Any dns server errors are returned in a hashtable (see openresty docs).
local function resolve(qname, r_opts, dns_cache_only, r, count)
  if count and (count > max_dns_recursion) then
    return nil, "maximum dns recursion level reached", r
  end
  qname = qname:lower()
  local opts
  
  if r_opts then
    if r_opts.qtype then
      -- type was provided, just resolve it and return
      return _lookup(qname, r_opts, dns_cache_only, r)
    end
    -- options table, but no type, preserve options given
    opts = {}
    for k,v in pairs(r_opts) do
      opts[k] = v
    end
  else
    opts = {}
  end
  
  -- go try a sequence of record types
  local last = cachegetsuccess(qname)  -- check if we have a previous succesful one
  local records, err
  for i = (last and 0 or 1), #type_order do
    local qtype = (i == 0) and last or type_order[i]
    if (qtype == last) and (i ~= 0) then
      -- already tried this one, based on 'last', no use in trying again
    else
      opts.qtype = qtype
      records, err, r = _lookup(qname, opts, dns_cache_only, r)
      -- NOTE: if the name exists, but the type doesn't match, we get 
      -- an empty table. Hence check the length!
      if records and (#records > 0) then
        if not dns_cache_only then 
          cachesetsuccess(qname, qtype) -- set last succesful type resolved
        end
        if qtype ~= _M.TYPE_CNAME then
          return records, nil, r
        else
          -- dereference CNAME
          opts.qtype = nil
          return resolve(records[1].cname, opts, dns_cache_only, r, (count and count+1 or 1))
        end
      end
    end
  end
  -- we failed, clear cache and return last error
  if not dns_cache_only then
    cachesetsuccess(qname, nil)
  end
  return records, err, r
end

--- Standardizes the `resolve` output to more standard Lua errors.
-- Both `nil+error+r` and succesful lookups are passed through.
-- A server error table is returned as `nil+error+r` (where `error` is a string extracted from the server error table).
-- An empty response is returned as `response+error+r` (where `error` is 'dns query returned no results').
-- @function stdError
-- @return a valid (non-empty) query result + nil + r, or nil + error + r
-- @usage
-- local result, err, r = client.stdError(client.resolve("my.hostname.com"))
-- 
-- if err then error(err) end         --> only passes if there is at least 1 result returned
-- if not result then error(err) end  --> does not error on an empty result table
local function stdError(result, err, r)
  if not result then return result, err, r end
  assert(type(result) == "table", "Expected table or nil")
  if result.errcode then return nil, ("dns server error; %s %s"):format(result.errcode, result.errstr), r end
  if #result == 0 then return result, "dns query returned no results", r end
  return result, nil, r
end

--- Resolves to an IP and port number.
-- Does a round-robin over the returned records. Builds on top of `resolve`, but will also further 
-- dereference SRV type records. Will round-robin on each level individually. Eg.
-- SRV with 2 entries; a) IPv4 address, b) hostname to an A record with also 2 entries, b1 and b2.
-- Calling `toip` 4 times will in turn result in; 1) a, 2) b1, 3) a, 4) b2. 
-- @function toip
-- @param qname hostname to resolve
-- @param port (optional) default port number to return if none was found in the lookup chain
-- @param dns_cache_only (optional) if truthy, no dns queries will be performed, only cache lookups
-- @param r (optional) dns resolver object to use
-- @return `ip address + port + r`, or `nil + error + r`
local function toip(qname, port, dns_cache_only, r)
  local rec, err
  rec, err, r = stdError(resolve(qname, nil, dns_cache_only, r))
  if err then
    return nil, err, r
  end

  local cursor = rec.last_cursor
  if rec[1].type == _M.TYPE_SRV then
    -- determine priority; stick to current or lower priority
    local last_prio
    if not cursor then
      -- new record, find lowest priority
      last_prio = rec[1].priority
      for _, r in ipairs(rec) do
        last_prio = math.min(last_prio, r.priority)
      end
      cursor = math.random(1, #rec)  -- initially randomize the cursor we're using to traverse
    else
      last_prio = rec[cursor].priority
    end
    -- find record
    repeat
      if cursor == #rec then
        cursor = 1
      else
        cursor = cursor + 1
      end
    until rec[cursor].priority <= last_prio
    rec.last_cursor = cursor
    -- our SRV might still contain a hostname, so recurse, with found port number
    return toip(rec[cursor].target, rec[cursor].port, dns_cache_only, r)
  else
    -- must be A or AAAA
    -- find next-up record
    if cursor then
      if cursor == #rec then
        cursor = 1
      else
        cursor = cursor + 1
      end
    else
      cursor = math.random(1, #rec)  -- initially randomize the cursor we're using to traverse
    end
    rec.last_cursor = cursor
    return rec[cursor].address, port, r
  end
end

--- Implements tcp-connect method with dns resolution.
-- This builds on top of `toip`. If the name resolves to an SRV record, 
-- the port returned by the DNS server will override the one provided.
-- __NOTE__: can also be used for other connect methods; http/redis as long as
-- the argument order is the same
-- @function connect
-- @param sock the socket to connect
-- @param host hostname to connect to
-- @param port port to connect to
-- @param opts the options table
-- @return success, or nil + error
local function connect(sock, host, port, sock_opts)
  local target_ip, target_port = toip(host, port)
  
  if not target_ip then 
    return nil, target_port 
  else
    -- need to do the extra check here: https://github.com/openresty/lua-nginx-module/issues/860
    if not sock_opts then
      return sock:connect(target_ip, target_port)
    else
      return sock:connect(target_ip, target_port, sock_opts)
    end
  end
end

-- export local functions
_M.resolve = resolve
_M.toip = toip
_M.stdError = stdError
_M.connect = connect

-- export the local cache in case we're testing
if _TEST then 
  _M.getcache = function() return cache end 
end 

return _M

