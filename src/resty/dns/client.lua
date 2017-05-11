--------------------------------------------------------------------------
-- DNS client.
--
-- Works with OpenResty only. Requires the [`lua-resty-dns`](https://github.com/openresty/lua-resty-dns) module.
-- 
-- _NOTES_: 
-- 
-- 1. parsing the config files upon initialization uses blocking i/o, so use with
-- care. See `init` for details.
-- 2. All returned records are directly from the cache. _So do not modify them!_ 
-- If you need to, copy them first.
-- 3. TTL for records is the TTL returned by the server at the time of fetching 
-- and won't be updated while the client serves the records from its cache.
-- 4. resolving IPv4 (A-type) and IPv6 (AAAA-type) addresses is explicitly supported. If
-- the hostname to be resolved is a valid IP address, it will be cached with a ttl of 
-- 10 years. So the user doesn't have to check for ip adresses.
--
-- @copyright 2016 Mashape Inc.
-- @author Thijs Schreijer
-- @license Apache 2.0

local utils = require("resty.dns.utils")
local fileexists = require("pl.path").exists
local semaphore = require("ngx.semaphore").new

local resolver = require("resty.dns.resolver")
local time = ngx.now
local ngx_log = ngx.log
local log_WARN = ngx.WARN

local math_min = math.min
local math_max = math.max
local math_fmod = math.fmod
local math_random = math.random
local table_remove = table.remove
local table_insert = table.insert
local table_concat = table.concat

local empty = setmetatable({}, 
  {__newindex = function() error("The 'empty' table is read-only") end})

-- resolver options
local config


local defined_hosts        -- hash table to lookup names originating from the hosts file
local emptyTtl             -- ttl (in seconds) for empty and 'name error' (3) errors
local badTtl               -- ttl (in seconds) for a other dns error results
local orderValids = {"LAST", "SRV", "A", "AAAA", "CNAME"} -- default order to query
for _,v in ipairs(orderValids) do orderValids[v:upper()] = v end

-- create module table
local _M = {}
-- copy resty based constants for record types
for k,v in pairs(resolver) do
  if type(k) == "string" and k:sub(1,5) == "TYPE_" then
    _M[k] = v
  end
end
-- insert our own special value for "last success"
_M.TYPE_LAST = -1

local function log(level, ...)
  return ngx_log(level, "[dns-client] ", ...)
end

-- ==============================================
--    In memory DNS cache
-- ==============================================

--- Caching.
-- The cache will not update the `ttl` field. So every time the same record
-- is served, the ttl will be the same. But the cache will insert extra fields
-- on the top-level; `touch` (timestamp of last access) and `expire` (expiry time
-- based on `ttl`)
-- @section caching


-- hostname cache indexed by "recordtype:hostname" returning address list.
-- Result is a list with entries. 
-- Keys only by "hostname" only contain the last succesfull lookup type 
-- for this name, see `resolve` function.
local cache = {}

-- lookup a single entry in the cache. Invalidates the entry if its beyond its ttl.
-- Even if the record is expired and `nil` is returned, the second return value
-- can be `true`.
-- @param qname name to lookup
-- @param qtype type number, any of the TYPE_xxx constants
-- @param peek just consult the cache, do not check ttl nor expire, just touch it
-- @return 1st; cached record or nil, 2nd; expect_ttl_0, true if the last one was ttl  0
local cachelookup = function(qname, qtype, peek)
  local now = time()
  local key = qtype..":"..qname
  local cached = cache[key]
  local expect_ttl_0
  
  if cached then
    expect_ttl_0 = ((cached[1] or empty).ttl == 0)
    if peek then
      -- cannot update, just update touch time
      cached.touch = now
    elseif expect_ttl_0 then
      -- ttl = 0 so we should not remove the cache entry, but we should also
      -- not return it
      cached.touch = now
      cached = nil
    elseif (cached.expire < now) then
      -- the cached entry expired, and we're allowed to mark it as such
      cache[key] = nil
      cached = nil
    else
      -- still valid, so nothing to do
      cached.touch = now
    end
  end
  
  return cached, expect_ttl_0
end

-- inserts an entry in the cache
-- Note: if the ttl=0, then it is also stored to enable 'cache-only' lookups
-- params qname, qtype are IGNORED unless `entry` has an empty list part.
local cacheinsert = function(entry, qname, qtype)

  local ttl, key
  local e1 = entry[1]
  if e1 then
    key = e1.type..":"..e1.name
    
    -- determine minimum ttl of all answer records
    ttl = e1.ttl
    for i = 2, #entry do
      ttl = math_min(ttl, entry[i].ttl)
    end
  elseif entry.errcode and entry.errcode ~= 3 then
    -- an error, but no 'name error' (3)
    ttl = badTtl
    key = qtype..":"..qname
  else
    -- empty or a 'name error' (3)
    ttl = emptyTtl
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
-- @param touched in seconds. Cleanup everything (also non-expired items) not 
-- touched in `touched` seconds. If omitted, only expired items (based on ttl) 
-- will be removed. 
-- @return number of entries deleted
_M.purgeCache = function(touched)
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

--- Resolving.
-- When resolving names, queries will be synchronized, such that only a single
-- query will be sent. Any other requests coming in while waiting for a 
-- response from the name server will be queued, and receive the same result
-- as the first request, once that returns.
-- The exception is when a `ttl=0` is expected (expectation
-- is based on a previous query returning `ttl=0`), in that case every request
-- will get its own name server query.
--
-- Because OpenResty will close sockets on boundaries of contexts, the 
-- resolver objects can only be reused in limited situations. To reuse
-- them see the `r` parameters of the `resolve` and `toip` functions. Applicable
-- for multiple consecutive calls in the same context.
--
-- The `dnsCacheOnly` parameter found with `resolve` and `toip` can be used in 
-- contexts where the co-socket api is unavailable. When the flag is set
-- only cached data is returned, which is possibly stale, but it will never
-- use blocking io. Also; the stale data will not
-- be invalidated from the cache when `dnsCacheOnly` is set.
--
-- __Housekeeping__; when using `toip` it has to do some housekeeping to apply
-- the (weighted) round-robin scheme. Those values will be stored in the 
-- dns record using field names starting with `__` (double underscores). So when
-- using `resolve` it might return a record from the cache with those fields if
-- it has been accessed by `toip` before.
-- @section resolving


local typeOrder
local poolMaxWait
local poolMaxRetry

--- Initialize the client. Can be called multiple times. When called again it 
-- will clear the cache.
-- @param options Same table as the [OpenResty dns resolver](https://github.com/openresty/lua-resty-dns), 
-- with some extra fields explained in the example below.
-- @return `true` on success, `nil+error`, or throw an error on bad input
-- @usage -- config files to parse
-- -- `hosts` and `resolvConf` can both be a filename, or a table with file-contents
-- -- The contents of the `hosts` file will be inserted in the cache.
-- -- From `resolv.conf` the `nameserver`, `search`, `ndots`, `attempts` and `timeout` values will be used.
-- local hosts = {}  -- initialize without any blocking i/o
-- local resolvConf = {}  -- initialize without any blocking i/o
--
-- -- when getting nameservers from `resolv.conf`, get ipv6 servers?
-- local enable_ipv6 = false
--
-- -- Order in which to try different dns record types when resolving
-- -- 'last'; will try the last previously successful type for a hostname.
-- local order = { "last", "SRV", "A", "AAAA", "CNAME" } 
--
-- -- Cache ttl for empty and 'name error' (3) responses
-- local emptyTtl = 30.0   -- in seconds (can have fractions)
--
-- -- Cache ttl for other error responses
-- local badTtl = 1.0   -- in seconds (can have fractions)
--
-- -- `ndots`, same as the `resolv.conf` option, if not given it is taken from
-- -- `resolv.conf` or otherwise set to 1
-- local ndots = 1
--
-- -- `search`, same as the `resolv.conf` option, if not given it is taken from
-- -- `resolv.conf`, or set to the `domain` option, or no search is performed
-- local search = {
--   "mydomain.com",
--   "site.domain.org",
-- }
-- 
-- assert(client.init({
--          hosts = hosts, 
--          resolvConf = resolvConf,
--          ndots = ndots,
--          search = search,
--          order = order,
--          badTtl = badTtl,
--          enable_ipv6 = enable_ipv6,
--        })
-- )
_M.init = function(options)
  
  local resolv, hosts, err
  options = options or {}
  cache = {}  -- clear cache on re-initialization
  defined_hosts = {}  -- reset hosts hash table
  
  local order = options.order or orderValids
  typeOrder = {} -- clear existing upvalue
  for i,v in ipairs(order) do 
    local t = v:upper()
    assert(orderValids[t], "Invalid dns record type in order array; "..tostring(v))
    typeOrder[i] = _M["TYPE_"..t]
  end
  assert(#typeOrder > 0, "Invalid order list; cannot be empty")
  
  
  -- Deal with the `hosts` file
  
  local hostsfile = options.hosts or utils.DEFAULT_HOSTS

  if ((type(hostsfile) == "string") and (fileexists(hostsfile)) or
     (type(hostsfile) == "table")) then
    hosts, err = utils.parseHosts(hostsfile)  -- results will be all lowercase!
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
      defined_hosts[name..":".._M.TYPE_A] = true 
    end
    if address.ipv6 then 
      cacheinsert({{  -- NOTE: nested list! cache is a list of lists
          name = name,
          address = address.ipv6,
          type = _M.TYPE_AAAA,
          class = 1,
          ttl = ttl,
        }})
      defined_hosts[name..":".._M.TYPE_AAAA] = true 
    end
  end


  -- Deal with the `resolv.conf` file

  local resolvconffile = options.resolvConf or utils.DEFAULT_RESOLV_CONF

  if ((type(resolvconffile) == "string") and (fileexists(resolvconffile)) or
     (type(resolvconffile) == "table")) then
    resolv, err = utils.applyEnv(utils.parseResolvConf(resolvconffile))
    if not resolv then return resolv, err end
  else
    log(log_WARN, "Resolv.conf file not found: "..tostring(resolvconffile))  
    resolv = {}
  end
  if not resolv.options then resolv.options = {} end

  if #(options.nameservers or {}) == 0 and resolv.nameserver then
    options.nameservers = {}
    -- some systems support port numbers in nameserver entries, so must parse those
    for _, address in ipairs(resolv.nameserver) do
      local ip, port, t = utils.parseHostname(address)
      if t == "ipv6" and not options.enable_ipv6 then
        -- should not add this one
      else
        if port then
          options.nameservers[#options.nameservers + 1] = { ip, port }
        else
          options.nameservers[#options.nameservers + 1] = ip
        end
      end
    end
  end
  assert(#(options.nameservers or {}) > 0, "Invalid configuration, no valid nameservers found")
  
  options.retrans = options.retrans or resolv.options.attempts or 5 -- 5 is openresty default
  
  if not options.timeout then
    if resolv.options.timeout then
      options.timeout = resolv.options.timeout * 1000
    else
      options.timeout = 2000  -- 2000 is openresty default
    end
  end

  -- setup the search order
  options.ndots = options.ndots or resolv.options.ndots or 1
  options.search = options.search or resolv.search or { resolv.domain }
  
  
  -- other options
  
  badTtl = options.badTtl or 1
  emptyTtl = options.emptyTtl or 30
  
  -- options.no_recurse = -- not touching this one for now
  
  config = options -- store it in our module level global
  
  poolMaxRetry = 1  -- do one retry, dns resolver is already doing 'retrans' number of retries on top
  poolMaxWait = options.timeout / 1000 * options.retrans -- default is to wait for the dns resolver to hit its timeouts
  
  return true
end

local queue = setmetatable({}, {__mode = "v"})
-- Performs a query, but only one at a time. While the query is waiting for a response, all
-- other queries for the same name+type combo will be yielded until the first one 
-- returns. All calls will then return the same response.
-- Reason; prevent a dog-pile effect when a dns record expires. Especially under load many dns 
-- queries would be fired at the dns server if we wouldn't do this.
-- The `poolMaxWait` is how long a thread waits for another to complete the query, after the timeout it will
-- clear the token in the cache and retry (all others will become in line after this new one)
-- The `poolMaxRetry` is how often we wait for another query to complete, after this number it will return
-- an error. A retry will be performed when a) waiting for the other thread times out, or b) when the
-- query by the other thread returns an error.
-- The maximum delay would be `poolMaxWait * poolMaxRetry`.
-- @return query result + nil + r, or nil + error + r
local function synchronizedQuery(qname, r_opts, r, expect_ttl_0, count)
  local key = qname..":"..r_opts.qtype
  local item = queue[key]
  if not item then
    -- no lookup being done so far
    if not r then
      local err
      r, err = resolver:new(config)
      if not r then
        return r, err, nil
      end
    end

    if expect_ttl_0 then
      -- we're not limiting the dns queries, but query on EVERY request
      local result, err = r:query(qname, r_opts)
      return result, err, r
    else
      -- we're limiting to one request at a time
      item = {
        semaphore = semaphore(),
      }
      queue[key] = item  -- insertion in queue; this is where the synchronization starts
      item.result, item.err = r:query(qname, r_opts)
      -- query done, but by now many others might be waiting for our result.
      -- 1) stop new ones from adding to our lock/semaphore
      queue[key] = nil
      -- 2) release all waiting threads
      item.semaphore:post(math_max(item.semaphore:count() * -1, 1))
      return item.result, item.err, r
    end
  else
    -- lookup is on its way, wait for it
    local ok, err = item.semaphore:wait(poolMaxWait)
    if ok and item.result then
      -- we were released, and have a query result from the
      -- other thread, so all is well, return it
      return item.result, item.err, r
    else
      -- there was an error, either a semaphore timeout, or 
      -- a lookup error, so retry (retry actually means; do 
      -- our own lookup instead of waiting for another lookup).
      count = count or 1
      if count > poolMaxRetry then
        return nil, "dns lookup pool exceeded retries ("..tostring(poolMaxRetry).."): "..(item.error or err or "unknown"), r
      end
      if queue[key] == item then queue[key] = nil end -- don't block on the same thread again
      return synchronizedQuery(qname, r_opts, r, expect_ttl_0, count + 1)
    end
  end
end

local msg_mt = {
  __tostring = function(self)
    return table_concat(self, "/")
  end
}

local try_list_mt = {
  __tostring = function(self)
    local l, i = {}, 0
    for _, entry in ipairs(self) do
      l[i+1] = entry.qname
      l[i+2] = ":"
      l[i+3] = entry.qtype
      local m = tostring(entry.msg)
      if m == "" then
        i = i + 4
      else
        l[i+4] = " - "
        l[i+5] = m
        i = i + 6
      end
      l[i]="\n"
    end
    return table_concat(l)
  end
}

-- adds a try to a list of tries.
-- The list keeps track of all queries tried so far. The array part lists the
-- order of attempts, whilst the `<qname>:<qtype>` key contains the index of that try.
-- @param self (optional) the list to add to, if omitted a new one will be created and returned
-- @param qname name being looked up
-- @param qtype query type being done
-- @param status (optional) message to be recorded
-- @return the list
local function try_add(self, qname, qtype, status)
  self = self or setmetatable({}, try_list_mt)
  local k = tostring(qname) .. ":" .. tostring(qtype)
  local i = #self + 1
  self[i] = {
    qname = qname,
    qtype = qtype,
    msg = setmetatable({ status }, msg_mt),
  }
  self[k] = i
  return self
end

-- adds a status to the last entry in the `msg` table.
local function try_status(self, status)
  local entry = self[#self]
  local msg = entry.msg
  msg[#msg + 1] = status
  return self
end

local function check_ipv6(qname, qtype, r, try_list)
  try_list = try_add(try_list, qname, qtype, "IPv6")

  -- check cache and always use "cacheonly" to not alter it as IP addresses are
  -- long lived in the cache anyway
  local record = cachelookup(qname, qtype, true)
  if record then
    try_status(try_list, "cached")
    return record, nil, r, try_list
  end

  local check = qname
  if check:sub(1,1) == ":" then check = "0"..check end
  if check:sub(-1,-1) == ":" then check = check.."0" end
  if check:find("::") then
    -- expand double colon
    local _, count = check:gsub(":","")
    local ins = ":"..string.rep("0:", 8 - count)
    check = check:gsub("::", ins, 1)  -- replace only 1 occurence!
  end
  if qtype == _M.TYPE_AAAA and
     check:match("^%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?$") then
    try_status(try_list, "validated")
    record = {{
      address = qname,
      type = _M.TYPE_AAAA,
      class = 1,
      name = qname,
      ttl = 10 * 365 * 24 * 60 * 60 -- TTL = 10 years
    }}
  else
    -- not a valid IPv6 address, or a bad type (non ipv6)
    -- return a "server error"
    try_status(try_list, "bad IPv6")
    record = {
      errcode = 3,
      errstr = "name error",
    }
  end
  cacheinsert(record, qname, qtype)
  return record, nil, r, try_list
end

local function check_ipv4(qname, qtype, r, try_list)
  try_list = try_add(try_list, qname, qtype, "IPv4")

  -- check cache and always use "cacheonly" to not alter it as IP addresses are
  -- long lived in the cache anyway
  local record = cachelookup(qname, qtype, true)
  if record then
    try_status(try_list, "cached")
    return record, nil, r, try_list
  end

  if qtype == _M.TYPE_A then
    try_status(try_list, "validated")
    record = {{
      address = qname,
      type = _M.TYPE_A,
      class = 1,
      name = qname,
      ttl = 10 * 365 * 24 * 60 * 60 -- TTL = 10 years
    }}
  else
    -- bad query type for this ipv4 address
    -- return a "server error"
    try_status(try_list, "bad IPv4")
    record = {
      errcode = 3,
      errstr = "name error",
    }
  end
  cacheinsert(record, qname, qtype)
  return record, nil, r, try_list
end

-- will lookup in the cache, or alternatively query dns servers and populate the cache.
-- only looks up the requested type.
-- It will always add an entry for the requested name in the `try_list`.
-- @return query result + nil + r + try_list, or nil + error + r + try_list
local function lookup(qname, r_opts, dnsCacheOnly, r, try_list)
  local qtype = r_opts.qtype
  
  local record, expect_ttl_0 = cachelookup(qname, qtype, dnsCacheOnly)
  if record then  -- cache hit
    try_list = try_add(try_list, qname, qtype, "cache hit")
    return record, nil, r, try_list
  end
  try_list = try_add(try_list, qname, qtype)
  if dnsCacheOnly then
    -- no active lookups allowed, so return error
    -- NOTE: this error response should never be cached, because it is caused 
    -- by the limited nginx context where we can't use sockets to do the lookup
    try_status(try_list, "cache only lookup failed")
    return {
      errcode = 4,                                         -- standard is "server failure"
      errstr = "server failure, cache only lookup failed", -- extended description
    }, nil, r, try_list
  end
  
  -- not found in our cache, so perform query on dns servers
  local answers, err
  answers, err, r = synchronizedQuery(qname, r_opts, r, expect_ttl_0)
--print("============================================================")
--print("Lookup: ",qname,":",r_opts.qtype)
--print("Error : ", tostring(err))
--print(require("pl.pretty").write(answers or {}))
  if not answers then
    try_status(try_list, tostring(err))
    return answers, err, r, try_list
  end

  -- check our answers and store them in the cache
  -- eg. A, AAAA, SRV records may be accompanied by CNAME records
  -- store them all, leaving only the requested type in so we can return that set
  local others = {}
  for i = #answers, 1, -1 do -- we're deleting entries, so reverse the traversal
    local answer = answers[i]
    if (answer.type ~= qtype) or (answer.name ~= qname) then
--print("removing: ",answer.type,":",answer.name, " (name or type mismatch)")
      local key = answer.type..":"..answer.name
      local lst = others[key]
      if not lst then
        lst = {}
        others[key] = lst
      end
      table_insert(lst, 1, answer)  -- pos 1: preserve order
      table_remove(answers, i)
    end
  end
  if next(others) then
    for _, lst in pairs(others) do
      -- only store if not already cached (this is only a 'by-product')
      if not cachelookup(lst[1].name, lst[1].type) then
        cacheinsert(lst)
      end
      -- set success-type, only if not set (this is only a 'by-product')
      if not cachegetsuccess(lst[1].name) then
        cachesetsuccess(lst[1].name, lst[1].type)
      end
    end
  end

  -- now insert actual target record in cache
  try_status(try_list, "queried")
  cacheinsert(answers, qname, qtype)
--print("------------------------------------------------------------")
--print(require("pl.pretty").write(answers or {}))
--print("============================================================")
  return answers, nil, r, try_list
end

-- iterator that iterates over all names and types to look up based on the
-- provided name, the `typeOrder`, `hosts`, `ndots` and `search` settings
-- @param qname the name to look up
-- @param qtype (optional) the type to look for, if omitted it will try the 
-- full `typeOrder` list
-- @return in order all the fully qualified names + types to look up
local function search_iter(qname, qtype)
  local _, dots = qname:gsub("%.", "")

  local type_list, type_start, type_end
  if qtype then
    type_list = { qtype }
    type_start = 0
  else
    type_list = typeOrder
    type_start = 0   -- just start at the beginning
  end
  type_end = #type_list
  
  local i_type = type_start
  local search = config.search
  local i_search, search_start, search_end
  local type_done = {}
  local type_current
  
  return  function()
            while true do
              -- advance the type-loop
              -- we need a while loop to make sure we skip LAST if already done
              while (not type_current) or type_done[type_current] do
                i_type = i_type + 1        -- advance type-loop
                if i_type > type_end then
                  return                   -- we reached the end, done iterating
                end

                type_current = type_list[i_type]
                if type_current == _M.TYPE_LAST then
                  type_current = cachegetsuccess(qname)
                end

                if type_current then
                  -- configure the search-loop
                  if (dots < config.ndots) and (not defined_hosts[qname..":"..type_current]) then
                    search_start = 0
                    search_end = #search + 1  -- +1: bare qname at the end
                  else
                    search_start = -1         -- -1: bare qname as first entry
                    search_end = #search
                  end
                  i_search = search_start    -- reset the search-loop
                end
              end

              -- advance the search-loop
              i_search = i_search + 1
              if i_search <= search_end then
                -- got the next one, return full search name and type
                local domain = search[i_search]
                return domain and qname.."."..domain or qname, type_current
              end

              -- finished the search-loop for this type, move to next type
              type_done[type_current] = true   -- mark current type as done
            end
          end
end

--- Resolve a name. 
-- If `r_opts.qtype` is given, then it will fetch that specific type only. If 
-- `r_opts.qtype` is not provided, then it will try to resolve
-- the name using the record types, in the order as provided to `init`.
-- 
-- Note that unless explictly requesting a CNAME record (by setting `r_opts.qtype`) this
-- function will dereference the CNAME records.
--
-- So requesting `my.domain.com` (assuming to be an AAAA record, and default `order`) will try to resolve
-- it (the first time) as;
--
-- - SRV, 
-- - then A, 
-- - then AAAA (success),
-- - then CNAME (after AAAA success, this will not be tried)
--
-- A second lookup will now try (assuming the cached entry expired);
--
-- - AAAA (as it was the last successful lookup),
-- - then SRV, 
-- - then A,
-- - then CNAME.
--
-- @function resolve
-- @param qname Name to resolve
-- @param r_opts Options table, see remark about the `qtype` field above and 
-- [OpenResty docs](https://github.com/openresty/lua-resty-dns) for more options.
-- @param dnsCacheOnly Only check the cache, won't do server lookups 
-- (will not invalidate any ttl expired data and will hence possibly return
-- expired data)
-- @param r (optional) dns resolver object to use, it will also be returned. 
-- In case of multiple calls, this allows to reuse the resolver object 
-- instead of recreating a new one on each call.
-- @param try_list (optional) list of tries to add to
-- @return `list of records + nil + r + try_list`, or `nil + err + r + try_list`.
local function resolve(qname, r_opts, dnsCacheOnly, r, try_list)

  qname = qname:lower()
  local qtype = (r_opts or empty).qtype
  local err, records
  local opts = {}
  if r_opts then
    for k,v in pairs(r_opts) do opts[k] = v end  -- copy the options table
  end
  
  -- check for qname being an ip address
  local name_type = utils.hostnameType(qname)
  if name_type ~= "name" then
    if name_type == "ipv4" then
      -- if no qtype is given, we're supposed to search, and hence we add TYPE_A as type
      records, err, r, try_list = check_ipv4(qname, qtype or _M.TYPE_A, r, try_list)
    else
      -- must be 'ipv6'
      -- if no qtype is given, we're supposed to search, and hence we add TYPE_AAAA as type
      records, err, r, try_list = check_ipv6(qname, qtype or _M.TYPE_AAAA, r, try_list)
    end
    if records.errcode then
      -- the query type didn't match the ip address, or a bad ip address
      return nil, 
             ("dns server error: %s %s"):format(records.errcode, records.errstr),
             r, try_list
    end
    -- valid ip
    return records, nil, r, try_list
  end

  -- go try a sequence of record types
  for try_name, try_type in search_iter(qname, qtype) do
    
    if try_list and try_list[try_name..":"..try_type] then
      -- recursion, been here before
      records = nil
      err = "recursion detected"
      -- insert an entry, error will be appended at the end of the search loop
      try_add(try_list, try_name, try_type)
    else
      -- go look it up
      opts.qtype = try_type
      records, err, r, try_list = lookup(try_name, opts, dnsCacheOnly, r, try_list)
    end
    
    if not records then
      -- nothing to do, an error
      -- fall through to the next entry in our search sequence
    elseif records.errcode then
      -- dns error: fall through to the next entry in our search sequence
      err = ("dns server error: %s %s"):format(records.errcode, records.errstr)
      records = nil
    elseif #records == 0 then
      -- empty: fall through to the next entry in our search sequence
      err = "dns server error: 3 name error" -- ensure same error message as "not found"
      records = nil
    else
      -- we got some records
      if not dnsCacheOnly and not qtype then
        -- only set the last succes, if we're not searching for a specific type
        -- and we're not limited by a cache-only request
        cachesetsuccess(qname, try_type) -- set last succesful type resolved
        cachesetsuccess(try_name, try_type) -- set last succesful type resolved
      end
      if try_type == _M.TYPE_CNAME then
        if try_type == qtype then
          -- a CNAME was explicitly requested, so no dereferencing
          return records, nil, r, try_list
        end
        -- dereference CNAME
        opts.qtype = nil
        try_status(try_list, "dereferencing")
        return resolve(records[1].cname, opts, dnsCacheOnly, r, try_list)
      end
      if qtype ~= _M.TYPE_SRV and try_type == _M.TYPE_SRV then
        -- check for recursive records, but NOT when requesting SRV explicitly
        local cnt = 0
        for _, record in ipairs(records) do
          if record.target == try_name then
            -- recursive record, pointing to itself
            cnt = cnt + 1
          end
        end
        if cnt == #records then
          -- fully recursive SRV record, specific Kubernetes problem
          -- which generates a SRV record for each host, pointing to 
          -- itself, hence causing a recursion loop.
          -- So we delete the record, set an error, so it falls through
          -- and retries other record types in the main loop here.
          records = nil
          err = "recursion detected"
        end
      end
      if records then
        return records, nil, r, try_list
      end
    end
    -- we had some error, record it in the status list
    try_status(try_list, err)
  end
  -- we failed, clear cache and return last error
  if not dnsCacheOnly then
    cachesetsuccess(qname, nil)
  end
  return nil, err, r, try_list
end

-- returns the index of the record next up in the round-robin scheme.
local function roundRobin(rec)
  local cursor = rec.__lastCursor or 0 -- start with first entry, trust the dns server! no random pick
  if cursor == #rec then
    cursor = 1
  else
    cursor = cursor + 1
  end
  rec.__lastCursor = cursor
  return cursor
end

-- greatest common divisor of 2 integers.
-- @return greatest common divisor
local function gcd(m, n)
  while m ~= 0 do
    m, n = math_fmod(n, m), m
  end
  return n
end

-- greatest common divisor of a list of integers.
-- @return 2 values; greatest common divisor for the whole list and
-- the sum of all weights
local function gcdl(list)
  local m = list[1]
  local n = list[2]
  if not n then return 1, m end
  local t = m
  local i = 2
  repeat
    t = t + n
    m = gcd(m, n)
    i = i + 1
    n = list[i]
  until not n
  return m, t
end

-- reduce a list of weights to their smallest relative counterparts.
-- eg. 20, 5, 5 --> 4, 1, 1 
-- @return 2 values; reduced list (index == original index) and
-- the sum of all the (reduced) weights
local function reducedWeights(list)
  local gcd, total = gcdl(list)
  local l = {}
  for i, val in  ipairs(list) do
    l[i] = val/gcd
  end
  return l, total/gcd
end

-- returns the index of the SRV entry next up in the weighted round-robin scheme.
local function roundRobinW(rec)
  
  -- determine priority; stick to current or lower priority
  local prioList = rec.__prioList -- list with indexes-to-entries having the lowest priority
  
  if not prioList then
    -- 1st time we're seeing this record, so go and
    -- find lowest priorities
    local topPriority = 999999
    local weightList -- weights for the entry
    local n = 0
    for i, r in ipairs(rec) do
      if r.priority == topPriority then
        n = n + 1
        prioList[n] = i
        weightList[n] = r.weight
      elseif r.priority < topPriority then
        n = 1
        topPriority = r.priority
        prioList = { i }
        weightList = { r.weight }
      end
    end
    rec.__prioList = prioList
    rec.__weightList = weightList
    return prioList[1]  -- start with first entry, trust the dns server!
  end

  local rrwList = rec.__rrwList
  local rrwPointer = rec.__rrwPointer

  if not rrwList then
    -- 2nd time we're seeing this record
    -- 1st time we trusted the dns server, now we do WRR by our selves, so
    -- must create a list based on the weights. We do this only when necessary
    -- for performance reasons, so only on 2nd or later calls. Especially for
    -- ttl=0 scenarios where there is only 1 call ever.
    local weightList = reducedWeights(rec.__weightList)
    rrwList = {}
    local x = 0
    -- create a list of entries, where each entry is repeated based on its
    -- relative weight.
    for i, idx in ipairs(prioList) do
      for _ = 1, weightList[i] do
        x = x + 1
        rrwList[x] = idx
      end
    end
    rec.__rrwList = rrwList
    -- The list has 2 parts, lower-part is yet to be used, higher-part was
    -- already used. The `rrwPointer` points to the last entry of the lower-part.
    -- On the initial call we served the first record, so we must rotate
    -- that initial call to be up-to-date.
    rrwList[1], rrwList[x] = rrwList[x], rrwList[1]
    rrwPointer = x-1  -- we have 1 entry in the higher-part now
    if rrwPointer == 0 then rrwPointer = x end
  end
  
  -- all structures are in place, so we can just serve the next up record
  local idx = math_random(1, rrwPointer)
  local target = rrwList[idx]
  
  -- rotate to next
  rrwList[idx], rrwList[rrwPointer] = rrwList[rrwPointer], rrwList[idx]
  if rrwPointer == 1 then 
    rec.__rrwPointer = #rrwList 
  else
    rec.__rrwPointer = rrwPointer-1
  end
  
  return target
end

--- Resolves to an IP and port number.
-- Builds on top of `resolve`, but will also further dereference SRV type records.
--
-- When calling multiple times on cached records, it will apply load-balancing
-- based on a round-robin (RR) scheme. For SRV records this will be a _weighted_ 
-- round-robin (WRR) scheme (because of the weights it will be randomized). It will 
-- apply the round-robin schemes on each level 
-- individually.
--
-- __Example__;
--
-- SRV record for "my.domain.com", containing 2 entries (this is the 1st level);
-- 
--   - `target = 127.0.0.1, port = 80, weight = 10`
--   - `target = "other.domain.com", port = 8080, weight = 5`
--
-- A record for "other.domain.com", containing 2 entries (this is the 2nd level);
--
--   - `ip = 127.0.0.2`
--   - `ip = 127.0.0.3`
--
-- Now calling `local ip, port = toip("my.domain.com", 123)` in a row 6 times will result in;
--
--   - `127.0.0.1, 80`
--   - `127.0.0.2, 8080` (port from SRV, 1st IP from A record)
--   - `127.0.0.1, 80`   (completes WRR 1st level, 1st run)
--   - `127.0.0.3, 8080` (port from SRV, 2nd IP from A record, completes RR 2nd level)
--   - `127.0.0.1, 80`
--   - `127.0.0.1, 80`   (completes WRR 1st level, 2nd run, with different order as WRR is randomized)
--
-- __Debugging__:
--
-- This function both takes and returns a `try_list`. This is an internal object
-- representing the entire resolution history for a call. To prevent unnecessary
-- string concatenations on a hot code path, it is not logged in this module.
-- If you need to log it, just log `tostring(try_list)` from the caller code.
-- @function toip
-- @param qname hostname to resolve
-- @param port (optional) default port number to return if none was found in 
-- the lookup chain (only SRV records carry port information, SRV with `port=0` will be ignored)
-- @param dnsCacheOnly Only check the cache, won't do server lookups (will 
-- not invalidate any ttl expired data and will hence possibly return expired data)
-- @param r (optional) dns resolver object to use, it will also be returned. 
-- In case of multiple calls, this allows to reuse the resolver object instead 
-- of recreating a new one on each call.
-- @param try_list (optional) list of tries to add to
-- @return `ip address + port + r + try_list`, or in case of an error `nil + error + r + try_list`
local function toip(qname, port, dnsCacheOnly, r, try_list)
  local rec, err
  rec, err, r, try_list = resolve(qname, nil, dnsCacheOnly, r, try_list)
  if err then
    return nil, err, r, try_list
  end

  if rec[1].type == _M.TYPE_SRV then
    local entry = rec[roundRobinW(rec)]
    -- our SRV entry might still contain a hostname, so recurse, with found port number
    local srvport = (entry.port ~= 0 and entry.port) or port -- discard port if it is 0
    try_status(try_list, "dereferencing SRV")
    return toip(entry.target, srvport, dnsCacheOnly, r, try_list)
  else
    -- must be A or AAAA
    return rec[roundRobin(rec)].address, port, r, try_list
  end
end


--- Socket functions
-- @section sockets

--- Implements tcp-connect method with dns resolution.
-- This builds on top of `toip`. If the name resolves to an SRV record,
-- the port returned by the DNS server will override the one provided.
--
-- __NOTE__: can also be used for other connect methods, eg. http/redis 
-- clients, as long as the argument order is the same
-- @function connect
-- @param sock the tcp socket
-- @param host hostname to connect to
-- @param port port to connect to (will be overridden if `toip` returns a port)
-- @param opts the options table
-- @return `success`, or `nil + error`
local function connect(sock, host, port, sock_opts)
  local targetIp, targetPort = toip(host, port)
  
  if not targetIp then
    return nil, targetPort 
  else
    -- need to do the extra check here: https://github.com/openresty/lua-nginx-module/issues/860
    if not sock_opts then
      return sock:connect(targetIp, targetPort)
    else
      return sock:connect(targetIp, targetPort, sock_opts)
    end
  end
end

--- Implements udp-setpeername method with dns resolution.
-- This builds on top of `toip`. If the name resolves to an SRV record,
-- the port returned by the DNS server will override the one provided.
-- @function setpeername
-- @param sock the udp socket
-- @param host hostname to connect to
-- @param port port to connect to (will be overridden if `toip` returns a port)
-- @return `success`, or `nil + error`
local function setpeername(sock, host, port)
  local targetIp, targetPort
  if host:sub(1,5) == "unix:" then
    targetIp = host  -- unix domain socket, nothing to resolve
  else
    targetIp, targetPort = toip(host, port)
    if not targetIp then
      return nil, targetPort
    end
  end
  return sock:connect(targetIp, targetPort)
end

-- export local functions
_M.resolve = resolve
_M.toip = toip
_M.connect = connect
_M.setpeername = setpeername

-- export the locals in case we're testing
if _TEST then 
  _M.getcache = function() return cache end 
  _M._search_iter = search_iter -- export as different name!
end 

return _M

