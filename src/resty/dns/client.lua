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
local log_DEBUG = ngx.DEBUG

local math_min = math.min
local math_max = math.max
local math_fmod = math.fmod
local math_random = math.random
local table_remove = table.remove
local table_insert = table.insert
local string_lower = string.lower

local empty = setmetatable({}, 
  {__newindex = function() error("The 'empty' table is read-only") end})

-- resolver options
local config

-- recursion level before erroring out
local maxDnsRecursion = 20
-- ttl (in seconds) for an empty/error dns result
local badTtl = 1
-- default order to query
local orderValids = {"LAST", "SRV", "A", "AAAA", "CNAME"}
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
  else
    -- list-part is empty, so no entries to grab data from
    -- (this is an empty response, or an error response)
    ttl = badTtl
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
-- -- From `resolve_conf` the `nameservers`, `attempts` and `timeout` values will be used.
-- local hosts = {}  -- initialize without any blocking i/o
-- local resolvConf = {}  -- initialize without any blocking i/o
--
-- -- Order in which to try different dns record types when resolving
-- -- 'last'; will try the last previously successful type for a hostname.
-- local order = { "last", "SRV", "A", "AAAA", "CNAME" } 
--
-- -- Cache ttl for empty and error responses
-- local badTtl = 1.0   -- in seconds (can have fractions)
--
-- assert(client.init({
--          hosts = hosts, 
--          resolvConf = resolvConf,
--          order = order,
--          badTtl = badTtl,
--        })
-- )
_M.init = function(options)

  log(log_DEBUG, "(re)configuring dns client")
  local resolv, hosts, err
  options = options or {}
  cache = {}  -- clear cache on re-initialization
  
  local order = options.order or orderValids
  typeOrder = {} -- clear existing upvalue
  for i,v in ipairs(order) do 
    local t = v:upper()
    assert(orderValids[t], "Invalid dns record type in order array; "..tostring(v))
    typeOrder[i] = _M["TYPE_"..t]
  end
  assert(#typeOrder > 0, "Invalid order list; cannot be empty")
  log(log_DEBUG, "query order : ", table.concat(order,", "))
  
  local hostsfile = options.hosts or utils.DEFAULT_HOSTS
  local resolvconffile = options.resolvConf or utils.DEFAULT_RESOLV_CONF

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
      log(log_DEBUG, "adding from 'hosts' file: ",name, " = ", address.ipv4)
    end
    if address.ipv6 then 
      cacheinsert({{  -- NOTE: nested list! cache is a list of lists
          name = name,
          address = address.ipv6,
          type = _M.TYPE_AAAA,
          class = 1,
          ttl = ttl,
        }})
      log(log_DEBUG, "adding from 'hosts' file: ",name, " = ", address.ipv6)
    end
  end

  if ((type(resolvconffile) == "string") and (fileexists(resolvconffile)) or
     (type(resolvconffile) == "table")) then
    resolv, err = utils.applyEnv(utils.parseResolvConf(resolvconffile))
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
        options.nameservers[#options.nameservers + 1] = { ip, port }
      else
        options.nameservers[#options.nameservers + 1] = ip
      end
    end
  end
  assert(#(options.nameservers or {}) > 0, "Invalid configuration, no dns servers found")
  for _, r in ipairs(options.nameservers) do
    log(log_DEBUG, "nameserver ", type(r) == "table" and (r[1]..":"..r[2]) or r)
  end

  options.retrans = options.retrans or resolv.attempts or 5 -- 5 is openresty default
  log(log_DEBUG, "attempts ", options.retrans)

  if not options.timeout then
    if resolv.timeout then
      options.timeout = resolv.timeout * 1000
    else
      options.timeout = 2000  -- 2000 is openresty default
    end
  end
  log(log_DEBUG, "timeout ", options.timeout, "ms")
  
  badTtl = options.badTtl or 1
  log(log_DEBUG, "badTtl ", badTtl, "s")
  
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

local function check_ipv6(qname, qtype, r)
  local check = qname
  if check:sub(1,1) == ":" then check = "0"..check end
  if check:sub(-1,-1) == ":" then check = check.."0" end
  if check:find("::") then
    -- expand double colon
    local _, count = check:gsub(":","")
    local ins = ":"..string.rep("0:", 8 - count)
    check = check:gsub("::", ins, 1)  -- replace only 1 occurence!
  end
  local record
  if (not qtype == _M.TYPE_AAAA) or
     (not check:match("^%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?:%x%x?%x?%x?$")) then
    -- not a valid IPv6 address, or a bad type (non ipv6)
    -- return a "server error"
    log(log_DEBUG, "bad ipv6 query: '", qname, "' with type ", qtype)
    record = {
      errcode = 3,
      errstr = "name error",
    }
  else
    log(log_DEBUG, "ipv6 query: '", qname, "' with type ", qtype)
    record = {{
      address = qname,
      type = _M.TYPE_AAAA,
      class = 1,
      name = qname,
      ttl = 10 * 365 * 24 * 60 * 60 -- TTL = 10 years
    }}
  end
  cacheinsert(record, qname, qtype)
  return record, nil, r
end

local function check_ipv4(qname, qtype, r)
  local record
  if qtype == _M.TYPE_A then
    log(log_DEBUG, "ipv4 query: '", qname, "' with type ", qtype)
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
    log(log_DEBUG, "bad ipv4 query: '", qname, "' with type ", qtype)
    record = {
      errcode = 3,
      errstr = "name error",
    }
  end
  cacheinsert(record, qname, qtype)
  return record, nil, r
end

-- will lookup in the cache, or alternatively query dns servers and populate the cache.
-- only looks up the requested type.
-- @return query result + nil + r, or r + nil + error
local function lookup(qname, r_opts, dnsCacheOnly, r)
  local qtype = r_opts.qtype
  local record, expect_ttl_0 = cachelookup(qname, qtype, dnsCacheOnly)
  if record then  -- cache hit
--    log(log_DEBUG, "cache-hit while querying '", qname, "' for type ", qtype)
    return record, nil, r
  end
  if not expect_ttl_0 then -- so no record, and no expected ttl, so not seen
    -- this one recently, this is the only time we do the expensive checks
    -- for ip addresses, as they will be inserted with a ttl of 10years and 
    -- hence never hit this code branch again
    if qname:find(":") then
      return check_ipv6(qname, qtype, r)    -- IPv6 or invalid
    end
    if qname:match("^%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?$") then
      return check_ipv4(qname, qtype, r)    -- IPv4 address
    end
--  else
--    log(log_DEBUG, "expecting ttl=0: '", qname, "' for type ", qtype)
  end
  if dnsCacheOnly then
    -- no active lookups allowed, so return error
    -- NOTE: this error response should never be cached, because it is caused 
    -- by the limited nginx context where we can't use sockets to do the lookup
    log(log_DEBUG, "cache only lookup failed: '", qname, "' for type ", qtype)
    return {
      errcode = 4,                                         -- standard is "server failure"
      errstr = "server failure, cache only lookup failed", -- extended description
    }, nil, r
  end
  
  -- not found in our cache, so perform query on dns servers
  local t_start = time()
  local answers, err
  answers, err, r = synchronizedQuery(qname, r_opts, r, expect_ttl_0)
  log(log_DEBUG, "querying '", qname, "' for type ", qtype, " took ", (time() - t_start) * 1000)
  if not answers then
    log(log_DEBUG, "querying: '", qname, "' for type ", qtype, " error: ", err)
    return answers, err, r
  end

  -- check our answers and store them in the cache
  -- eg. A, AAAA, SRV records may be accompanied by CNAME records
  -- store them all, leaving only the requested type in so we can return that set
  local others = {}
  for i = #answers, 1, -1 do -- we're deleting entries, so reverse the traversal
    local answer = answers[i]

    -- remove case sensitiveness
    answer.name = string_lower(answer.name)

    -- validate type and name
    if (answer.type ~= qtype) or (answer.name ~= qname) then
      log(log_DEBUG, "removing: ",answer.type,":",answer.name, " (name or type mismatch)")
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
  log(log_DEBUG, "querying '", qname, "' for type ", qtype,
    ": entries ", #answers, " error ", answers.errcode, " ", answers.errstr)
  cacheinsert(answers, qname, qtype)
  return answers, nil, r
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
-- @return `list of records + nil + r`, or `nil + err + r`. The list can be empty if 
-- the name is present on the server, but has a different record type. Any 
-- dns server errors are returned in a hashtable (see 
-- [OpenResty docs](https://github.com/openresty/lua-resty-dns)).
local function resolve(qname, r_opts, dnsCacheOnly, r, count)
  if count and (count > maxDnsRecursion) then
    return nil, "maximum dns recursion level reached", r
  end
  qname = string_lower(qname)
  local opts
  
  if r_opts then
    if r_opts.qtype then
      -- type was provided, just resolve it and return
      return lookup(qname, r_opts, dnsCacheOnly, r)
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
  local alreadyTried = { [_M.TYPE_LAST] = true }
  for _, qtype in ipairs(typeOrder) do
    if (qtype == _M.TYPE_LAST) and last then
      qtype = last
    end
    if alreadyTried[qtype] then
      -- already tried this one, based on 'last', no use in trying again
    else
      opts.qtype = qtype
      alreadyTried[qtype] = true
      
      records, err, r = lookup(qname, opts, dnsCacheOnly, r)
      -- NOTE: if the name exists, but the type doesn't match, we get 
      -- an empty table. Hence check the length!
      if records and (#records > 0) then
        if not dnsCacheOnly then
          cachesetsuccess(qname, qtype) -- set last succesful type resolved
        end
        if qtype == _M.TYPE_CNAME then
          -- dereference CNAME
          opts.qtype = nil
          return resolve(records[1].cname, opts, dnsCacheOnly, r, (count and count+1 or 1))
        end
        if qtype == _M.TYPE_SRV then
          -- check for recursive records
          local cnt = 0
          for _, record in ipairs(records) do
            if record.target == qname then
              -- recursive record, pointing to itself
              cnt = cnt + 1
            end
            if cnt == #records then
              -- fully recursive SRV record, specific Kubernetes problem
              -- which generates a SRV record for each host, pointing to 
              -- itself, hence causing a recursion loop.
              -- So we delete the record, set an error, so it falls through
              -- and retries other record types in the main loop here.
              records = nil
              err = "recursive SRV record"
            end
          end
        end
        if records and qtype ~= _M.TYPE_CNAME then
          return records, nil, r
        end
      end
    end
  end
  -- we failed, clear cache and return last error
  if not dnsCacheOnly then
    cachesetsuccess(qname, nil)
  end
  return records, err, r
end

--- Standardizes the `resolve` output to more standard Lua errors.
-- Both `nil+error+r` and successful lookups are passed through.
-- A server error table is returned as `nil+error+r` (where `error` is a string 
-- extracted from the server error table).
-- An empty response is returned as `response+error+r` (where `error` is 
-- 'dns query returned no results').
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
-- @function toip
-- @param qname hostname to resolve
-- @param port (optional) default port number to return if none was found in 
-- the lookup chain (only SRV records carry port information, SRV with `port=0` will be ignored)
-- @param dnsCacheOnly Only check the cache, won't do server lookups (will 
-- not invalidate any ttl expired data and will hence possibly return expired data)
-- @param r (optional) dns resolver object to use, it will also be returned. 
-- In case of multiple calls, this allows to reuse the resolver object instead 
-- of recreating a new one on each call.
-- @return `ip address + port + r`, or in case of an error `nil + error + r`
local function toip(qname, port, dnsCacheOnly, r)
  local rec, err
  rec, err, r = stdError(resolve(qname, nil, dnsCacheOnly, r))
  if err then
    return nil, err, r
  end

  if rec[1].type == _M.TYPE_SRV then
    local entry = rec[roundRobinW(rec)]
    -- our SRV entry might still contain a hostname, so recurse, with found port number
    local srvport = (entry.port ~= 0 and entry.port) or port -- discard port if it is 0
    return toip(entry.target, srvport, dnsCacheOnly, r)
  else
    -- must be A or AAAA
    return rec[roundRobin(rec)].address, port, r
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
_M.stdError = stdError
_M.connect = connect
_M.setpeername = setpeername

-- export the local cache in case we're testing
if _TEST then 
  _M.getcache = function() return cache end 
end 

return _M

