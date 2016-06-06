--------------------------------------------------------------------------
-- DNS client.
--
-- The DNS client will cache entries and DNS resolver objects.
--
-- Requires the `resolver` module. Either `resty.dns.resolver` when used with OpenResty
-- or the extracted version of that module for regular Lua.
-- 
-- _NOTES_: 
-- 
-- 1. parsing the config files upon initialization uses blocking i/o, so use with
-- care. See `init()` for details.
-- 2. All returned records are directly from the cache. So do not modify them! If you
-- need to, copy them first.
-- 3. TTL for records is the TTL returned by the server at the time of fetching 
-- and won't be updated while the client serves the records from its cache.
--
-- See `./examples/` for examples and output returned.
--
-- @copyright Thijs Schreijer, Mashape Inc.
-- @license Apache 2.0

local utils = require("dns.utils")
local fileexists = require("pl.path").exists

local resolver, time, log, log_WARN
-- check on nginx/OpenResty and fix some ngx replacements
if ngx then
  resolver = require("resty.dns.resolver")
  time = ngx.now
  log = ngx.log
  log_WARN = ngx.WARN
else
  resolver = require("dns.resolver")
  time = require("socket").gettime
  log_WARN = "WARNING"
  log = function(...) 
    --print(...)
  end
end

-- resolver options
local opts

-- recursion level before erroring out
local max_dns_recursion = 20

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
-- for this name, see `resolve`  function.
local cache = {}

-- lookup a single entry in the cache. Invalidates the entry if its beyond its ttl
local cachelookup = function(qname, qtype)
  local now = time()
  local key = qtype..":"..qname
  local cached = cache[key]
  
  if cached then
    if cached.expire < now then
      -- the cached entry expired
      cache[key] = nil
      cached = nil
    else
      cached.touch = now
    end
  end
  
  return cached
end

-- inserts an entry in the cache, except if the ttl=0, then it deletes it from the cache
local cacheinsert = function(entry)

  local e1 = entry[1]
  local key = e1.type..":"..e1.name
  
  -- determine minimum ttl of all answer records
  local ttl = e1.ttl
  for i = 2, #entry do
    ttl = math.min(ttl, entry[i].ttl)
  end
  
  -- special case; 0 ttl is never stored
  if ttl == 0 then
    cache[key] = nil
    return
  end
  
  -- set expire time
  local now = time()
  entry.touch = now
  entry.expire = now + ttl
  cache[key] = entry
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
  elseif type(all) == "number" then
    f = function(entry, now, count)  -- check ttl and touch
      if (entry.expire < now) or (entry.touch + touched < now) then
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
--    Cache for re-usable DNS resolver objects
-- ==============================================

-- resolver objects cache
local res_avail = {} -- available resolvers
local res_busy = {}  -- resolvers now busy
local res_count = 0  -- total resolver count
local res_max = 50   -- maximum nr of resolvers to retain
local res_top = res_max -- we warn, if count exceeds top

-- implements cached resolvers, so we don't create resolvers upon every request
-- @param same parameters as the openresty `query` methods
-- @return same results as the openresty queries
local function query(qname, r_opts)
  assert(opts, "Not initialized, call `init` first")
  
  local err, result
  -- get resolver from cache
  local r = next(res_avail)
  if not r then
    -- no resolver left in the cache, so create a new one
    r, err = resolver:new(opts)
    if not r then
      return r, err
    end
    res_busy[r] = r
    res_count = res_count + 1
  else
    -- found one, move it from avail to busy
    res_avail[r] = nil
    res_busy[r] = r
  end
  
  if res_count > res_top then
    res_top = res_count
    log(log_WARN, "DNS client: hit a new maximum of resolvers; "..
      res_top..", whilst cache max size is currently set at; "..res_max)  
  end
  
  result, err = r:query(qname, r_opts)

  res_busy[r] = nil
  if result and res_count <= res_max then
    -- if successful and within maximum number, reuse resolver
    res_avail[r] = r
  else
    -- failed, or too many, so drop the resolver object
    res_count = res_count - 1
  end
  
  return result, err
end

-- ==============================================
--    Main DNS functions for lookup
-- ==============================================

local cname_opt = { qtype = _M.TYPE_CNAME }
local a_opt = { qtype = _M.TYPE_A }
local aaaa_opt = { qtype = _M.TYPE_AAAA }
local srv_opt = { qtype = _M.TYPE_SRV }
local type_order = {
  a_opt,
  aaaa_opt,
  srv_opt,
}

--- initialize resolver. Will parse hosts and resolv.conf files/tables.
-- If the `hosts` and `resolv_conf` fields are not provided, it will fall back on default
-- filenames (see the `dns.utils` module for details). To prevent any potential 
-- blocking i/o all together, manually fetch the contents of those files and 
-- provide them as tables. Or provide both fields as empty tables.
-- @param options Same table as the openresty dns resolver, with extra fields `hosts`, `resolv_conf` containing the filenames to parse, and `max_resolvers` indicating the maximum number of resolver objects to cache.
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
  
  res_max = options.max_resolvers or res_max
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
    resolv, err = utils.apply_env(utils.parse_resolv_conf(options.resolve_conf))
    if not resolv then return resolv, err end
  else
    log(log_WARN, "Resolv.conf file not found: "..tostring(resolvconffile))  
    resolv = {}
  end
  
  if not options.nameservers and resolv.nameserver then
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
  
  options.retrans = options.retrans or resolv.attempts
  
  if not options.timeout and resolv.timeout then
    options.timeout = resolv.timeout * 1000
  end
  
  -- options.no_recurse = -- not touching this one for now
  
  opts = options -- store it in our module level global

  return true
end

-- will lookup in the cache, or alternatively query dns servers and populate the cache.
-- only looks up the requested type
local function _lookup(qname, r_opts)
  local qtype = r_opts.qtype
  local record = cachelookup(qname, qtype)
  
  if record then
    -- cache hit
    return record  
  else
    -- not found in our cache, so perform query on dns servers
    local answers, err = query(qname, r_opts)
    if not answers then return answers, err end
    
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
    if #answers > 0 then
      cacheinsert(answers)
    end
    return answers
  end
end

-- looks up the name, while following CNAME redirects
local function lookup(qname, r_opts, count)
  count = (count or 0) + 1
  if count > max_dns_recursion then
    return nil, "More than "..max_dns_recursion.." DNS redirects, recursion error?"
  end
  
  local records, err = _lookup(qname, r_opts)
  -- NOTE: if the name exists, but the type doesn't match, we get 
  -- an empty table. Hence check the length!
  if (records and #records > 0) or r_opts.qtype == _M.TYPE_CNAME then
    -- return record found, or the error in case it was a CNAME already
    -- because then there is nothing to follow.
    return records, err
  end
  
  -- try a CNAME
  local records2 = _lookup(qname, cname_opt)
  if (not records2) or (#records2 == 0) then
    return records, err   -- NOTE: return initial error!
  end
  
  -- CNAME success, now recurse the lookup to find the one we're actually looking for
  -- TODO: For CNAME we assume only one entry. Correct???
  return lookup(records2[1].cname, r_opts, count)
end

--- Resolves a name following CNAME redirects. CNAME will not be followed when
-- the requested type is CNAME.
-- @param qname Same as the openresty `query` method
-- @param r_opts Same as the openresty `query` method (defaults to A type query)
-- @return A list of records. The list can be empty if the name is present on the server, but as a different record type. Any dns server errors are returned in a hashtable (see openresty docs).
_M.resolve_type = function(qname, r_opts)
  qname = qname:lower()
  if not r_opts then
    r_opts = a_opt
  else
    r_opts.qtype = r_opts.qtype or _M.TYPE_A
  end
  return lookup(qname, r_opts)
end

--- Resolve a name using a generic type-order. It will try to resolve the given
-- name using the following record types, in the order listed;
-- 
-- 1. last succesful lookup type (if any), 
-- 2. A-record, 
-- 3. AAAA-record, 
-- 4. SRV-record.
--
-- So requesting `mysrv.domain.com` (assuming to be an SRV record) will try to resolve
-- it (the first time) as A, then AAAA, then SRV. If succesful, a second lookup 
-- will now try SRV, A, AAAA, SRV.
-- This function will dereference CNAME records, but will not resolv any SRV content.
-- @param qname Name to resolve
-- @return A list of records. The list can be empty if the name is present on the server, but as a different record type. Any dns server errors are returned in a hashtable (see openresty docs).
_M.resolve = function(qname)
  qname = qname:lower()
  local last = cache[qname]  -- check if we have a previous succesful one
  local records, err
  for i = (last and 0 or 1), #type_order do
    local type_opt = ((i == 0) and { qtype = last } or type_order[i])
    if (type_opt.qtype == last) and (i ~= 0) then
      -- already tried this one, based on 'last', no use in trying again
    else
      records, err = _M.resolve_type(qname, type_opt)
      -- NOTE: if the name exists, but the type doesn't match, we get 
      -- an empty table. Hence check the length!
      if records and #records > 0 then
        cache[qname] = type_opt.qtype -- set last succesful type resolved
        return records
      end
    end
  end
  -- we failed, clear cache and return last error
  cache[qname] = nil
  return records, err
end

-- export the local cache in case we're testing
if _TEST then _M.__cache = cache end 

return _M

