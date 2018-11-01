--------------------------------------------------------------------------
-- __Base-balancer__
--
-- The base class for balancers. It implements DNS resolution and fanning
-- out hostnames to addresses. It builds and maintains a tree structure:
--
--   `balancer <1 --- many> hosts <1 --- many> addresses`
--
-- Updating the dns records is passive, meaning that when `getPeer` accesses a
-- target, only then the check is done whether the record is stale, and if so
-- it is updated. The one exception is the failed dns queries (because they
-- have no indices assigned, and hence will never be hit by `getPeer`), which will
-- be actively refreshed using a timer.
--
-- __Weights__
--
-- Weights will be tracked as follows. Since a Balancer has multiple Hosts, and
-- a Host has multiple Addresses. The Host weight will be the sum of all its
-- addresses, and the Balancer weight will be the sum of all Hosts.
-- See `addHost` on how to set the weight for an `address`.
--
-- The weight of each `address` will be the weight provided as `nodeWeight` when adding
-- a `host`. So adding a `host` with weight 20, that resolves to 2 IP addresses, will
-- insert 2 `addresses` each with a weight of 20, totalling the weight of the `host` to
-- 40.
--
-- Exception 1: If the `host` resolves to an SRV record, in which case each
-- `address` gets the weight as specified in the DNS record. In this case the
-- `nodeWeight` property will be ignored.
--
-- Exception 2: If the DNS record for the `host` has a `ttl=0` then the record contents
-- will be ignored, and a single address with the original hostname will be
-- inserted. This address will get a weight assigned of `nodeWeight`.
-- Whenever the balancer hits this address, it will be resolved on the spot, hence
-- honouring the `ttl=0` value.
--
-- __Adding and resolving hosts__
--
-- When adding a host, it will be resolved and for each entry an `address` will be
-- added. With the exception of a `ttl=0` setting as noted above. When resolving the
-- names, any CNAME records will be dereferenced immediately, any other records
-- will not.
--
-- _Example 1: add an IP address "127.0.0.1"_
--
-- The host object will resolve the name, and since it's and IP address it
-- returns a single A record with 1 entry, that same IP address.
--
--    host object 1   : hostname="127.0.0.1"  --> since this is the name added
--    address object 1: ip="127.0.0.1"        --> single IP address
--
-- _Example 2: complex DNS lookup chain_
--
-- assuming the following lookup chain for a `host` added by name `"myhost"`:
--
--    myhost    --> CNAME yourhost
--    yourhost  --> CNAME herhost
--    herhost   --> CNAME theirhost
--    theirhost --> SRV with 2 entries: host1.com, host2.com
--    host1.com --> A with 1 entry: 192.168.1.10
--    host2.com --> A with 1 entry: 192.168.1.11
--
-- Adding a host by name `myhost` will first create a `host` by name `myhost`. It will then
-- resolve the name `myhost`, the CNAME chain will be dereferenced immediately, so the
-- result will be an SRV record with 2 named entries. The names will be used for the
-- addresses:
--
--    host object 1   : hostname="myhost"
--    address object 1: ip="host1.com"  --> NOT an ip, but a name!
--    address object 2: ip="host2.com"  --> NOT an ip, but a name!
--
-- When the balancer hits these addresses (when calling `getPeer`), it will
-- dereference them (so they will be resolved at balancer-runtime, not at
-- balancer-buildtime).
--
-- __Clustering__
--
-- The balancer is deterministic in the way it adds/removes elements. So as long as
-- the confguration is the same, and adding/removing hosts is done in the same order
-- the exact same balancer will be created. This is important in case of
-- consistent-hashing approaches, since each cluster member needs to behave the same.
--
-- _NOTE_: there is one caveat, DNS resolution is not deterministic, because timing
-- differences might cause different orders of adding/removing. Hence the structures
-- can potentially slowly diverge. If this is unacceptable, make sure you do not
-- invlove DNS by adding hosts by their IP adresses instead of their hostname.
--
-- __Housekeeping__
--
-- The balancer does some house keeping and may insert
-- some extra fields in dns records. Those fields will have an `__` prefix
-- (double underscores).
--
-- @author Thijs Schreijer
-- @copyright 2016-2018 Kong Inc. All rights reserved.
-- @license Apache 2.0


local DEFAULT_WEIGHT = 10   -- default weight for a host, if not provided
local DEFAULT_PORT = 80     -- Default port to use (A and AAAA only) when not provided
local TTL_0_RETRY = 60      -- Maximum life-time for hosts added with ttl=0, requery after it expires
local REQUERY_INTERVAL = 30 -- Interval for requerying failed dns queries
local SRV_0_WEIGHT = 1      -- SRV record with weight 0 should be hit minimally, hence we replace by 1

local dns_client = require "resty.dns.client"
local dns_utils = require "resty.dns.utils"
local resty_timer = require "resty.timer"
local time = ngx.now
local table_sort = table.sort
local table_remove = table.remove
local table_concat = table.concat
local math_floor = math.floor
local string_format = string.format
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_DEBUG = ngx.DEBUG
local ngx_WARN = ngx.WARN
local balancer_id_counter = 0

local empty = setmetatable({},
  {__newindex = function() error("The 'empty' table is read-only") end})

local errors = setmetatable({
  ERR_DNS_UPDATED = "Cannot get peer, a DNS update changed the balancer structure, please retry",
  ERR_ADDRESS_UNAVAILABLE = "Address is marked as unavailable",
  ERR_NO_PEERS_AVAILABLE = "No peers are available",
}, {
  __index = function(self, key)
    error("invalid key: " .. tostring(key))
  end
})


local _M = {}


-- Address object metatable to use for inheritance
local objAddr = {}
local mt_objAddr = { __index = objAddr }
local objHost = {}
local mt_objHost = { __index = objHost }
local objBalancer = {}
local mt_objBalancer = { __index = objBalancer }

------------------------------------------------------------------------------
-- Implementation properties.
-- These properties are only relevant for implementing a new balancer algorithm
-- using this base class. To use a balancer see the _User properties_ section.
-- @section implementation


-- ===========================================================================
-- address object.
-- Manages an ip address. It is generated by resolving a `host`, hence a single
-- `host` can have multiple `addresses` associated.
-- ===========================================================================

-- Returns the peer info.
-- @return ip-address, port and hostname of the target, or nil+err if unavailable
-- or lookup error
function objAddr:getPeer(cacheOnly)
  if not self.available then
    return nil, errors.ERR_ADDRESS_UNAVAILABLE
  end

  -- check with our Host whether the DNS record is still up to date
  if not self.host:addressStillValid(cacheOnly, self) then
    -- DNS expired, and this address was removed
    return nil, errors.ERR_DNS_UPDATED
  end

  if self.ipType == "name" then
    -- SRV type record with a named target
    local ip, port, try_list = dns_client.toip(self.ip, self.port, cacheOnly)
    if not ip then
      port = tostring(port) .. ". Tried: " .. tostring(try_list)
    end
    -- which is the proper name to return in this case?
    -- `self.host.hostname`? or the named SRV entry: `self.ip`?
    -- use our own hostname, as it might be used to mark this address
    -- as unhealthy, so we must be able to find it
    return ip, port, self.host.hostname
  else
    -- just an IP address
    return self.ip, self.port, self.host.hostname
  end
end

-- disables an address object from the balancer.
-- It will set its weight to 0, and the `disabled` flag to `true`.
-- @see delete
function objAddr:disable()
  ngx_log(ngx_DEBUG, self.log_prefix, "disabling address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  -- weight to 0; effectively disabling it
  self:change(0)
  self.disabled = true
end

-- Cleans up an address object.
-- The address must have been disabled before.
-- @see disable
function objAddr:delete()
  assert(self.disabled, "Cannot delete an address that wasn't disabled first")
  ngx_log(ngx_DEBUG, self.log_prefix, "deleting address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  self.host.balancer:callback("removed", self.ip,
                              self.port, self.host.hostname)
  self.host.balancer:onRemoveAddress(self)
  self.host = nil
end

-- Changes the weight of an address.
function objAddr:change(newWeight)
  ngx_log(ngx_DEBUG, self.log_prefix, "changing address weight: ", self.ip, ":", self.port,
          "(host ", (self.host or empty).hostname, ") ",
          self.weight, " -> ", newWeight)

  self.host:addWeight(newWeight - self.weight)
  self.weight = newWeight
end

-- Set the availability of the address.
function objAddr:setState(available)
  self.available = not not available -- force to boolean
end


--- Creates a new address object. There is no need to call this from user code.
-- When implementing a new balancer algorithm, you might want to override this method.
-- The `addr` table should contain:
--
-- - `ip`: the upstream ip address or target name
-- - `port`: the upstream port number
-- - `weight`: the relative weight for the balancer algorithm
-- - `host`: the host object the new address belongs to
-- @param addr table to be transformed to Address object
-- @return new address object, or error on bad input
function objBalancer:newAddress(addr)
  assert(type(addr.ip) == "string", "Expected 'ip' to be a string, got: " .. type(addr.ip))
  assert(type(addr.port) == "number", "Expected 'port' to be a number, got: " .. type(addr.port))
  assert(addr.port > 0 and addr.port < 65536, "Expected 'port` to be between 0 and 65536, got: " .. addr.port)
  assert(type(addr.weight) == "number", "Expected 'weight' to be a number, got: " .. type(addr.weight))
  assert(addr.weight >= 0, "Expected 'weight' to be equal or greater than 0, got: " .. addr.weight)
  assert(type(addr.host) == "table", "Expected 'host' to be a table, got: " .. type(addr.host))
  assert(getmetatable(addr.host) == mt_objHost, "Expected 'host' to be an objHost type")

  addr = setmetatable(addr, mt_objAddr)
  addr.super = objAddr
  addr.ipType = dns_utils.hostnameType(addr.ip)  -- 'ipv4', 'ipv6' or 'name'
  addr.log_prefix = addr.host.log_prefix
  addr.available = true      -- is this target available?
  addr.disabled = false      -- has this record been disabled? (before deleting)

  addr.host:addWeight(addr.weight)

  ngx_log(ngx_DEBUG, addr.host.log_prefix, "new address for host '", addr.host.hostname,
          "' created: ", addr.ip, ":", addr.port, " (weight ", addr.weight,")")

  addr.host.balancer:callback("added", addr.ip, addr.port, addr.host.hostname)
  addr.host.balancer:onAddAddress(addr)
  return addr
end


-- ===========================================================================
-- Host object.
-- Manages a single hostname, with DNS resolution and expanding into
-- multiple `address` objects.
-- ===========================================================================

-- define sort order for DNS query results
local sortQuery = function(a,b) return a.__balancerSortKey < b.__balancerSortKey end
local sorts = {
  [dns_client.TYPE_A] = function(result)
    local sorted = {}
    -- build table with keys
    for i, v in ipairs(result) do
      sorted[i] = v
      v.__balancerSortKey = v.address
    end
    -- sort by the keys
    table_sort(sorted, sortQuery)
    -- reverse index
    for i, v in ipairs(sorted) do sorted[v.__balancerSortKey] = i end
    return sorted
  end,
  [dns_client.TYPE_SRV] = function(result)
    local sorted = {}
    -- build table with keys
    for i, v in ipairs(result) do
      sorted[i] = v
      v.__balancerSortKey = string_format("%06d:%s:%s", v.priority, v.target, v.port)
    end
    -- sort by the keys
    table_sort(sorted, sortQuery)
    -- reverse index
    for i, v in ipairs(sorted) do sorted[v.__balancerSortKey] = i end
    return sorted
  end,
}
sorts[dns_client.TYPE_AAAA] = sorts[dns_client.TYPE_A] -- A and AAAA use the same sorting order
sorts = setmetatable(sorts,{
    -- all record types not mentioned above are unsupported, throw error
    __index = function(self, key)
      error("Unknown/unsupported DNS record type; "..tostring(key))
    end,
  })

-- Queries the DNS for this hostname. Updates the underlying address objects.
-- This method always succeeds, but it might leave the balancer in a 0-weight
-- state if none of the hosts resolves.
-- @return `true`, always succeeds
function objHost:queryDns(cacheOnly)

  ngx_log(ngx_DEBUG, self.log_prefix, "querying dns for ", self.hostname)

  -- first thing we do is the dns query, this is the only place we possibly
  -- yield (cosockets in the dns lib). So once that is done, we're 'atomic'
  -- again, and we shouldn't have any nasty race conditions
  local dns = self.balancer.dns
  local newQuery, err, try_list = dns.resolve(self.hostname, nil, cacheOnly)

  local oldQuery = self.lastQuery or {}
  local oldSorted = self.lastSorted or {}

  if err then
    ngx_log(ngx_WARN, self.log_prefix, "querying dns for ", self.hostname,
            " failed: ", err , ". Tried ", tostring(try_list))

    -- query failed, create a fake recorded, flagged as failed.
    -- the empty record will cause all existing addresses to be removed
    newQuery = {
      __errorQueryFlag = true  --flag to mark the record as a failed lookup
    }
    self.balancer:startRequery()
  end

  -- we're using the dns' own cache to check for changes.
  -- if our previous result is the same table as the current result, then nothing changed
  if oldQuery == newQuery then
    ngx_log(ngx_DEBUG, self.log_prefix, "no dns changes detected for ", self.hostname)

    return true    -- exit, nothing changed
  end

  -- To detect ttl = 0 we validate both the old and new record. This is done to ensure
  -- we do not hit the edgecase of https://github.com/Kong/lua-resty-dns-client/issues/51
  -- So if we get a ttl=0 twice in a row (the old one, and the new one), we update it. And
  -- if the very first request ever reports ttl=0 (we assume we're not hitting the edgecase
  -- in that case)
  if (newQuery[1] or empty).ttl == 0 and ((oldQuery[1] or empty).ttl or 0) == 0 then
    -- ttl = 0 means we need to lookup on every request.
    -- To enable lookup on each request we 'abuse' a virtual SRV record. We set the ttl
    -- to `ttl0Interval` seconds, and set the `target` field to the hostname that needs
    -- resolving. Now `getPeer` will resolve on each request if the target is not an IP address,
    -- and after `ttl0Interval` seconds we'll retry to see whether the ttl has changed to non-0.
    -- Note: if the original record is an SRV we cannot use the dns provided weights,
    -- because we can/are not going to possibly change weights on each request
    -- so we fix them at the `nodeWeight` property, as with A and AAAA records.
    if oldQuery.__ttl0Flag then
      -- still ttl 0 so nothing changed
      ngx_log(ngx_DEBUG, self.log_prefix, "no dns changes detected for ",
              self.hostname, ", still using ttl=0")
      return true
    end
    ngx_log(ngx_DEBUG, self.log_prefix, "ttl=0 detected for ",
            self.hostname)
    newQuery = {
        {
          type = dns.TYPE_SRV,
          target = self.hostname,
          name = self.hostname,
          port = self.port,
          weight = self.nodeWeight,
          priority = 1,
          ttl = self.balancer.ttl0Interval,
        },
        expire = time() + self.balancer.ttl0Interval,
        touched = time(),
        __ttl0Flag = true,        -- flag marking this record as a fake SRV one
      }
  end

  -- a new dns record, was returned, but contents could still be the same, so check for changes
  -- sort table in unique order
  local rtype = (newQuery[1] or empty).type
  if not rtype then
    -- we got an empty query table, so assume A record, because it's empty
    -- all existing addresses will be removed
    ngx_log(ngx_DEBUG, self.log_prefix, "blank dns record for ",
              self.hostname, ", assuming A-record")
    rtype = dns.TYPE_A
  end
  local newSorted = sorts[rtype](newQuery)
  local dirty

  if rtype ~= (oldSorted[1] or empty).type then
    -- DNS recordtype changed; recycle everything
    ngx_log(ngx_DEBUG, self.log_prefix, "dns record type changed for ",
            self.hostname, ", ", (oldSorted[1] or empty).type, " -> ",rtype)
    for i = #oldSorted, 1, -1 do  -- reverse order because we're deleting items
      self:disableAddress(oldSorted[i])
    end
    for _, entry in ipairs(newSorted) do -- use sorted table for deterministic order
      self:addAddress(entry)
    end
    dirty = true
  else
    -- new record, but the same type
    local topPriority = (newSorted[1] or empty).priority -- nil for non-SRV records
    local done = {}
    local dCount = 0
    for _, newEntry in ipairs(newSorted) do
      if newEntry.priority ~= topPriority then break end -- exit when priority changes, as SRV only uses top priority

      local key = newEntry.__balancerSortKey
      local oldEntry = oldSorted[oldSorted[key] or "__key_not_found__"]
      if not oldEntry then
        -- it's a new entry
        ngx_log(ngx_DEBUG, self.log_prefix, "new dns record entry for ",
                self.hostname, ": ", (newEntry.target or newEntry.address),
                ":", newEntry.port) -- port = nil for A or AAAA records
        self:addAddress(newEntry)
        dirty = true
      else
        -- it already existed (same ip, port)
        if newEntry.weight and
           newEntry.weight ~= oldEntry.weight and
           not (newEntry.weight == 0  and oldEntry.weight == SRV_0_WEIGHT) then
          -- weight changed (can only be an SRV)
          self:findAddress(oldEntry):change(newEntry.weight == 0 and SRV_0_WEIGHT or newEntry.weight)
          dirty = true
        else
          ngx_log(ngx_DEBUG, self.log_prefix, "unchanged dns record entry for ",
                  self.hostname, ": ", (newEntry.target or newEntry.address),
                  ":", newEntry.port) -- port = nil for A or AAAA records
        end
        done[key] = true
        dCount = dCount + 1
      end
    end
    if dCount ~= #oldSorted then
      -- not all existing entries were handled, remove the ones that are not in the
      -- new query result
      for _, entry in ipairs(oldSorted) do
        if not done[entry.__balancerSortKey] then
          ngx_log(ngx_DEBUG, self.log_prefix, "removed dns record entry for ",
                  self.hostname, ": ", (entry.target or entry.address),
                  ":", entry.port) -- port = nil for A or AAAA records
          self:disableAddress(entry)
        end
      end
      dirty = true
    end
  end

  self.lastQuery = newQuery
  self.lastSorted = newSorted

  if dirty then
    -- above we already added and updated records. Removed addresses are disabled, and
    -- need yet to be deleted from the Host
    ngx_log(ngx_DEBUG, self.log_prefix, "updating balancer based on dns changes for ",
            self.hostname)

    -- allow balancer to update its algorithm
    self.balancer:afterHostUpdate(self)

    -- delete addresses previously disabled
    self:deleteAddresses()
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "querying dns and updating for ", self.hostname, " completed")
  return true
end

-- Changes the host overall weight. It will also update the parent balancer object.
-- This will be called by the `address` object whenever it changes its weight.
function objHost:addWeight(delta)
  self.weight = self.weight + delta
  self.balancer:addWeight(delta)
end

-- Updates the host nodeWeight.
-- @return `true` if something changed that might impact the balancer algorithm
function objHost:change(newWeight)
  local dirty = false
  self.nodeWeight = newWeight
  local lastQuery = self.lastQuery or {}
  if #lastQuery > 0 then
    if lastQuery[1].type == dns_client.TYPE_SRV and not lastQuery.__ttl0Flag then
      -- this is an SRV record (and not a fake ttl=0 one), which
      -- carries its own weight setting, so nothing to update
      ngx_log(ngx_DEBUG, self.log_prefix, "ignoring weight change for ", self.hostname,
              " as SRV records carry their own weight")
    else
      -- so here we have A, AAAA, or a fake SRV, which uses the `nodeWeight` property
      -- go update all our addresses
      for _, addr in ipairs(self.addresses) do
        addr:change(newWeight)
      end
      dirty = true
    end
  end
  return dirty
end

-- Adds an `address` object to the `host`.
-- @param entry (table) DNS entry (single entry, not the full record)
function objHost:addAddress(entry)
  local weight = entry.weight  -- this is nil for anything else than SRV
  if weight == 0 then
    -- Special case: SRV with weight = 0 should be included, but with
    -- the lowest possible probability of being hit. So we force it to
    -- weight 1.
    weight = SRV_0_WEIGHT
  end
  local addresses = self.addresses
  addresses[#addresses + 1] = self.balancer:newAddress {
    ip = entry.address or entry.target,
    port = (entry.port ~= 0 and entry.port) or self.port,
    weight = weight or self.nodeWeight,
    host = self,
  }
end

-- Looks up an `address` by a dns entry
-- @param entry (table) DNS entry (single entry, not the full record)
-- @return address object or nil if not found
function objHost:findAddress(entry)
  for _, addr in ipairs(self.addresses) do
    if (addr.ip == (entry.address or entry.target)) and
        addr.port == (entry.port or self.port) then
      -- found it
      return addr
    end
  end
  return -- not found
end

-- Looks up and disables an `address` object from the `host`.
-- @param entry (table) DNS entry (single entry, not the full record)
-- @return address object that was disabled
function objHost:disableAddress(entry)
  local addr = self:findAddress(entry)
  if addr and not addr.disabled then
    addr:disable()
  end
  return addr
end

-- Looks up and deletes previously disabled `address` objects from the `host`.
-- @return `true`
function objHost:deleteAddresses()
  for i = #self.addresses, 1, -1 do -- deleting entries, hence reverse traversal
    if self.addresses[i].disabled then
      self.addresses[i]:delete()
      table_remove(self.addresses, i)
    end
  end

  return true
end

-- disables a host, by setting all adressess to 0
-- Host can only be deleted after updating the balancer algorithm!
-- @return true
function objHost:disable()
  -- set weights to 0
  for _, addr in ipairs(self.addresses) do
    addr:disable()
  end

  return true
end

-- Cleans up a host. Only when its weight is 0.
-- Should only be called after updating the balancer algorithm!
-- @return true or throws an error if weight is non-0
function objHost:delete()
  assert(self.weight == 0, "Cannot delete a host with a non-0 weight")

  for i = #self.addresses, 1, -1 do  -- reverse traversal as we're deleting
    self.addresses[i]:delete()
  end

  self.balancer = nil
end


function objHost:addressStillValid(cacheOnly, address)

  if (self.lastQuery.expire or 0) < time() and not cacheOnly then
    -- ttl expired, so must renew
    self:queryDns(cacheOnly)

    if (address or empty).host ~= self then
      -- the address no longer points to this host, so it is not valid anymore
      ngx_log(ngx_DEBUG, self.log_prefix, "DNS record for ", self.hostname,
              " was updated and no longer contains the address")
      return false
    end
  end

  return true
end

--- Creates a new host object. There is no need to call this from user code.
-- When implementing a new balancer algorithm, you might want to override this method.
-- The `host` table should have fields:
--
-- - `hostname`: the upstream hostname (as used in dns queries)
-- - `port`: the upstream port number for A and AAAA dns records. For SRV records
--   the reported port by the DNS server will be used.
-- - `nodeWeight`: the relative weight for the balancer algorithm to assign to each A
--   or AAAA dns record. For SRV records the reported weight by the DNS server
--   will be used.
-- - `balancer`: the balancer object the host belongs to
-- @param host table to create the host object from.
-- @return new host object, or error on bad input.
function objBalancer:newHost(host)
  assert(type(host.hostname) == "string", "Expected 'host' to be a string, got: " .. type(host.hostname))
  assert(type(host.port) == "number", "Expected 'port' to be a number, got: " .. type(host.port))
  assert(host.port > 0 and host.port < 65536, "Expected 'port` to be between 0 and 65536, got: " .. host.port)
  assert(type(host.nodeWeight) == "number", "Expected 'nodeWeight' to be a number, got: " .. type(host.nodeWeight))
  assert(host.nodeWeight >= 0, "Expected 'nodeWeight' to be equal or greater than 0, got: " .. host.nodeWeight)
  assert(type(host.balancer) == "table", "Expected 'balancer' to be a table, got: " .. type(host.balancer))
  assert(getmetatable(host.balancer) == mt_objBalancer, "Expected 'balancer' to be an objBalancer type")

  host = setmetatable(host, mt_objHost)
  host.super = objHost
  host.log_prefix = host.balancer.log_prefix
  host.weight = 0           -- overall weight of all addresses within this hostname
  host.lastQuery = nil      -- last successful dns query performed
  host.lastSorted = nil     -- last successful dns query, sorted for comparison
  host.addresses = {}       -- list of addresses (address objects) this host resolves to
  host.expire = nil         -- time when the dns query this host is based upon expires


  -- insert into our parent balancer before recalculating (in queryDns)
  -- This should actually be a responsibility of the balancer object, but in
  -- this case we do it here, because it is needed before we can redistribute
  -- the indices in the queryDns method just below.
  host.balancer.hosts[#host.balancer.hosts + 1] = host

  ngx_log(ngx_DEBUG, host.balancer.log_prefix, "created a new host for: ", host.hostname)

  host:queryDns()

  return host
end


-- ===========================================================================
-- Balancer object.
-- Manages a set of hostnames, to balance the requests over.
-- ===========================================================================

--- List of addresses.
-- This is a list of addresses, ordered based on when they were added.
-- @field objBalancer.addresses

--- List of hosts.
-- This is a list of addresses, ordered based on when they were added.
-- @field objBalancer.hosts


-- Address iterator.
-- Iterates over all addresses in the balancer (nested through the hosts)
-- @return weight (number), address (address object), host (host object the address belongs to)
function objBalancer:addressIter()
  local host_idx = 1
  local addr_idx = 1
  return function()
    local host = self.hosts[host_idx]
    if not host then return end -- done

    local addr
    while not addr do
      addr = host.addresses[addr_idx]
      if addr then
        addr_idx = addr_idx + 1
        return addr.weight, addr, host
      end
      addr_idx = 1
      host_idx = host_idx + 1
      host = self.hosts[host_idx]
      if not host then return end -- done
    end
  end
end


--- This method is called after changes have been made to the addresses.
--
-- When implementing a new balancer algorithm, you might want to override this method.
--
-- The call is after the addition of new, and disabling old, but before
-- deleting old addresses.
-- The `address.disabled` field will be `true` for addresses that are about to be deleted.
-- @param host the `host` object that had its addresses updated
function objBalancer:afterHostUpdate(host)
end

--- Adds a host to the balancer.
-- The name will be resolved and for each DNS entry an `address` will be added.
--
-- Within a balancer the combination of `hostname` and `port` must be unique, so
-- multiple calls with the same target will only update the `weight` of the
-- existing entry.
-- @return balancer object, or throw an error on bad input
-- @within User properties
function objBalancer:addHost(hostname, port, nodeWeight)
  assert(type(hostname) == "string", "expected a hostname (string), got "..tostring(hostname))
  port = port or DEFAULT_PORT
  nodeWeight = nodeWeight or DEFAULT_WEIGHT
  assert(type(nodeWeight) == "number" and
         math_floor(nodeWeight) == nodeWeight and
         nodeWeight >= 1,
         "Expected 'weight' to be an integer >= 1; got "..tostring(nodeWeight))

  local host
  for _, host_entry in ipairs(self.hosts) do
    if host_entry.hostname == hostname and host_entry.port == port then
      -- found it
      host = host_entry
      break
    end
  end

  if not host then
    -- create the new host, that will insert itself in the balancer
    self:newHost {
      hostname = hostname,
      port = port,
      nodeWeight = nodeWeight,
      balancer = self
    }
  else
    -- this one already exists, update if different
    ngx_log(ngx_DEBUG, self.log_prefix, "host ", hostname, ":", port,
            " already exists, updating weight ",
            host.nodeWeight, "-> ",nodeWeight)

    if host.nodeWeight ~= nodeWeight then
      -- weight changed, go update
      local dirty = host:change(nodeWeight)
      if dirty then
        -- update had an impact so must redistribute indices
        self:afterHostUpdate(host)
      end
    end
  end

  return self
end


--- This method is called after a host is being removed from the balancer.
--
--  When implementing a new balancer algorithm, you might want to override this method.
--
-- The call is after disabling, but before deleting the associated addresses. The
-- address.disabled field will be true for addresses that are about to be deleted.
-- @param host the `host` object about to be deleted
function objBalancer:beforeHostDelete(host)
end


--- This method is called after an address is being added to the balancer.
--
-- When implementing a new balancer algorithm, you might want to override this method.
function objBalancer:onAddAddress(address)
  local list = self.addresses
  assert(list[address] == nil, "Can't add address twice")

  list[#list + 1] = address
end


--- This method is called after an address has been deleted from the balancer.
--
-- When implementing a new balancer algorithm, you might want to override this method.
function objBalancer:onRemoveAddress(address)
  local list = self.addresses

  -- go remove it
  for i, addr in ipairs(list) do
    if addr == address then
      -- found it
      table_remove(list, i)
      return
    end
  end
  error("Address not in the list")
end

--- Removes a host from the balancer. All associated addresses will be
-- deleted, causing updates to the balancer algorithm.
-- Will not throw an error if the hostname is not in the current list.
-- @param hostname hostname to remove
-- @param port port to remove (optional, defaults to 80 if omitted)
-- @return balancer object, or an error on bad input
-- @within User properties
function objBalancer:removeHost(hostname, port)
  assert(type(hostname) == "string", "expected a hostname (string), got "..tostring(hostname))
  port = port or DEFAULT_PORT
  for i, host in ipairs(self.hosts) do
    if host.hostname == hostname and host.port == port then

      ngx_log(ngx_DEBUG, self.log_prefix, "removing host ", hostname, ":", port)

      -- set weights to 0
      host:disable()

      -- removing hosts must always be recalculated to make sure
      -- its order is deterministic (only dns updates are not)
      self:beforeHostDelete(host)

      -- remove host
      host:delete()
      table_remove(self.hosts, i)
      break
    end
  end
  return self
end

-- Updates the total weight.
-- @param delta the in/decrease of the overall weight (negative for decrease)
function objBalancer:addWeight(delta)
  self.weight = self.weight + delta
end


--- Gets the next ip address and port according to the loadbalancing scheme.
-- If the dns record attached to the requested wheel index is expired, then it will
-- be renewed and as a consequence the balancer algorithm might be updated.
-- @param cacheOnly If truthy, no dns lookups will be done, only cache.
-- @param handle the `handle` returned by a previous call to `getPeer`. This will
-- retain some state over retries. See also `setPeerStatus`.
-- @param hashValue (optional) number for consistent hashing, round-robins if
-- omitted. The hashValue must be an (evenly distributed) `integer >= 0`.
-- @return `ip + port + hostname` + `handle`, or `nil+error`
-- @within User properties
function objBalancer:getPeer(cacheOnly, handle, hashValue)

  error(("Not implemented. cacheOnly: %s hashValue: %s"):format(
      tostring(cacheOnly), tostring(hashValue)))


  --[[ below is just some example code:

  if handle then
    -- existing handle, so it's a retry
    if hashValue then
      -- we have a new hashValue, use it anyway
      handle.hashValue = hashValue
    else
      hashValue = handle.hashValue  -- reuse exiting (if any) hashvalue
    end
    handle.retryCount = handle.retryCount + 1
  else
    -- no handle, so this is a first try
    handle = {
      retryCount = 0,
      hashValue = hashValue,
    }
  end

  local address
  while true do
    if self.weight == 0 then
      -- the balancer weight is 0, so we have no targets at all.
      -- This check must be inside the loop, since caling getPeer could
      -- cause a DNS update.
      return nil, errors.ERR_NO_PEERS_AVAILABLE
    end


    -- go and find the next `address` object according to the LB policy
    address = nil


    local ip, port, hostname = address:getPeer(cacheOnly)
    if ip then
      -- success, exit
      handle.address = address
      return ip, port, hostname, handle

    elseif port == errors.ERR_ADDRESS_UNAVAILABLE then
      -- the address was marked as unavailable, keep track here
      -- if all of them fail, then do:
      return nil, errors.ERR_NO_PEERS_AVAILABLE

    elseif port ~= errors.ERR_DNS_UPDATED then
      -- an unknown error
      return nil, port
    end

    -- if here, we're going to retry because of an unavailable
    -- peer, or because of a dns update
  end

  -- unreachable   --]]
end


--- Sets the current status of an address.
-- This allows to temporarily suspend peers when they are offline/unhealthy,
-- it will not modify the address held by the record. The parameters passed in should
-- be previous results from `getPeer`.
-- Call this either as `setPeerStatus(available, handle)` or as `setPeerStatus(available, ip, port, <hostname>)`.
-- Using the `handle` is preferred since it is guaranteed to match an address. By ip/port/name
-- might fail if there are too many DNS levels.
-- @param available `true` for enabled/healthy, `false` for disabled/unhealthy
-- @param ip_or_handle ip address of the peer, or the `handle` returned by `getPeer`
-- @param port the port of the peer (in address object, not as recorded with the Host!)
-- @param hostname (optional, defaults to the value of `ip`) the hostname
-- @return `true` on success, or `nil+err` if not found
-- @within User properties
function objBalancer:setPeerStatus(available, ip_or_handle, port, hostname)

  if type(ip_or_handle) == "table" then
    -- it's a handle from `setPeer`.
    ip_or_handle.address:setState(available)
    return true
  end

  -- no handle, so go and search for it
  hostname = hostname or ip_or_handle
  local name_srv = {}
  for _, addr, host in self:addressIter() do
    if host.hostname == hostname and addr.port == port then
      if addr.ip == ip_or_handle then
        -- found it
        addr:setState(available)
        return true
      elseif addr.ipType == "name" then
        -- so.... the ip is a name. This means that the host that
        -- was added most likely resolved to an SRV, which then has
        -- in turn names as targets instead of ip addresses.
        -- (possibly a fake SRV for ttl=0 records)
        -- Those names are resolved last minute by `getPeer`.
        -- TLDR: we don't track the IP in this case, so we cannot match the
        -- inputs back to an address to disable/enable it.
        -- We record this fact here, and if we have no match in the end
        -- we can provide a more specific message
        name_srv[#name_srv + 1] = addr.ip .. ":" .. addr.port
      end
    end
  end
  local msg = ("no peer found by name '%s' and address %s:%s"):format(hostname, ip_or_handle, tostring(port))
  if name_srv[1] then
    -- no match, but we did find a named one, so making the message more explicit
    msg = msg .. ", possibly the IP originated from these nested dns names: " ..
          table_concat(name_srv, ",")
    ngx_log(ngx_WARN, self.log_prefix, msg)
  end
  return nil, msg
end

-- Timer invoked to check for failed queries
function objBalancer:requeryTimerCallback()

  ngx_log(ngx_DEBUG, self.log_prefix, "executing requery timer")

  local all_ok = true
  for _, host in ipairs(self.hosts) do
    -- only retry the errorred ones
    if host.lastQuery.__errorQueryFlag then
      all_ok = false -- note: only if NO requery at all is done, we are 'all_ok'
      -- if even a single dns query is performed, we yield on the dns socket
      -- operation and our universe might have changed. Could lead to nasty
      -- race-conditions otherwise.
      host:queryDns(false) -- timer-context; cacheOnly always false
    end
  end

  if all_ok then
    -- shutdown recurring timer
    ngx_log(ngx_DEBUG, self.log_prefix, "requery success, stopping timer")
    self.requeryTimer:cancel()
    self.requeryTimer = nil
  else
    -- not done yet
    ngx_log(ngx_DEBUG, self.log_prefix, "requery failure")
  end
end

-- Starts the requery timer.
function objBalancer:startRequery()
  if self.requeryTimer then return end  -- already running, nothing to do here

  local err
  self.requeryTimer, err = resty_timer({
      recurring = true,
      interval = self.requeryInterval,
      detached = false,
      expire = self.requeryTimerCallback,
    }, self)

  if not self.requeryTimer then
    ngx_log(ngx_ERR, self.log_prefix, "failed to create the timer: ", err)
  end
end

--- Sets an event callback for user code. The callback is invoked for
-- every address added to/removed from the balancer.
-- Signature of the callback is:
--
--   `function(balancer, action, ip, port, hostname)`
--
-- where `ip` might also
-- be a hostname if the DNS resolution returns another name (usually in
-- SRV records). The `action` parameter will be either `"added"` or `"removed"`.
-- @param callback a function called when an address is added/removed
-- @return `true`, or throws an error on bad input
-- @within User properties
function objBalancer:setCallback(callback)
  assert(type(callback) == "function", "expected a callback function")
  self.callback = callback
  return true
end

--- Creates a new base balancer.
--
-- A single balancer can hold multiple hosts. A host can be an ip address or a
-- name. As such each host can have multiple addresses (or actual ip+port
-- combinations).
--
-- The options table has the following fields;
--
-- - `dns` (required) a configured `dns.client` object for querying the dns server.
-- - `requery` (optional) interval of requerying the dns server for previously
-- failed queries. Defaults to 30 if omitted (in seconds)
-- - `ttl0` (optional) Maximum lifetime for records inserted with `ttl=0`, to verify
-- the ttl is still 0. Defaults to 60 if omitted (in seconds)
-- - `callback` (optional) a function called when an address is added. See
-- `setCallback` for details.
-- - `log_prefix` (optional) a name used in the prefix for log messages. Defaults to
-- `"balancer"` which results in log prefix `"[balancer 1]"` (the number is a sequential
-- id number)
-- @param opts table with options
-- @return new balancer object or nil+error
-- @within User properties
_M.new = function(opts)
  assert(type(opts) == "table", "Expected an options table, but got: "..type(opts))
  assert(opts.dns, "expected option `dns` to be a configured dns client")
  assert((opts.requery or 1) > 0, "expected 'requery' parameter to be > 0")
  assert((opts.ttl0 or 1) > 0, "expected 'ttl0' parameter to be > 0")
  assert(type(opts.callback) == "function" or type(opts.callback) == "nil",
    "expected 'callback' to be a function or nil, but got: " .. type(opts.callback))

  balancer_id_counter = balancer_id_counter + 1
  local self = {
    -- properties
    log_prefix = "[" .. (opts.log_prefix or "balancer") .. " " .. tostring(balancer_id_counter) .. "] ",
    hosts = {},    -- a list a host objects
    addresses = {}, -- a list of addresses, including reverse lookup
    weight = 0,    -- total weight of all hosts
    dns = opts.dns,  -- the configured dns client to use for resolving
    requeryTimer = nil,  -- requery timer is not running, see `startRequery`
    requeryInterval = opts.requery or REQUERY_INTERVAL,  -- how often to requery failed dns lookups (seconds)
    ttl0Interval = opts.ttl0 or TTL_0_RETRY, -- refreshing ttl=0 records
    callback = opts.callback or function() end, -- callback for address mutations
  }
  self = setmetatable(self, mt_objBalancer)
  self.super = objBalancer

  ngx_log(ngx_DEBUG, self.log_prefix, "balancer_base created")
  return self
end

-- export the error constants
_M.errors = errors
objBalancer.errors = errors

return _M
