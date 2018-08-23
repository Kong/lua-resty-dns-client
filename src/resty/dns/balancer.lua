--------------------------------------------------------------------------
-- Ring-balancer.
--
-- This loadbalancer is designed for consistent hashing approaches and
-- to retain consistency on a maximum level while dealing with dynamic
-- changes like adding/removing hosts/targets.
--
-- Due to its deterministic way of operating it is also capable of running
-- identical balancers (identical consistent rings) on multiple servers/workers
-- (though it does not implement inter-server/worker communication).
--
-- Only dns is non-deterministic as it might occur when a peer is requested,
-- and hence should be avoided (by directly inserting ip addresses).
-- Adding/deleting hosts, etc (as long as done in the same order) is always
-- deterministic.
--
-- Updating the dns records is passive, meaning that when `getPeer` accesses a
-- target, only then the check is done whether the record is stale, and if so
-- it is updated. The one exception is the failed dns queries (because they
-- have no indices assigned, and hence will never be hit by `getPeer`), which will
-- be actively refreshed using a timer.
--
-- Whenever dns resolution fails for a hostname, the host will relinguish all
-- the indices it owns, and they will be reassigned to other targets.
-- Periodically the query for the hostname will be retried, and if it succeeds
-- it will get (different) indices reassigned to it.
--
-- __Housekeeping__; the ring-balancer does some house keeping and may insert
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
local ERR_INDEX_REASSIGNED = "Cannot get peer, current index got reassigned to another address"
local ERR_ADDRESS_UNAVAILABLE = "Address is marked as unavailable"
local ERR_NO_PEERS_AVAILABLE = "No peers are available"

local bit = require "bit"
local dns = require "resty.dns.client"
local utils = require "resty.dns.utils"
local lrandom = require "random"
local empty = setmetatable({},
  {__newindex = function() error("The 'empty' table is read-only") end})

local time = ngx.now
local table_sort = table.sort
local table_remove = table.remove
local table_concat = table.concat
local math_floor = math.floor
local string_sub = string.sub
local string_format = string.format
local ngx_md5 = ngx.md5_bin
local timer_at = ngx.timer.at
local bxor = bit.bxor
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_DEBUG = ngx.DEBUG
local ngx_WARN = ngx.WARN
local balancer_id_counter = 0

local _M = {}

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function() return {} end
end

--------------------------------------------------------
-- GC'able timer implementation with 'self'
--------------------------------------------------------
local timer_registry = setmetatable({},{ __mode = "v" })
local timer_id = 0

local timer_callback = function(premature, cb, id, ...)
  local self = timer_registry[id]
  if not self then return end  -- GC'ed, nothing more to do
  timer_registry[id] = nil
  return cb(premature, self, ...)
end

local gctimer = function(t, cb, self, ...)
  assert(type(cb) == "function", "expected arg #2 to be a function")
  assert(type(self) == "table", "expected arg #3 to be a table")
  timer_id = timer_id + 1
  timer_registry[timer_id] = self
  -- if in the call below we'd be passing `self` instead of the scalar `timer_id`, it
  -- would prevent the whole `self` object from being garbage collected because
  -- it is anchored on the timer.
  return timer_at(t, timer_callback, cb, timer_id, ...)
end


-- ===========================================================================
-- address object.
-- Manages an ip address. It links to a `host`, and is associated with a number
-- of indices of the balancer-wheel managed by the `balancer`.
-- ===========================================================================
local objAddr = {}

-- Returns the peer info.
-- @return ip-address, port and hostname of the target, or nil+err if unavailable
-- or lookup error
function objAddr:getPeer(cacheOnly)
  if not self.available then
    return nil, ERR_ADDRESS_UNAVAILABLE
  end

  if self.ipType == "name" then
    -- SRV type record with a named target
    local ip, port, try_list = dns.toip(self.ip, self.port, cacheOnly)
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

-- Adds a list of indices to the address. The indices added to the address will
-- be removed from the provided `availableIndicesList`.
-- @param availableIndicesList a list of wheel-indices available for adding
-- @param count the number of indices to take from the list provided, defaults to ALL if omitted
-- @return the address object
function objAddr:addIndices(availableIndicesList, count)
  count = count or #availableIndicesList
  if count > 0 then
    local myWheelIndices = self.indices
    local size = #myWheelIndices
    if count > #availableIndicesList then
      error("more indices requested to be added ("..count..") than provided ("..#availableIndicesList..
            ") for host '"..self.host.hostname..":"..self.port.."' ("..tostring(self.ip)..")")
    end

    local wheel = self.host.balancer.wheel
    local lsize = #availableIndicesList + 1
    for i = 1, count do
      local availableIdx = lsize - i
      local wheelIdx = availableIndicesList[availableIdx]
      availableIndicesList[availableIdx] = nil
      myWheelIndices[size + i] = wheelIdx

      wheel[wheelIdx] = self
    end
    -- track maximum table size reached
    local max = count + size
    if max > self.indicesMax then
      self.indicesMax = max
    end
  end
  return self
end

-- Drop an amount of indices and return them to the overall balancer.
-- @param availableIndicesList The list to add the dropped indices to
-- @param count (optional) The number of indices to drop, defaults to ALL if omitted
-- @return availableIndicesList with added to it the indices removed from this address
function objAddr:dropIndices(availableIndicesList, count)
  local myWheelIndices = self.indices
  local size = #myWheelIndices
  count = count or size
  if count > 0 then
    if count > size then
      error("more indices requested to drop ("..count..") than available ("..size..
            ") in address '"..self.host.hostname..":"..self.port.."' ("..self.ip..")")
    end

    local wheel = self.host.balancer.wheel
    local lsize = #availableIndicesList
    for i = 1, count do
      local myIdx = size + 1 - i
      local wheelIdx = myWheelIndices[myIdx]
      myWheelIndices[myIdx] = nil
      availableIndicesList[lsize + i] = wheelIdx

      wheel[wheelIdx] = nil
    end
    -- track table size reduction
    size = size + count
    if size * 2 < self.indicesMax then
      -- table was reduced by at least half, so drop the original to reduce
      -- memory footprint
      self.indicesMax = size
      self.indices = table.move(self.indices, 1, size, 1, {})
    end
  end
  return availableIndicesList
end

-- disables an address object from the balancer.
-- It will set its weight to 0, so the next indices-recalculation
-- can delete the address by calling `delete`.
-- @see delete
function objAddr:disable()
  ngx_log(ngx_DEBUG, self.log_prefix, "disabling address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  -- weight to 0; force dropping all indices assigned, before actually removing
  self.host:addWeight(-self.weight)
  self.weight = 0
  self.disabled = true
end

-- cleans up an address object.
-- The address must have been disabled before.
-- @see disable
function objAddr:delete()
  ngx_log(ngx_DEBUG, self.log_prefix, "deleting address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  assert(#self.indices == 0, "Cannot delete address while it owns indices")
  self.host.balancer:callback("removed", self.ip,
                              self.port, self.host.hostname)
  self.host = nil
end

-- changes the weight of an address.
-- requires redistributing indices afterwards
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

-- creates a new address object.
-- @param ip the upstream ip address or target name
-- @param port the upstream port number
-- @param weight the relative weight for the balancer algorithm
-- @param host the host object the address belongs to
-- @return new address object
local newAddress = function(ip, port, weight, host)
  local addr = {
    -- properties
    ip = ip,              -- might be a name (for SRV records)
    ipType = utils.hostnameType(ip),  -- 'ipv4', 'ipv6' or 'name'
    port = port,
    weight = weight,
    available = true,     -- is this target available?
    host = host,          -- the host this address belongs to
    indices = {},         -- the indices of the wheel assigned to this address
    indicesMax = 0,       -- max size reached for 'indices' table
    disabled = false,     -- has this record been disabled? (before deleting)
    log_prefix = host.log_prefix
  }
  for name, method in pairs(objAddr) do addr[name] = method end

  host:addWeight(weight)

  ngx_log(ngx_DEBUG, host.log_prefix, "new address for host '", host.hostname,
          "' created: ", ip, ":", port, " (weight ", weight,")")
  host.balancer:callback("added", ip, port, host.hostname)
  return addr
end


-- ===========================================================================
-- Host object.
-- Manages a single hostname, with DNS resolution and expanding into
-- multiple upstream IPs.
-- ===========================================================================
local objHost = {}

-- define sort order for DNS query results
local sortQuery = function(a,b) return a.__balancerSortKey < b.__balancerSortKey end
local sorts = {
  [dns.TYPE_A] = function(result)
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
  [dns.TYPE_SRV] = function(result)
    local sorted = {}
    -- build table with keys
    for i, v in ipairs(result) do
      sorted[i] = v
      v.__balancerSortKey = string_format("%06d:%s:%s:%s", v.priority, v.target, v.port, v.weight)
    end
    -- sort by the keys
    table_sort(sorted, sortQuery)
    -- reverse index
    for i, v in ipairs(sorted) do sorted[v.__balancerSortKey] = i end
    return sorted
  end,
}
sorts[dns.TYPE_AAAA] = sorts[dns.TYPE_A] -- A and AAAA use the same sorting order
sorts = setmetatable(sorts,{
    -- all record types not mentioned above are unsupported, throw error
    __index = function(self, key)
      error("Unknown/unsupported DNS record type; "..tostring(key))
    end,
  })

-- Queries the DNS for this hostname. Updates the underlying address objects.
-- This method always succeeds, but it might leave the balancer in a 0-weight
-- state if none of the hosts resolves, and hence none of the indices are allocated
-- to 'addresss' objects.
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

    -- TODO: so we got an error, what to do? multiple options;
    -- 1) disable the current information (dropping all indices for this host)
    -- 2) keep running on existing info, until some timeout, and then do 1.
    -- For now option 1.
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
        -- it already existed (same ip, port and weight)
        ngx_log(ngx_DEBUG, self.log_prefix, "unchanged dns record entry for ",
                self.hostname, ": ", (newEntry.target or newEntry.address),
                ":", newEntry.port) -- port = nil for A or AAAA records
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

  if dirty then -- changes imply we need to redistribute indices
    ngx_log(ngx_DEBUG, self.log_prefix, "updating wheel based on dns changes for ",
            self.hostname)

    -- recalculate to move indices of disabled addresses
    self.balancer:redistributeIndices()
    -- delete addresses previously disabled
    self:deleteAddresses()
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "querying dns and updating for ", self.hostname, " completed")
  return true
end

-- Changes the host overall weight. It will also update the parent balancer object.
-- To be called whenever an address is added/removed/changed
function objHost:addWeight(delta)
  self.weight = self.weight + delta
  self.balancer:addWeight(delta)
end

-- Updates the host nodeWeight.
-- @return `true` if something changed and indices must be redistributed
function objHost:change(newWeight)
  local dirty = false
  self.nodeWeight = newWeight
  local lastQuery = self.lastQuery or {}
  if #lastQuery > 0 then
    if lastQuery[1].type == dns.TYPE_SRV and not lastQuery.__ttl0Flag then
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
-- @param entry (table) DNS entry
function objHost:addAddress(entry)
  local weight = entry.weight  -- nil for anything else than SRV
  if weight == 0 then
    -- Special case: SRV with weight = 0 should be included, but with
    -- the lowest possible probability of being hit. So we force it to
    -- weight 1.
    weight = 1
  end
  local addresses = self.addresses
  addresses[#addresses+1] = newAddress(
    entry.address or entry.target,
    (entry.port ~= 0 and entry.port) or self.port,
    weight or self.nodeWeight,
    self
  )
end

-- Looks up and disables an `address` object from the `host`.
-- @param entry (table) DNS entry
-- @return address object that was disabled
function objHost:disableAddress(entry)
  -- first lookup address object
  for _, addr in ipairs(self.addresses) do
    if (addr.ip == (entry.address or entry.target)) and
        addr.port == (entry.port or self.port) and
        not addr.disabled then
      -- found it
      addr:disable()
      return addr
    end
  end
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
-- Host can only be deleted after recalculating the indices!
-- @return true
function objHost:disable()
  -- set weights to 0
  for _, addr in ipairs(self.addresses) do
    addr:disable()
  end

  return true
end

-- Cleans up a host. Only when its weight is 0.
-- Should only be called AFTER recalculating indices
-- @return true or throws an error if weight is non-0
function objHost:delete()
  assert(self.weight == 0, "Cannot delete a host with a non-0 weight")

  for i = #self.addresses, 1, -1 do  -- reverse traversal as we're deleting
    self.addresses[i]:delete()
  end

  self.balancer = nil
end

-- Gets address and port number from a specific address owned by the host.
-- The address/index MUST be owned by the host. Call balancer:getPeer, never this one.
-- access: balancer:getPeer->address->host->returns addr+port
function objHost:getPeer(cacheOnly, address)

  if (self.lastQuery.expire or 0) < time() and not cacheOnly then
    -- ttl expired, so must renew
    self:queryDns(cacheOnly)

    if (address or empty).host ~= self then
      -- our index has been reallocated to another host/address, so recurse to start over
      ngx_log(ngx_DEBUG, self.log_prefix, "index previously assigned to ", self.hostname,
              " was reassigned to another due to a dns update")
      return nil, ERR_INDEX_REASSIGNED
    end
  end

  return address:getPeer(cacheOnly)
end

-- creates a new host object.
-- A host refers to a host name. So a host can have multiple addresses.
-- @param hostname the upstream hostname (as used in dns queries)
-- @param port the upstream port number for A and AAAA dns records. For SRV records the reported port by the DNS server will be used.
-- @param weight the relative weight for the balancer algorithm to assign to each A or AAAA dns record. For SRV records the reported weight by the DNS server will be used.
-- @param balancer the balancer object the host belongs to
-- @return new host object, does not fail.
local newHost = function(hostname, port, weight, balancer)
  local host = {
    -- properties
    hostname = hostname,
    port = port,          -- if set, the port to use for A and AAAA records
    weight = 0,           -- overall weight of all addresses within this hostname
    nodeWeight = weight,  -- weight for entries by this host, for A and AAAA records only
    balancer = balancer,  -- the balancer this host belongs to
    lastQuery = nil,      -- last succesful dns query performed
    lastSorted = nil,     -- last succesful dns query, sorted for comparison
    addresses = {},       -- list of addresses (address objects) this host resolves to
    expire = nil,         -- time when the dns query this host is based upon expires
    log_prefix = balancer.log_prefix
  }
  for name, method in pairs(objHost) do host[name] = method end

  -- insert into our parent balancer before recalculating (in queryDns)
  -- This should actually be a responsibility of the balancer object, but in
  -- this case we do it here, because it is needed before we can redistribute
  -- the indices in the queryDns method just below.
  balancer.hosts[#balancer.hosts+1] = host

  ngx_log(ngx_DEBUG, balancer.log_prefix, "created a new host for: ", hostname)

  host:queryDns()

  return host
end


-- ===========================================================================
-- Balancer object.
-- Manages a set of hostnames, to balance the requests over.
-- ===========================================================================

local objBalancer = {}

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

-- Recalculates the weights. Updates the indices assigned for all hostnames.
-- Must be called whenever a weight might have changed; added/removed hosts.
-- @return balancer object
function objBalancer:redistributeIndices()
  local totalWeight = self.weight
  local movingIndexList = self.unassignedWheelIndices

  -- NOTE: calculations are based on the "remaining" indices and weights, to
  -- prevent issues due to rounding: eg. 10 equal systems with 19 indices.
  -- Calculated to get each 1.9 indices => 9 systems would get 1, last system would get 10
  -- by using "remaining" indices, the first would get 1 index, the other 9 would get 2.

  -- first; reclaim extraneous indices
  local weightLeft = totalWeight
  local indicesLeft = self.wheelSize
  local addList = {}      -- addresses that need additional indices
  local addListCount = {} -- how many extra indices the address needs
  local addCount = 0
  local dropped, added = 0, 0

  for weight, address, _ in self:addressIter() do

    local count
    if weightLeft == 0 then
      count = 0
    else
      count = math_floor(indicesLeft * (weight / weightLeft) + 0.0001) -- 0.0001 to bypass float arithmetic issues
    end
    local drop = #address.indices - count
    if drop > 0 then
      -- we need to reclaim some indices
      address:dropIndices(movingIndexList, drop)
      dropped = dropped + drop
    elseif drop < 0 then
      -- this one needs extra indices, so record the changes needed
      addCount = addCount + 1
      addList[addCount] = address
      addListCount[addCount] = -drop  -- negate because we need to add them
    end
    indicesLeft = indicesLeft - count
    weightLeft = weightLeft - weight
  end

  -- second: add freed indices to the recorded addresses that were short of them
  for i, address in ipairs(addList) do
    address:addIndices(movingIndexList, addListCount[i])
    added = added + addListCount[i]
  end

  ngx_log( #movingIndexList == 0 and ngx_DEBUG or ngx_WARN,
          self.log_prefix, "redistributed indices, size=", self.wheelSize,
          ", dropped=", dropped, ", assigned=", added,
          ", left unassigned=", #movingIndexList)

  return self
end

--- Adds a host to the balancer. If the name resolves to multiple entries,
-- each entry will be added, all with the same weight. So adding an A record
-- with 2 entries, with weight 10, will insert both entries with weight 10,
-- and increase to overall balancer weight by 20. The one exception is that if
-- it resolves to a record with `ttl=0`, then it will __always__ only insert a single
-- (unresolved) entry. The unresolved entry will be resolved by `getPeer` when
-- requested.
--
-- Resolving will be done by the dns clients `resolve` method. Which will
-- dereference CNAME record if set to, but it will not dereference SRV records.
-- An unresolved SRV `target` field will also be resolved by `getPeer` when requested.
--
-- Only the wheel indices assigned to the newly added targets will loose their
-- consistency, all other wheel indices are guaranteed to remain the same.
--
-- Within a balancer the combination of `hostname` and `port` must be unique, so
-- multiple calls with the same target will only update the `weight` of the
-- existing entry.
--
-- __multi-server/worker consistency__; to keep multiple servers/workers consistent it is
-- important to apply all modifications to the ring-balancer in the exact same
-- order on each system. Also the initial `order` list must be the same. And; do
-- not use names, only ip adresses, as dns resolution is non-deterministic
-- across servers/workers.
-- @function addHost
-- @param hostname hostname to add (note: not validated, as long as it's a
-- string it will be accepted, but remain unresolved!)
-- @param port (optional) the port to use (defaults to 80 if omitted)
-- @param weight (optional) relative weight for A/AAAA records (defaults to
-- 10 if omitted), and will be ignored in case of an SRV record.
-- @return balancer object, or throw an error on bad input
function objBalancer:addHost(hostname, port, weight)
  assert(type(hostname) == "string", "expected a hostname (string), got "..tostring(hostname))
  port = port or DEFAULT_PORT
  weight = weight or DEFAULT_WEIGHT
  assert(type(weight) == "number" and
         math_floor(weight) == weight and
         weight >= 1,
         "Expected 'weight' to be an integer >= 1; got "..tostring(weight))

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
    newHost(hostname, port, weight, self)
  else
    -- this one already exists, update if different
    ngx_log(ngx_DEBUG, self.log_prefix, "host ", hostname, ":", port,
            " already exists, updating weight ",
            host.nodeWeight, "-> ",weight)

    if host.nodeWeight ~= weight then
      -- weight changed, go update
      if host:change(weight) then
        -- update had an impact so must redistribute indices
        self:redistributeIndices()
      end
    end
  end

  if #self.unassignedWheelIndices == 0 then
    self.unassignedWheelIndices = {}  -- replace table because of initial memory footprint
  end
  return self
end

--- Removes a host from a balancer. All assigned indices will be redistributed
-- to the remaining targets. Only the indices from the removed host will loose
-- their consistency, all other indices are guaranteed to remain in place.
-- Will not throw an error if the hostname is not in the current list.
--
-- See `addHost` for multi-server consistency.
-- @function removeHost
-- @param hostname hostname to remove
-- @param port port to remove (optional, defaults to 80 if omitted)
-- @return balancer object, or an error on bad input
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
      self:redistributeIndices()

      -- remove host
      host:delete()
      table_remove(self.hosts, i)
      break
    end
  end
  if #self.unassignedWheelIndices == 0 then
    self.unassignedWheelIndices = {}  -- replace table because of initial memory footprint
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
-- be renewed and as a consequence the ring-balancer might be updated.
--
-- If the wheel index that is requested holds either an unresolved SRV entry (where
-- the `target` field contains a name instead of an ip), or a record with `ttl=0` then
-- this call will perform the final query to resolve to an ip using the `toip`
-- function of the dns client (this also invokes the loadbalancing as done by
-- the `toip` function).
-- @function getPeer
-- @param hashValue (optional) number for consistent hashing, round-robins if
-- omitted. The hashValue must be an (evenly distributed) `integer >= 0`. See also `hash`.
-- @param retryCount should be 0 (or `nil`) on the initial try, 1 on the first
-- retry, etc. If provided, it will be added to the `hashValue` to make it fall-through.
-- @param cacheOnly If truthy, no dns lookups will be done, only cache.
-- @return `ip + port + hostname`, or `nil+error`
function objBalancer:getPeer(hashValue, retryCount, cacheOnly)
  local pointer
  if self.weight == 0 then
    return nil, ERR_NO_PEERS_AVAILABLE
  end

  -- calculate starting point
  if hashValue then
    hashValue = hashValue + (retryCount or 0)
    pointer = 1 + (hashValue % self.wheelSize)
  else
    -- no hash, so get the next one, round-robin like
    pointer = self.pointer
    if pointer < self.wheelSize then
      self.pointer = pointer + 1
    else
      self.pointer = 1
    end
  end

  local initial_pointer = pointer
  while true do
    local address = self.wheel[pointer]
    local ip, port, hostname = address.host:getPeer(cacheOnly, address)
    if ip then
      return ip, port, hostname
    elseif port == ERR_INDEX_REASSIGNED then
      -- we just need to retry the same index, no change for 'pointer', just
      -- in case of dns updates, we need to check our weight again.
      if self.weight == 0 then
        return nil, ERR_NO_PEERS_AVAILABLE
      end
    elseif port == ERR_ADDRESS_UNAVAILABLE then
      -- fall through to the next wheel index
      if hashValue then
        pointer = pointer + 1
        if pointer > self.wheelSize then pointer = 1 end
      else
        pointer = self.pointer
        if pointer < self.wheelSize then
          self.pointer = pointer + 1
        else
          self.pointer = 1
        end
      end
      if pointer == initial_pointer then
        -- we went around, but still nothing...
        return nil, ERR_NO_PEERS_AVAILABLE
      end
    else
      -- an unknown error occured
      return nil, port
    end
  end

end

--- Sets the current status of the peer.
-- This allows to temporarily suspend peers when they are offline/unhealthy,
-- it will not alter the index distribution. The parameters passed in should
-- be previous results from `getPeer`.
-- @param available `true` for enabled/healthy, `false` for disabled/unhealthy
-- @param ip ip address of the peer
-- @param port the port of the peer (in address object, not as recorded with the Host!)
-- @param hostname (optional, defaults to the value of `ip`) the hostname
-- @return `true` on success, or `nil+err` if not found
function objBalancer:setPeerStatus(available, ip, port, hostname)
  hostname = hostname or ip
  local name_srv = {}
  for _, addr, host in self:addressIter() do
    if host.hostname == hostname and addr.port == port then
      if addr.ip == ip then
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
  local msg = ("no peer found by name '%s' and address %s:%s"):format(hostname, ip, tostring(port))
  if name_srv[1] then
    -- no match, but we did find a named one, so making the message more explicit
    msg = msg .. ", possibly the IP originated from these nested dns names: " ..
          table_concat(name_srv, ",")
    ngx_log(ngx_WARN, self.log_prefix, msg)
  end
  return nil, msg
end

-- Timer invoked to check for failed queries
local function timerCallback(premature, self)
  if premature then return end

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
    self.requeryRunning = false
  else
    -- not done yet, reschedule timer
    ngx_log(ngx_DEBUG, self.log_prefix, "requery failure, rescheduling timer")
    local ok, err = gctimer(self.requeryInterval, timerCallback, self)
    if not ok then
      ngx_log(ngx_ERR, self.log_prefix, "failed to create the timer: ", err)
    end
  end
end

-- Starts the requery timer.
function objBalancer:startRequery()
  if self.requeryRunning then return end  -- already running, nothing to do here

  self.requeryRunning = true
  local ok, err = gctimer(self.requeryInterval, timerCallback, self)
  if not ok then
    ngx_log(ngx_ERR, self.log_prefix, "failed to create the timer: ", err)
  end
end

--- Sets an event callback for user code.
-- @param callback a function called when an address is added (after DNS
-- resolution for example). Signature of the callback is
-- `function(balancer, action, ip, port, hostname)`, where `ip` might also
-- be a hostname if the DNS resolution returns another name (usually in
-- SRV records). The `action` parameter will be either "added" or "removed".
function objBalancer:setCallback(callback)
  assert(type(callback) == "function", "expected a callback function")
  self.callback = callback
end

local randomlist_cache = {}

local function randomlist(size)
  if randomlist_cache[size] then
    return randomlist_cache[size]
  end
  -- create a new randomizer with just any seed, we do not care about
  -- uniqueness, only about distribution, and repeatability, each orderlist
  -- must be identical!
  local randomizer = lrandom.new(158841259)
  local rnds = new_tab(size, 0)
  local out = new_tab(size, 0)
  for i = 1, size do
    local n = math_floor(randomizer() * size) + 1
    while rnds[n] do
      n = n + 1
      if n > size then
        n = 1
      end
    end
    out[i] = n
    rnds[n] = true
  end
  randomlist_cache[size] = out
  return out
end

--- Creates a new balancer. The balancer is based on a wheel with a number of
-- positions (the index on the wheel). The
-- indices will be randomly distributed over the targets. The number of indices
-- assigned will be relative to the weight.
--
-- A single balancer can hold multiple hosts. A host can be an ip address or a
-- name. As such each host can have multiple targets (or actual ip+port
-- combinations).
--
-- The options table has the following fields;
--
-- - `hosts` (optional) containing hostnames, ports and weights. If omitted,
-- ports and weights default respectively to 80 and 10. The list will be sorted
-- before being added, so the order of entry is deterministic.
-- - `wheelSize` (optional) for total number of positions in the balancer (the
-- indices), if omitted
-- the size of `order` is used, or 1000 if `order` is not provided. It is important
-- to have enough indices to keep the ring properly randomly distributed. If there
-- are to few indices for the number of targets then the load distribution might
-- become to coarse. Consider the maximum number of targets expected, as new
-- hosts can be dynamically added, and dns renewals might yield larger record
-- sets. The `wheelSize` cannot be altered, only a new wheel can be created, but
-- then all consistency would be lost. On a similar note; making it too big,
-- will have a performance impact when the wheel is modified as too many indices
-- will have to be moved between targets. A value of 50 to 200 indices per entry
-- seems about right.
-- - `order` (optional) if given, a list of random numbers, size `wheelSize`, used to
-- randomize the wheel. Duplicates are not allowed in the list.
-- - `dns` (required) a configured `dns.client` object for querying the dns server.
-- - `requery` (optional) interval of requerying the dns server for previously
-- failed queries. Defaults to 1 if omitted (in seconds)
-- - `ttl0` (optional) Maximum lifetime for records inserted with ttl=0, to verify
-- the ttl is still 0. Defaults to 60 if omitted (in seconds)
-- - `callback` (optional) a function called when an address is added (after dns
-- resolution for example). Signature of the callback is
-- `function(balancer, action, ip, port, hostname)`, where `ip` might also be a hostname if the
-- dns resolution returns another name (usually in SRV records). The `action` parameter
-- will be either "added" or "removed".
-- @param opts table with options
-- @return new balancer object or nil+error
-- @usage -- hosts
-- local hosts = {
--   "kong.com",                                        -- name only, as string
--   { name = "gelato.io" },                            -- name only, as table
--   { name = "getkong.org", port = 80, weight = 25 },  -- fully specified, as table
-- }
_M.new = function(opts)
  assert(type(opts) == "table", "Expected an options table, but got: "..type(opts))
  assert(opts.dns, "expected option `dns` to be a configured dns client")
  if (not opts.wheelSize) and opts.order then
    opts.wheelSize = #opts.order
  end
  if opts.order then
    assert(opts.order and (opts.wheelSize == #opts.order), "mismatch between size of 'order' and 'wheelSize'")
  end
  assert((opts.requery or 1) > 0, "expected 'requery' parameter to be > 0")
  assert((opts.ttl0 or 1) > 0, "expected 'ttl0' parameter to be > 0")
  assert(type(opts.callback) == "function" or type(opts.callback) == "nil",
    "expected 'callback' to be a function or nil, but got: " .. type(opts.callback))

  balancer_id_counter = balancer_id_counter + 1
  local self = {
    -- properties
    log_prefix = "[ringbalancer " .. tostring(balancer_id_counter) .. "] ",
    hosts = {},    -- a table, index by both the hostname and index, the value being a host object
    weight = 0,    -- total weight of all hosts
    wheel = nil,   -- wheel with entries (fully randomized)
    pointer = nil, -- pointer to next-up index for the round robin scheme
    wheelSize = opts.wheelSize or 1000, -- number of entries (indices) in the wheel
    dns = opts.dns,  -- the configured dns client to use for resolving
    unassignedWheelIndices = nil, -- list to hold unassigned indices (initially, and when all hosts fail)
    requeryRunning = false,  -- requery timer is not running, see `startRequery`
    requeryInterval = opts.requery or REQUERY_INTERVAL,  -- how often to requery failed dns lookups (seconds)
    ttl0Interval = opts.ttl0 or TTL_0_RETRY, -- refreshing ttl=0 records
    callback = opts.callback or function() end, -- callback for address mutations
  }
  for name, method in pairs(objBalancer) do self[name] = method end
  self.wheel = new_tab(self.wheelSize, 0)
  self.unassignedWheelIndices = new_tab(self.wheelSize, 0)
  self.pointer = math.random(1, self.wheelSize)  -- ensure each worker starts somewhere else

  -- Create a list of entries, and randomize them.
  local unassignedWheelIndices = self.unassignedWheelIndices
  local duplicateCheck = new_tab(self.wheelSize, 0)
  local orderlist = opts.order or randomlist(self.wheelSize)

  for i = 1, self.wheelSize do
    local order = orderlist[i]
    if duplicateCheck[order] then  -- no duplicates allowed! order must be deterministic!
      -- it was a user provided value, so error out
      error("the 'order' list contains duplicates")
    end
    duplicateCheck[order] = true

    unassignedWheelIndices[i] = order
  end

  -- Sort the hosts, to make order deterministic
  local hosts = {}
  for i, host in ipairs(opts.hosts or empty) do
    if type(host) == "table" then
      hosts[i] = host
    else
      hosts[i] = { name = host }
    end
  end
  table_sort(hosts, function(a,b) return (a.name..":"..(a.port or "") < b.name..":"..(b.port or "")) end)
  -- Insert the hosts
  for _, host in ipairs(hosts) do
    local ok, err = self:addHost(host.name, host.port, host.weight)
    if not ok then
      return ok, "Failed creating a balancer: "..tostring(err)
    end
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "balancer created")
  return self
end


--- Creates a MD5 hash value from a string.
-- The string will be hashed using MD5, and then shortened to 4 bytes.
-- The returned hash value can be used as input for the `getpeer` function.
-- @function hashMd5
-- @param str (string) value to create the hash from
-- @return 32-bit numeric hash
_M.hashMd5 = function(str)
  local md5 = ngx_md5(str)
  return bxor(
    tonumber(string_sub(md5, 1, 4), 16),
    tonumber(string_sub(md5, 5, 8), 16)
  )
end


--- Creates a CRC32 hash value from a string.
-- The string will be hashed using CRC32. The returned hash value can be
-- used as input for the `getpeer` function. This is simply a shortcut to
-- `ngx.crc32_short`.
-- @function hashCrc32
-- @param str (string) value to create the hash from
-- @return 32-bit numeric hash
_M.hashCrc32 = ngx.crc32_short


return _M
