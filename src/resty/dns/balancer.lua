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
-- have no slots assigned, and hence will never be hit by `getPeer`), which will
-- be actively refreshed using a timer. 
--
-- Whenever dns resolution fails for a hostname, the host will relinguish all
-- the slots it owns, and they will be reassigned to other targets. 
-- Periodically the query for the hostname will be retried, and if it succeeds
-- it will get slots reassigned to it.
--
-- __Housekeeping__; the ring-balancer does some house keeping and may insert
-- some extra fields in dns records. Those fields will, similar to the `toip`
-- function, have an `__` prefix (double underscores).
--
-- @author Thijs Schreijer
-- @copyright Mashape Inc. All rights reserved.
-- @license Apache 2.0


local DEFAULT_WEIGHT = 10   -- default weight for a host, if not provided
local DEFAULT_PORT = 80     -- Default port to use (A and AAAA only) when not provided
local TTL_0_RETRY = 60      -- Maximum life-time for hosts added with ttl=0, requery after it expires
local REQUERY_INTERVAL = 1  -- Interval for requerying failed dns queries

local dns = require "resty.dns.client"
local utils = require "resty.dns.utils"
local empty = setmetatable({}, 
  {__newindex = function() error("The 'empty' table is read-only") end})

local time = ngx.now
local table_sort = table.sort
local table_remove = table.remove
local math_floor = math.floor
local math_random = math.random
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_DEBUG = ngx.DEBUG
local ngx_WARN = ngx.WARN
local log_prefix = "[ringbalancer] "

local _M = {}


-- ===========================================================================
-- address object.
-- Manages an ip address. It links to a `host`, and is associated with a number
-- of slots managed by the `balancer`.
-- ===========================================================================
local objAddr = {}

-- Returns the peer info.
-- @return ip-address, port and hostname of the target
function objAddr:getPeer(cacheOnly)
  if self.ipType == "name" then
    -- SRV type record with a named target
    local ip, port = dns.toip(self.ip, self.port, cacheOnly)
    return ip, port, self.host.name
  else
    -- just an IP address
    return self.ip, self.port, self.host.hostname
  end
end

-- Adds a list of slots to the address. The slots added to the address will be removed from 
-- the provided `slotList`.
-- @param slotList a list of slots to be added
-- @param count the number of slots to take from the list provided, defaults to ALL if omitted
-- @return the address object
function objAddr:addSlots(slotList, count)
  count = count or #slotList
  if count > 0 then
    local slots = self.slots
    local size = #slots
    if count > #slotList then 
      error("more slots requested to be added ("..count..") than provided ("..#slotList..
            ") for host '"..self.host.hostname..":"..self.port.."' ("..tostring(self.ip)..")") 
    end
    
    local lsize = #slotList + 1
    for i = 1, count do
      local idx = lsize - i
      local slot = slotList[idx]
      slotList[idx] = nil
      slots[size + i] = slot
      
      slot.address = self
    end
  end
  return self
end

-- Drop an amount of slots and return them to the overall balancer.
-- @param slotList The list to add the dropped slots to
-- @param count (optional) The number of slots to drop, defaults to ALL if omitted
-- @return slotList with added to it the slots removed from this address
function objAddr:dropSlots(slotList, count)
  local slots = self.slots
  local size = #slots
  count = count or size
  if count > 0 then
    if count > size then 
      error("more slots requested to drop ("..count..") than available ("..size..
            ") in address '"..self.host.hostname..":"..self.port.."' ("..self.ip..")") 
    end
    
    local lsize = #slotList
    for i = 1, count do
      local idx = size + 1 - i
      local slot = slots[idx]
      slots[idx] = nil
      slotList[lsize + i] = slot
      
      slot.address = nil
    end
  end
  return slotList
end

-- disables an address object from the balancer.
-- It will set its weight to 0, so the next slot-recalculation
-- can delete the address by calling `delete`.
-- @see delete
function objAddr:disable()
  ngx_log(ngx_DEBUG, log_prefix, "disabling address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  -- weight to 0; force dropping all slots assigned, before actually removing
  self.host:addWeight(-self.weight)
  self.weight = 0
  self.disabled = true
end

-- cleans up an address object.
-- The address must have been disabled before.
-- @see disable
function objAddr:delete()
  ngx_log(ngx_DEBUG, log_prefix, "deleting address: ", self.ip, ":", self.port,
          " (host ", (self.host or empty).hostname, ")")

  assert(#self.slots == 0, "Cannot delete address while it contains slots")
  self.host = nil
end

-- changes the weight of an address.
-- requires redistributing slots afterwards
function objAddr:change(newWeight)
  ngx_log(ngx_DEBUG, log_prefix, "changing address weight: ", self.ip, ":", self.port,
          "(host ", (self.host or empty).hostname, ") ",
          self.weight, " -> ", newWeight)

  self.host:addWeight(newWeight - self.weight)
  self.weight = newWeight
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
    host = host,          -- the host this address belongs to
    slots = {},           -- the slots assigned to this address
    index = nil,          -- reverse index in the ordered part of the containing balancer
    disabled = false,     -- has this record been disabled? (before deleting)
  }
  for name, method in pairs(objAddr) do addr[name] = method end
  
  host:addWeight(weight)

  ngx_log(ngx_DEBUG, log_prefix, "new address for host '",host.hostname,"' created: ", ip, ":", port, " (weight ",weight,")")
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
      v.__balancerSortKey = string.format("%06d:%s:%s:%s", v.priority, v.target, v.port, v.weight)
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
-- state if none of the hosts resolves, and hence none of the slots are allocated
-- to 'addresss' objects.
-- @return `true`, always succeeds
function objHost:queryDns(cacheOnly)

  ngx_log(ngx_DEBUG, log_prefix, "querying dns for ", self.hostname)

  -- first thing we do is the dns query, this is the only place we possibly
  -- yield (cosockets in the dns lib). So once that is done, we're 'atomic'
  -- again, and we shouldn't have any nasty race conditions
  local dns = self.balancer.dns
  local newQuery, err = dns.stdError(dns.resolve(self.hostname, nil, cacheOnly))

  local oldQuery = self.lastQuery or {}
  local oldSorted = self.lastSorted or {}
  
  if err then
    ngx_log(ngx_WARN, log_prefix, "querying dns for ", self.hostname, 
            " failed: ", err)

    -- TODO: so we got an error, what to do? multiple options;
    -- 1) disable the current information (dropping all slots for this host)
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
    ngx_log(ngx_DEBUG, log_prefix, "no dns changes detected for ", self.hostname)

    return true    -- exit, nothing changed
  end

  if (newQuery[1] or empty).ttl == 0 then
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
      ngx_log(ngx_DEBUG, log_prefix, "no dns changes detected for ", 
              self.hostname, ", still using ttl=0")
      return true
    end
    ngx_log(ngx_DEBUG, log_prefix, "ttl=0 detected for ", 
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
    ngx_log(ngx_DEBUG, log_prefix, "blank dns record for ", 
              self.hostname, ", assuming A-record")
    rtype = dns.TYPE_A
  end
  local newSorted = sorts[rtype](newQuery)
  local dirty

  if rtype ~= (oldSorted[1] or empty).type then
    -- DNS recordtype changed; recycle everything
    ngx_log(ngx_DEBUG, log_prefix, "dns record type changed for ", 
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
        ngx_log(ngx_DEBUG, log_prefix, "new dns record entry for ", 
                self.hostname, ": ", (newEntry.target or newEntry.address),
                ":", newEntry.port) -- port = nil for A or AAAA records
        self:addAddress(newEntry)
        dirty = true
      else
        -- it already existed (same ip, port and weight)
        ngx_log(ngx_DEBUG, log_prefix, "unchanged dns record entry for ", 
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
          ngx_log(ngx_DEBUG, log_prefix, "removed dns record entry for ", 
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
  
  if dirty then -- changes imply we need to redistribute slots
    ngx_log(ngx_DEBUG, log_prefix, "updating wheel based on dns changes for ", 
            self.hostname)

    -- recalculate to move slots of disabled addresses
    self.balancer:redistributeSlots()
    -- delete addresses previously disabled
    self:deleteAddresses()
  end

  ngx_log(ngx_DEBUG, log_prefix, "querying dns and updating for ", self.hostname, " completed")
  return true
end

-- Changes the host overall weight. It will also update the parent balancer object.
-- To be called whenever an address is added/removed/changed
function objHost:addWeight(delta)
  self.weight = self.weight + delta
  self.balancer:addWeight(delta)
end

-- Updates the host nodeWeight.
-- @return `true` if something changed and slots must be redistributed
function objHost:change(newWeight)
  local dirty = false
  self.nodeWeight = newWeight
  local lastQuery = self.lastQuery or {}
  if #lastQuery > 0 then
    if lastQuery[1].type == dns.TYPE_SRV and not lastQuery.__ttl0Flag then
      -- this is an SRV record (and not a fake ttl=0 one), which 
      -- carries its own weight setting, so nothing to update
      ngx_log(ngx_DEBUG, log_prefix, "ignoring weight change for ", self.hostname,
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
  local addresses = self.addresses
  addresses[#addresses+1] = newAddress(
    entry.address or entry.target, 
    entry.port or self.port, 
    entry.weight or self.nodeWeight, 
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
      table.remove(self.addresses, i)
    end
  end

  return true
end

-- disables a host, by setting all adressess to 0
-- Host can only be deleted after recalculating the slots!
-- @return true
function objHost:disable()
  -- set weights to 0
  for _, addr in ipairs(self.addresses) do
    addr:disable()
  end
  
  return true
end

-- Cleans up a host. Only when its weight is 0.
-- Should only be called AFTER recalculating slots
-- @return true or throws an error if weight is non-0
function objHost:delete()
  assert(self.weight == 0, "Cannot delete a host with a non-0 weight")
  
  for i = #self.addresses, 1, -1 do  -- reverse traversal as we're deleting
    self.addresses[i]:delete()
  end
  
  self.balancer = nil
end
 
-- Gets address and port number from a specific slot owned by the host.
-- The slot MUST be owned by the host. Call balancer:getPeer, never this one.
-- access: balancer:getPeer->slot->address->host->returns addr+port
function objHost:getPeer(hashValue, cacheOnly, slot)
  
  if (self.lastQuery.expire or 0) < time() and not cacheOnly then
    -- ttl expired, so must renew
    self:queryDns(cacheOnly)

    if slot.address.host ~= self then
      -- our slot has been reallocated to another host, so recurse to start over
      ngx_log(ngx_DEBUG, log_prefix, "slot previously assigned to ", self.hostname,
              " was reassigned to another due to a dns update")
      return self.balancer:getPeer(hashValue, cacheOnly)
    end
  end

  return slot.address:getPeer(cacheOnly)
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
  }
  for name, method in pairs(objHost) do host[name] = method end

  -- insert into our parent balancer before recalculating (in queryDns)
  -- This should actually be a responsibility of the balancer object, but in 
  -- this case we do it here, because it is needed before we can redistribute
  -- the slots in the queryDns method just below.
  balancer.hosts[#balancer.hosts+1] = host

  ngx_log(ngx_DEBUG, log_prefix, "created a new host for: ", hostname)

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

-- Recalculates the weights. Updates the slot lists for all hostnames.
-- Must be called whenever a weight might have changed; added/removed hosts.
-- @return balancer object
function objBalancer:redistributeSlots()
  local totalWeight = self.weight
  local slotList = self.unassignedSlots
  
  -- NOTE: calculations are based on the "remaining" slots and weights, to prevent issues due to rounding;
  -- eg. 10 equal systems with 19 slots.
  -- Calculated to get each 1.9 slots => 9 systems would get 1, last system would get 10
  -- by using "remaining" slots, the first would get 1 slot, the other 9 would get 2.
  
  -- first; reclaim extraneous slots
  local weightLeft = totalWeight
  local slotsLeft = self.wheelSize
  local addList = {}      -- addresses that next additional slots
  local addListCount = {} -- how many extra slots the address needs
  local addCount = 0
  local dropped, added = 0, 0

  for weight, address, host in self:addressIter() do

    local count
    if weightLeft == 0 then
      count = 0
    else
      count = math_floor(slotsLeft * (weight / weightLeft) + 0.0001) -- 0.0001 to bypass float arithmetic issues
    end
    local slots = #address.slots
    local drop = slots - count
    if drop > 0 then
      -- we need to reclaim some slots
      address:dropSlots(slotList, drop)
      dropped = dropped + drop
    elseif drop < 0 then
      -- this one needs extra slots, so record the changes needed
      addCount = addCount + 1
      addList[addCount] = address
      addListCount[addCount] = -drop  -- negate because we need to add them
    end
    slotsLeft = slotsLeft - count
    weightLeft = weightLeft - weight
  end 

  -- second: add freed slots to the recorded addresses that were short of them
  for i, address in ipairs(addList) do
    address:addSlots(slotList, addListCount[i])
    added = added + addListCount[i]
  end

  ngx_log( #slotList == 0 and ngx_DEBUG or ngx_WARN, 
          log_prefix, "redistributed slots, size=", self.wheelSize,
          ", dropped=", dropped, ", assigned=", added,
          ", left unassigned=", #slotList)

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
-- Only the slots assigned to the newly added targets will loose their
-- consistency, all other slots are guaranteed to remain the same.
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
    ngx_log(ngx_DEBUG, log_prefix, "host ", hostname, ":", port,
            " already exists, updating weight ",
            host.nodeWeight, "-> ",weight)

    if host.nodeWeight ~= weight then
      -- weight changed, go update
      if host:change(weight) then
        -- update had an impact so must redistribute slots
        self:redistributeSlots()
      end
    end
  end
  
  return self
end

--- Removes a host from a balancer. All assigned slots will be redistributed 
-- to the remaining targets. Only the slots from the removed host will loose
-- their consistency, all other slots are guaranteed to remain in place.
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

      ngx_log(ngx_DEBUG, log_prefix, "removing host ", hostname, ":", port)

      -- set weights to 0
      host:disable()
  
      -- removing hosts must always be recalculated to make sure
      -- its order is deterministic (only dns updates are not)
      self:redistributeSlots()

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
-- If the dns record attached to the requested slot is expired, then it will 
-- be renewed and as a consequence the ring-balancer might be updated.
--
-- If the slot that is requested holds either an unresolved SRV entry (where
-- the `target` field contains a name instead of an ip), or a record with `ttl=0` then
-- this call will perform the final query to resolve to an ip using the `toip` 
-- function of the dns client (this also invokes the loadbalancing as done by 
-- the `toip` function).
-- @function getPeer
-- @param hashValue (optional) number for consistent hashing, round-robins if 
-- omitted. The hashValue can be an integer from 1 to `wheelSize`, or a float 
-- from 0 up to, but not including, 1.
-- @param cacheOnly If truthy, no dns lookups will be done, only cache.
-- @return `ip + port + hostname`, or `nil+error`
function objBalancer:getPeer(hashValue, cacheOnly)
  local pointer
  if self.weight == 0 then
    return nil, "No peers are available"
  end
  
  if not hashValue then
    -- get the next one
    pointer = (self.pointer or 0) + 1
    if pointer > self.wheelSize then pointer = 1 end
    self.pointer = pointer
  elseif hashValue < 1 then
    pointer = math_floor(self.wheelSize * hashValue)
  else
    pointer = hashValue
  end
  
  local slot = self.wheel[pointer]
  
  return slot.address.host:getPeer(hashValue, cacheOnly, slot)
end

-- Timer invoked to check for failed queries
local function timerCallback(premature, self)
  if premature then return end
  
  ngx_log(ngx_DEBUG, log_prefix, "executing requery timer")

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
    ngx_log(ngx_DEBUG, log_prefix, "requery success, stopping timer")
    self.requeryRunning = false
  else
    -- not done yet, reschedule timer
    ngx_log(ngx_DEBUG, log_prefix, "requery failure, rescheduling timer")
    local ok, err = ngx.timer.at(self.requeryInterval, timerCallback, self)
    if not ok then
      ngx_log(ngx_ERR, "failed to create the timer: ", err)
    end
  end
end

-- Starts the requery timer.
function objBalancer:startRequery()
  if self.requeryRunning then return end  -- already running, nothing to do here
  
  self.requeryRunning = true
  local ok, err = ngx.timer.at(self.requeryInterval, timerCallback, self)
  if not ok then
    ngx_log(ngx_ERR, "failed to create the timer: ", err)
  end
end

--- Creates a new balancer. The balancer is based on a wheel with slots. The 
-- slots will be randomly distributed over the targets. The number of slots 
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
-- - `wheelSize` (optional) for total number of slots in the balancer, if omitted 
-- the size of `order` is used, or 1000 if `order` is not provided. It is important 
-- to have enough slots to keep the ring properly randomly distributed. If there
-- are to few slots for the number of targets then the load distribution might
-- become to coarse. Consider the maximum number of targets expected, as new
-- hosts can be dynamically added, and dns renewals might yield larger record 
-- sets. The `wheelSize` cannot be altered, only a new wheel can be created, but
-- then all consistency would be lost. On a similar note; making it too big, 
-- will have a performance impact when the wheel is modified as too many slots
-- will have to be moved between targets. A value of 50 to 200 slots per entry 
-- seems about right.
-- - `order` (optional) if given, a list of random numbers, size `wheelSize`, used to 
-- randomize the wheel. This entry is solely to support multiple servers with 
-- the same consistency, as it allows to use the same randomization on each
-- server, and hence the same slot assignment. Duplicates are not allowed in
-- the list.
-- - `dns` (required) a configured `dns.client` object for querying the dns server.
-- - `requery` (optional) interval of requerying the dns server for previously 
-- failed queries. Defaults to 1 if omitted (in seconds)
-- - `ttl0` (optional) Maximum lifetime for records inserted with ttl=0, to verify
-- the ttl is still 0. Defaults to 60 if omitted (in seconds)
-- @param opts table with options
-- @return new balancer object or nil+error
-- @usage -- hosts
-- local hosts = {
--   "mashape.com",                                     -- name only, as string
--   { name = "gelato.io" },                            -- name only, as table
--   { name = "getkong.org", port = 80, weight = 25 },  -- fully specified, as table
-- }
_M.new = function(opts)
  assert(type(opts) == "table", "Expected an options table, but got; "..type(opts))
  --assert(type(opts.hosts) == "table", "expected option 'hosts' to be a table")
  --assert(#opts.hosts > 0, "at least one host entry is required in the 'hosts' option")
  assert(opts.dns, "expected option `dns` to be a configured dns client")
  if (not opts.wheelSize) and opts.order then
    opts.wheelSize = #opts.order
  end
  if opts.order then
    assert(opts.order and (opts.wheelSize == #opts.order), "mismatch between size of 'order' and 'wheelSize'")
  end
  assert((opts.requery or 1) > 0, "expected 'requery' parameter to be > 0")
  assert((opts.ttl0 or 1) > 0, "expected 'ttl0' parameter to be > 0")
  
  local self = {
    -- properties
    hosts = {},    -- a table, index by both the hostname and index, the value being a host object
    weight = 0  ,  -- total weight of all hosts
    wheel = {},    -- wheel with entries (fully randomized)
    slots = {},    -- list of slots in no particular order
    wheelSize = opts.wheelSize or 1000, -- number of entries in the wheel
    dns = opts.dns,  -- the configured dns client to use for resolving
    unassignedSlots = {}, -- list to hold unassigned slots (initially, and when all hosts fail)
    requeryRunning = false,  -- requery timer is not running, see `startRequery`
    requeryInterval = opts.requery or REQUERY_INTERVAL,  -- how often to requery failed dns lookups (seconds)
    ttl0Interval = opts.ttl0 or TTL_0_RETRY -- refreshing ttl=0 records
  }
  for name, method in pairs(objBalancer) do self[name] = method end

  -- Create a list of entries, and randomize them.
  -- 'slots' is just for tracking the individual entries, no notion of order is necessary
  -- 'wheel' is fully randomized, no matter how 'slots' is modified, 'wheel' remains random.
  -- Create the wheel
  local wheel = self.wheel
  local slots = self.slots
  local slotList = self.unassignedSlots
  local duplicateCheck = {}
  local empty = {}
  for i = 1, self.wheelSize do
    
    local slot = {}
    local order = (opts.order or empty)[i] or math_random()
    while duplicateCheck[order] do  -- no duplicates allowed! order must be deterministic!
      if (opts.order or empty)[i] then -- it was a user provided value, so error out
        error("the 'order' list contains duplicates")
      end
      order = math_random()
    end
    duplicateCheck[order] = true
    slot.order = order           -- the order in the slot wheel
    slot.address = nil           -- the address this slot belongs to (set by `addSlots` and `dropSlots` methods)
    
    slots[i] = slot
    wheel[i] = slot
    slotList[i] = slot
  end
  -- sort the wheel, randomizing the order of the slots
  table_sort(wheel, function(a,b) return a.order < b.order end)
  for i, slot in ipairs(wheel) do
    slot.order = i               -- replace by order id (float by integer)
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
  -- setup initial slotlist
  self.unassignedSlots = slotList  -- will be picked up by first call to redistributeSlots
  -- Insert the hosts
  for _, host in ipairs(hosts) do
    local ok, err = self:addHost(host.name, host.port, host.weight)
    if not ok then
      return ok, "Failed creating a balancer: "..tostring(err)
    end
  end

  return self
end

return _M
