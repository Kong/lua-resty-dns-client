--------------------------------------------------------------------------
-- DNS based loadbalancer.
--
--
-- See `./examples/` for examples and output returned.
--
-- @author Thijs Schreijer
-- @copyright Mashape Inc. All rights reserved.
-- @license Apache 2.0

local DEFAULT_WEIGHT = 10   -- default weight for a host, if not provided
local DEFAULT_PORT = 80     -- Default port to use (A and AAAA only) when not provided

local dns = require "dns.client"

local dumptree = function(balancer, marker)
  local pref = ""
  if marker then 
    pref = "   "
    print(string.rep("=",30).."\n"..marker.."\n"..string.rep("=",30))
  end
  for _, host in ipairs(balancer.hosts) do
    print(pref..host.hostname)
    for _, address in ipairs(host.addresses) do
      print(pref.."   ",address.ip," with ", #address.slots," slots")
    end
  end
end

local _M = {}

--[[
set a fixed number of slots up-front, generate a set of random numbers of this size (store centrally)
based on the weights
each host name gets a number of slots from the random list.
The first added host goes first, then the second and so on.
Note: kong should not maintain a list of nodes, but a history of adding and removing nodes
By replaying that, the exact same sequence can be recreated on every kong node.

adding a node should then probably write a current snapshot, followed by the change. Based on timestamps all nodes
can only apply the change, while new nodes can replay from the snapshot onwards.
--]]

-----------------------------------------------------------------------------
-- address object.
-- Manages an ip address. It links to a `host`, and is associated with a number
-- of slots managed by the `balancer`.
-----------------------------------------------------------------------------
local mt_addr = {}

--- Adds a list of slots to the address. The slots added to the address will be removed from 
-- the provided `slotList`.
-- @param slotList a list of slots to be added
-- @param count the number of slots to take from the list provided, defaults to ALL if omitted
-- @return the address object
function mt_addr:addSlots(slotList, count)
  count = count or #slotList
  if count > 0 then
    local slots = self.slots
    local size = #slots
    if count > #slotList then 
      error("more slots requested to be added ("..count..") than provided ("..#slotList..
            ") for host '"..self.host.hostname..":"..self.port.."' ("..self.ip..")") 
    end
    
    local lsize = #slotList + 1
    for i = 1, count do
      local idx = lsize - i
      local slot = slotList[idx]
      slotList[idx] = nil
      slots[size + i] = slot
      
      -- TODO: implement whatever needs to be done with the added slot...
      slot.address = self
    
    end
  end
  return self
end

--- Drop an amount of slots and return them to the overall balancer.
-- @param slotList The list to add the dropped slots to
-- @param count (optional) The number of slots to drop, defaults to ALL if omitted
-- @return slotList with added to it the slots removed from this address
function mt_addr:dropSlots(slotList, count)
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
      
      -- TODO: implement whatever needs to be done with the removed slot...
      slot.address = nil
    
    end
  end
  return slotList
end

--- disables an address object from the balancer.
-- It will set its weight to 0 and the `deleteMe` flag to `true`. So the next slot-recalculation
-- will delete the address.
-- @see delete
function mt_addr:disable()
  -- weight to 0; force dropping all slots assigned, before actually removing
  self.host:addWeight(-self.weight)
  self.weight = 0       
--  self.deleteMe = true
end

--[[- deletes an address object.
-- The address must have been disabled before. Deleting is automatically done after weight recalculation.
-- @see disable
function mt_addr:delete()
  assert(self.deleteMe, "Cannot delete address that hasn't been disabled first")
  self.host = nil
  -- todo: remove from host as well? any other references to clean up?
end --]]

--- creates a new address object.
-- @param ip the upstream ip address
-- @param port the upstream port number
-- @param weight the relative weight for the balancer algorithm
-- @param host the host object the address belongs to
-- @return new address object
local newAddress = function(ip, port, weight, host)
  local addr = { 
    -- properties
    ip = ip,
    port = port,
    weight = weight,
    host = host,          -- the host this address belongs to
    slots = {},           -- the slots assigned to this address
    index = nil,          -- reverse index in the ordered part of the containing balancer
--    deleteMe = nil,       -- when `true` a recalculation will remove this address from the balancer
  }
  for name, method in pairs(mt_addr) do addr[name] = method end
  
  host:addWeight(weight) 
  return addr
end


-----------------------------------------------------------------------------
-- Host object.
-- Manages a single hostname, with DNS resolution and expanding into 
-- multiple upstream IPs.
-----------------------------------------------------------------------------
local mt_host = {}

-- define sort order for DNS query results
local sortQuery = function(a,b) return a._balancer_sortkey < b._balancer_sortkey end
local sorts = {
  [dns.TYPE_A] = function(result)
    local sorted = {}
    -- build table with keys
    for i, v in ipairs(result) do
      sorted[i] = v
      v._balancer_sortkey = v.address
    end
    -- sort by the keys
    table.sort(sorted, sortQuery)
    -- reverse index
    for i, v in ipairs(sorted) do sorted[v._balancer_sortkey] = i end
    return sorted
  end,
  [dns.TYPE_SRV] = function(result)
    local sorted = {}
    -- build table with keys
    for i, v in ipairs(result) do
      sorted[i] = v
      v._balancer_sortkey = string.format("%06d:%s:%s:%s", v.priority, v.target, v.port, v.weight)
    end
    -- sort by the keys
    table.sort(sorted, sortQuery)
    -- reverse index
    for i, v in ipairs(sorted) do sorted[v._balancer_sortkey] = i end
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

--- Queries the DNS for this hostname. Updates the underlying address objects.
-- @return `true` when something changed and a recalculation of the weights is required
function mt_host:queryDns()
  local dns = self.balancer.dns
  local oldQuery = self.lastQuery or {}
  local oldSorted = self.lastSorted or {}
  
  local newQuery, err = dns.stdError(dns.resolve(self.hostname))
  if err then
    -- TODO: dns lookup failed... what TODO, set weight to 0? after how much time to retry? keep failure count, with a max before failing?
    return nil, err
  end
  
  -- we're using the dns' own cache to check for changes.
  -- if our previous result is the same table as the current result, then nothing changed
  if oldQuery == newQuery then return false end  -- exit, nothing changed
  
  -- a new dns record, was returned, but contents could still be the same, so check for changes
  -- sort table in unique order
  local rtype = newQuery[1].type
  local newSorted = sorts[rtype](newQuery)
  local changed = false
  
  if rtype ~= (oldSorted[1] or {}).type then
    -- DNS recordtype changed; recycle everything
    for i = #oldSorted, 1, -1 do  -- reverse order because we're deleting items
      self:removeAddress(oldSorted[i])
    end
    for _, entry in ipairs(newSorted) do -- use sorted table for deterministic order
      self:addAddress(entry)
    end
    changed = true
  else
    local topPriority = newSorted[1].priority
    local done = {}
    local dCount = 0
    for _, newEntry in ipairs(newSorted) do
      if newEntry.priority ~= topPriority then break end -- exit when priority changes, as SRV only uses top priority
      
      local key = newEntry._balancer_sortkey
      local oldEntry = oldSorted[oldSorted[key] or "__key_not_found__"]
      if not oldEntry then
        -- it's a new entry
        self:addAddress(newEntry)
        changed = true
      else
        -- it already existed
        done[key] = true
        dCount = dCount + 1
      end
    end
    if dCount ~= #oldSorted then
      -- not all existing entries we're handled, remove the ones that are not in the
      -- new query result
      for _, entry in ipairs(oldSorted) do
        if not done[entry._balancer_sortkey] then
          self:removeAddress(entry)
        end
      end
      changed = true
    end
  end
  
  self.lastQuery = newQuery
  self.lastSorted = newSorted
  return changed
end

--- Changes the host overall weight. It will also update the parent balancer object.
-- To be called whenever an address is added/removed/changed
function mt_host:addWeight(delta)
  self.weight = self.weight + delta
  self.balancer:addWeight(delta)
end

--- Adds an `address` object to the `host`.
-- @param entry (table) DNS entry
function mt_host:addAddress(entry)
  local weight = entry.weight or self.nodeWeight
  local port = entry.port or self.port
  local addr = newAddress(entry.address, port, weight, self)
  local addresses = self.addresses
  addresses[#addresses+1] = addr
end

--- Removes an `address` object from the `host`.
-- @param entry (table) DNS entry
function mt_host:removeAddress(entry)
  error("not implemented")
  self:addWeight(-entry.weight) 
  
  -- TODO: implement
  
end

--- creates a new host object.
-- A host refers to a host name. So a host can have multiple addresses.
-- @param hostname the upstream hostname (as used in dns queries)
-- @param port the upstream port number for A and AAAA dns records. For SRV records the reported port by the DNS server will be used.
-- @param weight the relative weight for the balancer algorithm to assign to each A or AAAA dns record. For SRV records the reported weight by the DNS server will be used.
-- @param balancer the balancer object the host belongs to
-- @return new host object, or nil+error on failure
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
  }
  for name, method in pairs(mt_host) do host[name] = method end

  local _, err = host:queryDns()
  if err then
    host.weight = 0
    -- TODO: log resolver error
    return nil, "dns query for '"..tostring(hostname).."' failed; "..tostring(err)
  end
  if host.weight == 0 then
    -- we didn't get any nodes, so report error
    return nil, "total weight == 0 for '"..tostring(hostname).."'"
  end
  return host
end


-----------------------------------------------------------------------------
-- Balancer object.
-- Manages a set of hostnames, to balance the requests over.
-----------------------------------------------------------------------------

local mt_balancer = {}

--- Address iterator.
-- Iterates over all addresses in the balancer (nested through the hosts)
-- @return weight (number), address (address object), host (host object the address belongs to)
function mt_balancer:addressIter()
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

--- Recalculates the weights. Updates the slot lists for all hostnames.
-- Must be called whenever a weight might have changed; added/removed hosts. 
-- @return balancer object
function mt_balancer:redistributeSlots()
  local totalWeight = self.weight
  local slotList = {}
  
  -- NOTE: calculations are based on the "remaining" slots and weights, to prevent issues due to rounding;
  -- eg. 10 equal systems with 19 slots.
  -- Calculated to get each 1.9 slots => 9 systems would get 1, last system would get 10
  -- by using "remaining" slots, the first would get 1 slot, the other 9 would get 2.
  
  -- first iteration; reclaim extraneous slots
  local weightLeft = totalWeight
  local slotsLeft = self.wheelSize
  for weight, address, host in self:addressIter() do
    local count
    if weightLeft == 0 then
      count = 0
    else
      count = math.floor(slotsLeft * (weight / weightLeft) + 0.0001) -- 0.0001 to bypass float arithmetic issues
    end
    address:dropSlots(slotList, #address.slots - count)
    slotsLeft = slotsLeft - count
    weightLeft = weightLeft - weight
  end 
  -- second iteration; add the slots freed up to the hosts that are short in slots
  weightLeft = totalWeight
  slotsLeft = self.wheelSize
  for weight, address, host in self:addressIter() do
    local count
    if weightLeft == 0 then
      count = 0
    else
      count = math.floor(slotsLeft * (weight / weightLeft) + 0.0001) -- 0.0001 to bypass float arithmetic issues
    end
    address:addSlots(slotList, count - #address.slots)
    slotsLeft = slotsLeft - count
    weightLeft = weightLeft - weight
  end
  self.dirty = false
  return self
end

-- see `addHost`
local function _addHost(self, hostname, port, weight)
  assert(type(hostname) == "string", "expected a hostname (string), got "..tostring(hostname))
  port = port or DEFAULT_PORT
  weight = weight or DEFAULT_WEIGHT
  assert(weight and math.floor(weight) >= 1, "Expected 'weight' to be a number equal to, or greater than 1")

  for _, host in ipairs(self.hosts) do
    if host.hostname == hostname and host.port == port then
      error("duplicate entry, hostname entry already exists; "..tostring(hostname)..", port "..tostring(port))
    end
  end
  
  self.hosts[#self.hosts+1] = assert(newHost(hostname, port, weight, self))
  return self
end

--- Adds a host to the balancer.
-- @param hostname hostname to add
-- @param port (optional) the port to use (defaults to 80 if omitted)
-- @param weight (optional) relative weight (defaults to 10 if omitted)
-- @return balancer object, or throw an error if it already is in the list
function mt_balancer:addHost(hostname, port, weight)
  local result, err = _addHost(self, hostname, port, weight)
  if self.dirty then
    self:redistributeSlots()
  end
  return result, err
end

--- Removes a host from a balancer. Will not throw an error if the
-- hostname is not in the current list
-- @param hostname hostname to remove
-- @param port port to remove (optional, defaults to 80 if omitted)
-- @return balancer object
function mt_balancer:removeHost(hostname, port)
  assert(type(hostname) == "string", "expected a hostname, got "..tostring(hostname))
  port = port or DEFAULT_PORT
  for i, host in ipairs(self.hosts) do
    if host.hostname == hostname and host.port == port then
      assert(#self.hosts > 1, "cannot remove the last host, at least one must remain")
      
      -- set weights to 0
      for _, addr in ipairs(host.addresses) do
        addr:disable()
      end
      -- recalculate
      self:redistributeSlots()
      
      -- remove host and update the references
      host.balancer = nil
      table.remove(self.hosts, i)
      break
    end
  end
  return self
end

--- Updates the total weight. Will mark the balancer as 'dirty' for recalculation of the slots
-- @param delta the in/decrease of the overall weight (negative for decrease)
function mt_balancer:addWeight(delta)
  self.weight = self.weight + delta
  self.dirty = true
end

--- Gets the next host according to the loadbalancing scheme.
-- @return hostname/ip and port
function mt_balancer:getPeer()
  -- get the next one
  local pointer = (self.pointer or 0) + 1
  if pointer > self.wheelSize then pointer = 1 end
  self.pointer = pointer
  local slot = self.wheel[pointer]
  
  local hostname, port = slot.host:getPeer(slot)
  if not hostname then
    -- Host could not deliver a name/ip, so something changed in the setup, try again
    return self:getPeer()
  end
  return hostname, port
end

--- Creates a new balancer. The balancer is based on a wheel with slots. The slots will be randomly distributed
-- over the hosts. The number of slots assigned will be relative to the weight.
-- 
-- The options table has the following fields;
--
-- - `hosts` (required) containing hostnames and (optional) weights, must have at least one entry.
-- - `wheelsize` (required) for total number of slots in the balancer
-- - `dns` (required) a configured `dns.client` object for querying the dns server
-- @param opts table with options
-- @return new balancer object or nil+error
_M.new = function(opts)
  assert(type(opts) == "table", "Expected an options table, but got; "..type(opts))
  assert(type(opts.hosts) == "table", "expected option 'hosts' to be a table")
  assert(#opts.hosts > 0, "at least one host entry is required in the 'hosts' option")
  assert(opts.dns, "expected option `dns` to be a configured dns client")
  
  local self = {
    -- properties
    hosts = {},    -- a table, index by both the hostname and index, the value being a host object
    weight = 0  ,  -- total weight of all hosts
    dirty = false, -- if true, the slots need to be recalculated
    wheel = {},    -- wheel with entries (fully randomized)
    slots = {},    -- list of slots in no particular order
    wheelSize = opts.wheelsize or 1000, -- number of entries in the wheel
    dns = opts.dns,
  }
  for name, method in pairs(mt_balancer) do self[name] = method end

  -- Create a list of entries, and randomize them.
  -- 'slots' is just for tracking the individual entries, no notion of order is necessary
  -- 'wheel' is fully randomized, no matter how 'slots' is modified, 'wheel' remains random.
  -- Create the wheel
  local wheel = self.wheel
  local slots = self.slots
  local slotList = {}
  local duplicate_check = {}
  for i = 1, self.wheelSize do
    
    local slot = {}
    local order = math.random()
    while duplicate_check[order] do  -- no duplicates allowed! order must be deterministic!
      order = math.random()
    end
    slot.order = order           -- the order in the slot wheel
    slot.address = nil           -- the address this slot belongs to (set by `addSlots` and `dropSlots` methods)
    
    slots[i] = slot
    wheel[i] = slot
    slotList[i] = slot
  end
  -- sort the wheel, randomizing the order of the slots
  table.sort(wheel, function(a,b) return a.order < b.order end)
  for i, slot in ipairs(wheel) do
    slot.order = i               -- replace by order id (float by integer)
  end
  
  -- Sort the hosts, to make order deterministic
  local hosts = {}
  for i, host in ipairs(opts.hosts) do
    if type(host) == "table" then
      hosts[i] = host
    else
      hosts[i] = { name = host }
    end
  end
  table.sort(hosts, function(a,b) return a.name < b.name end)
  -- Insert the hosts
  for _, host in ipairs(hosts) do
    _addHost(self, host.name, host.port, host.weight)
  end
  
  assert(self.weight > 0, "cannot create balancer with weight == 0, invalid hostnames?")
  
  self.hosts[1].addresses[1]:addSlots(slotList)   -- initially insert all slots into the first address
  self:redistributeSlots()                        -- redistribute the slots to all addresses
--  dumptree(self,"final recalculation")
  
  return self
end

return _M
