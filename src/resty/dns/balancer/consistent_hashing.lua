--------------------------------------------------------------------------
-- Consistent-Hashing balancer
--
-- This balancer implements a consistent-hashing algorithm based on the
-- Ketama algorithm.
--
-- This load balancer is designed to make sure that every time a load
-- balancer object is built, it is built the same, no matter the order the
-- process is done.
--
-- __NOTE:__ This documentation only described the altered user
-- methods/properties, see the `user properties` from the `balancer_base`
-- for a complete overview.
--
-- @author Vinicius Mignot
-- @copyright 2020 Kong Inc. All rights reserved.
-- @license Apache 2.0


local balancer_base = require "resty.dns.balancer.base"
local xxhash32 = require "luaxxhash"

local floor = math.floor
local ngx_log = ngx.log
local ngx_CRIT = ngx.CRIT
local ngx_DEBUG = ngx.DEBUG
local ngx_ERR = ngx.ERR
local table_sort = table.sort


-- constants
local DEFAULT_CONTINUUM_SIZE = 1000
local MAX_CONTINUUM_SIZE = 2^32
local MIN_CONTINUUM_SIZE = 1000
local SERVER_POINTS = 160 -- number of points when all targets have same weight
local SEP = " " -- string separator to be used when hashing hostnames


local _M = {}
local consistent_hashing = {}


-- returns the index a value will point to in a generic continuum, based on
-- continuum size
local function get_continuum_index(value, points)
  return ((xxhash32(tostring(value)) % points) + 1)
end


-- hosts and addresses must be sorted lexically before adding to the continuum,
-- so they are added always in the same order. This makes sure that collisions
-- will be treated always the same way.
local function sort_hosts_and_addresses(balancer)
  if type(balancer) ~= "table" then
    error("balancer must be a table")
  end

  if balancer.hosts == nil then
    return
  end

  table_sort(balancer.hosts, function(a, b)
    local ta = tostring(a.hostname)
    local tb = tostring(b.hostname)
    return ta < tb or (ta == tb and tonumber(a.port) < tonumber(b.port))
  end)

  for _, host in ipairs(balancer.hosts) do
    table_sort(host.addresses, function(a, b)
      return (tostring(a.ip) .. ":" .. tostring(a.port)) <
             (tostring(b.ip) .. ":" .. tostring(b.port))
    end)
  end

end


--- Adds a host to the balancer.
-- This function checks if there is enough points to add more hosts and
-- then call the base class's `addHost()`.
-- see `addHost()` from the `balancer_base` for more details.
function consistent_hashing:addHost(hostname, port, weight)
  local host_count = #self.hosts + 1

  if (host_count * SERVER_POINTS) >= self.points then
    ngx_log(ngx_ERR, self.log_prefix, "consistent hashing balancer requires ",
            "more entries to be able to add the number of hosts requested, ",
            "please increase the wheel size")
    return nil, "not enough free slots to add more hosts"
  end

  self.super.addHost(self, hostname, port, weight)

  return self
end


--- Actually adds the addresses to the continuum.
-- This function should not be called directly, as it will called by
-- `addHost()` after adding the new host.
-- This function makes sure the continuum will be built identically every
-- time, no matter the order the hosts are added.
function consistent_hashing:afterHostUpdate(host)
  local points = self.points
  local new_continuum = {}
  local total_weight = self.weight
  local host_count = #self.hosts
  local total_collision = 0

  sort_hosts_and_addresses(self)

  for weight, address, h in self:addressIter() do
    local addr_prop = weight / total_weight
    local entries = floor(addr_prop * host_count * SERVER_POINTS)
    if weight > 0 and entries == 0 then
      entries = 1 -- every address with weight > 0 must have at least one entry
    end
    local port = address.port and ":" .. tostring(address.port) or ""
    local i = 1
    while i <= entries do
      local name = tostring(address.ip) .. ":" .. port .. SEP .. tostring(i)
      local index = get_continuum_index(name, points)
      if new_continuum[index] == nil then
        new_continuum[index] = address
      else
        entries = entries + 1 -- move the problem forward
        total_collision = total_collision + 1
      end
      i = i + 1
      if i > self.points then
        -- this should happen only if there are an awful amount of hosts with
        -- low relative weight.
        ngx_log(ngx_CRIT, "consistent hashing balancer requires more entries ",
                "to add the number of hosts requested, please increase the ",
                "wheel size")
        return
      end
    end
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "continuum of size ", self.points,
          " updated with ", total_collision, " collisions")

  self.continuum = new_continuum

end


--- Gets an IP/port/hostname combo for the value to hash
-- This function will hash the `valueToHash` param and use it as an index
-- in the continuum. It will return the address that is at the hashed
-- value or the first one found going counter-clockwise in the continuum.
-- @param cacheOnly If truthy, no dns lookups will be done, only cache.
-- @param handle the `handle` returned by a previous call to `getPeer`.
-- This will retain some state over retries. See also `setAddressStatus`.
-- @param valueToHash value for consistent hashing. Please note that this
-- value will be hashed, so no need to hash it prior to calling this
-- function.
-- @return `ip + port + hostheader` + `handle`, or `nil+error`
function consistent_hashing:getPeer(cacheOnly, handle, valueToHash)
  ngx_log(ngx_DEBUG, self.log_prefix, "trying to get peer with value to hash: [",
          valueToHash, "]")
  if not self.healthy then
    return nil, balancer_base.errors.ERR_BALANCER_UNHEALTHY
  end

  if handle then
    -- existing handle, so it's a retry
    handle.retryCount = handle.retryCount + 1
  else
    -- no handle, so this is a first try
    handle = self:getHandle()  -- no GC specific handler needed
    handle.retryCount = 0
  end

  if not handle.hashValue then
    if not valueToHash then
      error("can't getPeer with no value to hash", 2)
    end
    handle.hashValue = get_continuum_index(valueToHash, self.points)
  end

  local address
  local index = handle.hashValue
  local ip, port, hostname
  while (index - 1) ~= handle.hashValue do
    if index == 0 then
      index = self.points
    end

    address = self.continuum[index]
    if address ~= nil and address.available and not address.disabled then
      ip, port, hostname = address:getPeer(cacheOnly)
      if ip then
        -- success, update handle
        handle.address = address
        return ip, port, hostname, handle

      elseif port == balancer_base.errors.ERR_DNS_UPDATED then
        -- we just need to retry the same index, no change for 'pointer', just
        -- in case of dns updates, we need to check our health again.
        if not self.healthy then
          return nil, balancer_base.errors.ERR_BALANCER_UNHEALTHY
        end
      elseif port == balancer_base.errors.ERR_ADDRESS_UNAVAILABLE then
        ngx_log(ngx_DEBUG, self.log_prefix, "found address but it was unavailable. ",
                " trying next one.")
      else
        -- an unknown error occured
        return nil, port
      end

    end

    index = index - 1
  end

  return nil, balancer_base.errors.ERR_NO_PEERS_AVAILABLE
end

--- Creates a new balancer.
--
-- The balancer is based on a wheel (continuum) with a number of points
-- between MIN_CONTINUUM_SIZE and MAX_CONTINUUM_SIZE points. Key points
-- will be assigned to addresses based on their IP and port. The number
-- of points each address will be assigned is proportional to their weight.
--
-- The options table has the following fields, additional to the ones from
-- the `balancer_base`:
--
-- - `hosts` (optional) containing hostnames, ports, and weights. If
-- omitted, ports and weights default respectively to 80 and 10. The list
-- will be sorted before being added, so the order of entry is
-- deterministic.
-- - `wheelSize` (optional) for total number of positions in the
-- continuum. If omitted `DEFAULT_CONTINUUM_SIZE` is used. It is important
-- to have enough indices to fit all addresses entries, keep in mind that
-- each address will use 160 entries in the continuum (more or less,
-- proportional to its weight, but the total points will always be
-- `160 * addresses`). Consider the maximum number of targets expected, as
-- new hosts can be dynamically added, and DNS renewals might yield
-- larger record sets. The `wheelSize` cannot be altered, the object has
-- to built again to change this value. On a similar note, making it too
-- big will have a performance impact to get peers from the continuum, as
-- the values will be too dispersed among them.
-- @param opts table with options
-- @return new balancer object or nil+error
function _M.new(opts)
  assert(type(opts) == "table", "Expected an options table, but got: "..type(opts))
  if not opts.log_prefix then
    opts.log_prefix = "hash-lb"
  end

  local self = assert(balancer_base.new(opts))

  self.continuum = {}
  self.points = (opts.wheelSize and
                opts.wheelSize >= MIN_CONTINUUM_SIZE and
                opts.wheelSize <= MAX_CONTINUUM_SIZE) and
                opts.wheelSize or DEFAULT_CONTINUUM_SIZE

  -- inject overridden methods
  for name, method in pairs(consistent_hashing) do
    self[name] = method
  end

  for _, host in ipairs(opts.hosts or {}) do
    local new_host = type(host) == "table" and host or { name = host }
    local ok, err = self:addHost(new_host.name, new_host.port, new_host.weight)
    if not ok then
      return ok, "Failed creating a balancer: " .. tostring(err)
    end
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "consistent_hashing balancer created")

  return self
end


--------------------------------------------------------------------------------
-- for testing only

function consistent_hashing:_get_continuum()
  return self.continuum
end


function consistent_hashing:_hit_all()
  for _, address in pairs(self.continuum) do
    if address.host then
      address:getPeer()
    end
  end
end



return _M
