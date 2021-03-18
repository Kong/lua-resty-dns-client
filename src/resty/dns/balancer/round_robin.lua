--------------------------------------------------------------------------
-- Round-Robin balancer
--
-- __NOTE:__ This documentation only described the altered user
-- methods/properties, see the `user properties` from the `balancer_base`
-- for a complete overview.
--
-- @author Vinicius Mignot
-- @copyright 2021 Kong Inc. All rights reserved.
-- @license Apache 2.0


local balancer_base = require "resty.dns.balancer.base"

local ngx_log = ngx.log
local ngx_DEBUG = ngx.DEBUG
local random = math.random

local MAX_WHEEL_SIZE = 2^32


local _M = {}
local roundrobin_balancer = {}
local random_indexes = {}


-- calculate the greater common divisor, used to find the smallest wheel
-- possible
local function gcd(a, b)
  if b == 0 then
    return a
  end

  return gcd(b, a % b)
end

--- get a list of random indexes
-- @param count number of random indexes
-- @return table with random indexes
local function get_random_indexes(count)
  -- if new wheel is smaller than before redo the indexes, else just add more
  if count < #random_indexes then
    random_indexes = {}
  end

  -- create a list of missing indexes
  local seq = {}
  for i = #random_indexes + 1, count do
    table.insert(seq, i)
  end

  -- randomize missing indexes
  for i = #random_indexes + 1, count do
    local index = random(#seq)
    random_indexes[i] = seq[index]
    table.remove(seq, index)
  end

  return random_indexes
end


function roundrobin_balancer:afterHostUpdate(host)
  local new_wheel = {}
  local total_points = 0
  local total_weight = 0
  local addr_count = 0
  local divisor = 0
  local indexes

  -- calculate the gcd to find the proportional weight of each address
  for host_idx = 1, #self.hosts do
    local host = self.hosts[host_idx]
    for addr_idx = 1, #host.addresses do
      addr_count = addr_count + 1
      local address_weight = host.addresses[addr_idx].weight
      divisor = gcd(divisor, address_weight)
      total_weight = total_weight + address_weight
    end
  end

  if divisor > 0 then
    total_points = total_weight / divisor
  end

  if total_points == 0 then
    ngx_log(ngx_DEBUG, self.log_prefix, "trying to set a round-robin balancer with no addresses")
    return
  end


  -- get wheel indexes
  -- note: if one of the addresses has much greater weight than the others
  -- it is not relevant to randomize the indexes
  if total_points/divisor < 100 then
    -- get random indexes so the addresses are distributed in the wheel
    indexes = get_random_indexes(total_points)
  end

  local wheel_index = 1
  for host_idx = 1, #self.hosts do
    local host = self.hosts[host_idx]
    for addr_idx = 1, #host.addresses do
      local address_points = host.addresses[addr_idx].weight / divisor
      for _ = 1, address_points do
        local actual_index = indexes and indexes[wheel_index] or wheel_index
        new_wheel[actual_index] = host.addresses[addr_idx]
        wheel_index = wheel_index + 1
      end
    end
  end

  self.wheel = new_wheel
  self.wheelSize = total_points
  self.weight = total_weight

end


function roundrobin_balancer:getPeer(cacheOnly, handle, hashValue)
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

  local starting_pointer = self.pointer
  local address
  local ip, port, hostname
  repeat
    self.pointer = self.pointer + 1

    if self.pointer > self.wheelSize then
      self.pointer = 1
    end

    address = self.wheel[self.pointer]
    if address ~= nil and address.available and not address.disabled then
      ip, port, hostname = address:getPeer(cacheOnly)
      if ip then
        -- success, update handle
        handle.address = address
        return ip, port, hostname, handle

      elseif port == balancer_base.errors.ERR_DNS_UPDATED then
        -- if healty we just need to try again
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

  until self.pointer == starting_pointer
end


function _M.new(opts)
  assert(type(opts) == "table", "Expected an options table, but got: "..type(opts))
  if not opts.log_prefix then
    opts.log_prefix = "round-robin"
  end

  local self = assert(balancer_base.new(opts))

  for name, method in pairs(roundrobin_balancer) do
    self[name] = method
  end

  -- inject additional properties
  self.pointer = 1 -- pointer to next-up index for the round robin scheme
  self.wheelSize = 0
  self.maxWheelSize = opts.maxWheelSize or opts.wheelSize or MAX_WHEEL_SIZE
  self.wheel = {}

  for _, host in ipairs(opts.hosts or {}) do
    local new_host = type(host) == "table" and host or { name = host }
    local ok, err = self:addHost(new_host.name, new_host.port, new_host.weight)
    if not ok then
      return ok, "Failed creating a balancer: " .. tostring(err)
    end
  end

  ngx_log(ngx_DEBUG, self.log_prefix, "round_robin balancer created")

  return self

end

return _M
