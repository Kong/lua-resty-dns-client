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
local ngx_CRIT = ngx.CRIT
local ngx_DEBUG = ngx.DEBUG

local MAX_WHEEL_SIZE = 2^32


local _M = {}
local roundrobin_balancer = {}


-- calculate the greater common divisor, used to find the smallest wheel
-- possible
local function gcd(a, b)
  if b == 0 then
    return a
  end

  return gcd(b, a % b)
end


function roundrobin_balancer:afterHostUpdate(host)
  local new_wheel = {}
  local addresses = {}
  local total_points = 0
  local total_weight = 0
  local divisor = 0

  for weight, address in self:addressIter() do
    divisor = gcd(divisor, weight)
    table.insert(addresses, { address = address, weight = weight })
  end

  if #addresses < 1 then
    ngx_log(ngx_DEBUG, self.log_prefix, "trying to set a round-robin balancer with no addresses")
    return
  end

  -- set the proportional weight in each address to use least amount of entries
  -- in the wheel
  for i = 1, #addresses do
    total_weight = total_weight + addresses[i].weight
    local address_points = addresses[i].weight/divisor
    total_points = total_points + address_points
    addresses[i].weight = address_points
  end

  if total_points > self.maxWheelSize then
    ngx_log(ngx_CRIT, self.log_prefix, "round-robin balancer requires more ",
                "entries than available, please increase the wheel size or ",
                "use closer host weights")
    return
  end

  -- actually set the wheel entries
  local cur_addr = 1
  for i = 1, total_points do
    local added = false
    while not added do
      if cur_addr > #addresses then
        cur_addr = 1
      end

      -- if not all address weight was added to the wheel, add now
      if addresses[cur_addr].weight > 0 then
        new_wheel[i] = addresses[cur_addr].address
        addresses[cur_addr].weight = addresses[cur_addr].weight - 1
        added = true
      end

      cur_addr = cur_addr + 1
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
