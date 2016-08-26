
assert:set_parameter("TableFormatLevel", 5) -- when displaying tables, set a bigger default depth

local icopy = require("pl.tablex").icopy

------------------------
-- START TEST HELPERS --
------------------------
local dnscache, client, balancer

local gettime, sleep
if ngx then
  gettime = ngx.now
  sleep = ngx.sleep
else
  local socket = require("socket")
  gettime = socket.gettime
  sleep = socket.sleep
end

-- creates an SRV record in the cache
local dnsSRV = function(records)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for i, record in ipairs(records) do
    record.type = client.TYPE_SRV
    
    -- check required input
    assert(record.target, "target field is required for SRV record")
    assert(record.name, "name field is required for SRV record")
    assert(record.port, "port field is required for SRV record")
    record.name = record.name:lower()
    
    -- optionals, insert defaults
    record.weight = record.weight or 10
    record.ttl = record.ttl or 600
    record.priority = record.priority or 20
    record.class = record.class or 1
  end
  -- set timeouts
  records.touch = gettime()
  records.expire = gettime() + records[1].ttl
  
  -- create key, and insert it
  dnscache[records[1].type..":"..records[1].name] = records
  -- insert last-succesful lookup type
  dnscache[records[1].name] = records[1].type
  return records
end

-- creates an A record in the cache
local dnsA = function(records)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for i, record in ipairs(records) do
    record.type = client.TYPE_A
    
    -- check required input
    assert(record.address, "address field is required for A record")
    assert(record.name, "name field is required for A record")
    record.name = record.name:lower()
    
    -- optionals, insert defaults
    record.ttl = record.ttl or 600
    record.class = record.class or 1
  end
  -- set timeouts
  records.touch = gettime()
  records.expire = gettime() + records[1].ttl
  
  -- create key, and insert it
  dnscache[records[1].type..":"..records[1].name] = records
  -- insert last-succesful lookup type
  dnscache[records[1].name] = records[1].type
  return records
end

-- creates an AAAA record in the cache
local dnsAAAA = function(records)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for i, record in ipairs(records) do
    record.type = client.TYPE_AAAA
    
    -- check required input
    assert(record.address, "address field is required for AAAA record")
    assert(record.name, "name field is required for AAAA record")
    record.name = record.name:lower()
    
    -- optionals, insert defaults
    record.ttl = record.ttl or 600
    record.class = record.class or 1
  end
  -- set timeouts
  records.touch = gettime()
  records.expire = gettime() + records[1].ttl
  
  -- create key, and insert it
  dnscache[records[1].type..":"..records[1].name] = records
  -- insert last-succesful lookup type
  dnscache[records[1].name] = records[1].type
  return records
end

-- checks the integrity of a list, returns the length of list + number of non-array keys
local check_list = function(t)
  local size = 0
  local keys = 0
  for i, v in pairs(t) do
    if (type(i) == "number") then
      if (i > size) then size = i end
    else
      keys = keys + 1
    end
  end
  for i = 1, size do
    assert(t[i], "invalid sequence, index "..tostring(i).." is missing")
  end
  return size, keys
end

-- returns number of entries (hash + array part)
local table_size = function(t)
  local s = 0
  for _, _ in pairs(t) do s = s + 1 end
  return s
end

-- checks the integrity of the balancer, hosts, addresses, and slots. returns the balancer.
local check_balancer = function(balancer)
  assert.is.table(balancer)
  -- hosts
  check_list(balancer.hosts)
  -- slots
  local size = check_list(balancer.slots)
  assert.are.equal(balancer.wheelSize, size)
  assert.are.equal(check_list(balancer.wheel), size)
  local templist = {}
  for i, slot in ipairs(balancer.slots) do
    local idx = slot.order
    assert.are.equal(balancer.wheel[idx], slot)
    templist[slot] = i
  end
  assert.are.equal(balancer.wheelSize, table_size(templist))
  for i, slot in ipairs(balancer.wheel) do
    assert.are.equal(slot.order, i)
    templist[slot] = nil
  end
  assert.are.equal(0, table_size(templist))
  -- addresses
  local addrlist = {}
  for i, slot in ipairs(balancer.wheel) do -- calculate slots per address based on the wheel
    addrlist[slot.address] = (addrlist[slot.address] or 0) + 1
  end
  for addr, count in pairs(addrlist) do
    assert.are.equal(#addr.slots, count)
  end
  for i, host in ipairs(balancer.hosts) do -- remove slots per address based on hosts (results in 0)
    for n, addr in ipairs(host.addresses) do
      if addr.weight > 0 then
        for j, slot in ipairs(addr.slots) do
          addrlist[addr] = addrlist[addr] - 1
        end
      end
    end
  end
  for addr, count in pairs(addrlist) do
    assert.are.equal(0, count)
  end
  return balancer
end

-- creates a hash table with "address:port" keys and as value the number of slots
local function count_slots(balancer)
  local r = {}
  for i, slot in ipairs(balancer.wheel) do
    local key = tostring(slot.address.ip)
    if key:find(":",1,true) then
      key = "["..key.."]:"..slot.address.port
    else
      key = key..":"..slot.address.port
    end
    r[key] = (r[key] or 0) + 1
  end
  return r
end

----------------------
-- END TEST HELPERS --
----------------------


describe("Loadbalancer", function()

  setup(function()
    _G.package.loaded["dns.client"] = nil -- make sure module is reloaded
    _G._TEST = true  -- expose internals for test purposes
    balancer = require "dns.balancer"
    client = require "dns.client"
    dnscache = client.__cache
    
    assert(client:init {
      hosts = {}, 
      resolv_conf = {
        "nameserver 8.8.8.8"
      },
    })
  end)

  teardown(function()
    client.purge_cache(0)
  end)
  
  before_each(function()
    client.purge_cache(0)
  end)
  
  describe("unit tests", function()
    it("addressIter", function() 
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      dnsSRV({ 
        { name = "gelato.io", target = "1.2.3.6", port = 8001 },
        { name = "gelato.io", target = "1.2.3.6", port = 8002 },
        { name = "gelato.io", target = "1.2.3.6", port = 8003 },
      })
      local b = balancer.new { 
        hosts = {"mashape.com", "getkong.org", "gelato.io" },
        dns = client,
        wheelsize = 10,
      }
      local count = 0
      for a,b,c in b:addressIter() do count = count + 1 end
      assert.equals(6, count)
    end)
  
    describe("create", function()
      it("fails without proper options", function()
        assert.has.error(
          function() balancer.new() end, 
          "Expected an options table, but got; nil"
        )
        assert.has.error(
          function() balancer.new("should be a table") end,
          "Expected an options table, but got; string"
        )
      end)
      it("fails without proper 'hosts' option", function()
        assert.has.error(
          function() balancer.new({}) end,
          "expected option 'hosts' to be a table"
        )
        assert.has.error(
          function() balancer.new({ hosts = 1 }) end, 
          "expected option 'hosts' to be a table"
        )
        assert.has.error(
          function() balancer.new({ hosts = {} }) end, 
          "at least one host entry is required in the 'hosts' option"
        )
      end)
      it("fails without proper 'dns' option", function()
        assert.has.error(
          function() balancer.new({ hosts = {"mashape.com"} }) end,
          "expected option `dns` to be a configured dns client"
        )
      end)
      it("succeeds with proper options", function()
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
          { name = "mashape.com", address = "1.2.3.5" },
        })
        local b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelsize = 10,
        })
      end)
      it("succeeds with multiple hosts", function()
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        dnsAAAA({ 
          { name = "getkong.org", address = "::1" },
        })
        dnsSRV({ 
          { name = "gelato.io", target = "1.2.3.4", port = 8001 },
        })
        local b = balancer.new { 
          hosts = {"mashape.com", "getkong.org", "gelato.io" },
          dns = client,
          wheelsize = 10,
        }
        check_balancer(b)
      end)
    end)
  end)
  
  

  describe("slot manipulation", function()
    it("equal weights and 'fitting' slots", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      local b = check_balancer(balancer.new { 
        hosts = {"mashape.com"},
        dns = client,
        wheelsize = 10,
      })
      local expected = {
        ["1.2.3.4:80"] = 5,
        ["1.2.3.5:80"] = 5,
      }
      assert.are.same(expected, count_slots(b))
    end)
    it("equal weights and 'non-fitting' slots", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.1" },
        { name = "mashape.com", address = "1.2.3.2" },
        { name = "mashape.com", address = "1.2.3.3" },
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
        { name = "mashape.com", address = "1.2.3.6" },
        { name = "mashape.com", address = "1.2.3.7" },
        { name = "mashape.com", address = "1.2.3.8" },
        { name = "mashape.com", address = "1.2.3.9" },
        { name = "mashape.com", address = "1.2.3.10" },
      })
      local b = check_balancer(balancer.new { 
        hosts = {"mashape.com"},
        dns = client,
        wheelsize = 19,
      })
      local expected = {
        ["1.2.3.1:80"] = 1,
        ["1.2.3.2:80"] = 2,
        ["1.2.3.3:80"] = 2,
        ["1.2.3.4:80"] = 2,
        ["1.2.3.5:80"] = 2,
        ["1.2.3.6:80"] = 2,
        ["1.2.3.7:80"] = 2,
        ["1.2.3.8:80"] = 2,
        ["1.2.3.9:80"] = 2,
        ["1.2.3.10:80"] = 2, 
      }
      assert.are.same(expected, count_slots(b))
    end)
    it("DNS record order has no effect", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.1" },
        { name = "mashape.com", address = "1.2.3.2" },
        { name = "mashape.com", address = "1.2.3.3" },
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
        { name = "mashape.com", address = "1.2.3.6" },
        { name = "mashape.com", address = "1.2.3.7" },
        { name = "mashape.com", address = "1.2.3.8" },
        { name = "mashape.com", address = "1.2.3.9" },
        { name = "mashape.com", address = "1.2.3.10" },
      })
      local b = check_balancer(balancer.new { 
        hosts = {"mashape.com"},
        dns = client,
        wheelsize = 19,
      })
      local expected = count_slots(b)
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.8" },
        { name = "mashape.com", address = "1.2.3.3" },
        { name = "mashape.com", address = "1.2.3.1" },
        { name = "mashape.com", address = "1.2.3.2" },
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
        { name = "mashape.com", address = "1.2.3.6" },
        { name = "mashape.com", address = "1.2.3.9" },
        { name = "mashape.com", address = "1.2.3.10" },
        { name = "mashape.com", address = "1.2.3.7" },
      })
      local b = check_balancer(balancer.new { 
        hosts = {"mashape.com"},
        dns = client,
        wheelsize = 19,
      })
      
      assert.are.same(expected, count_slots(b))
    end)
    it("changing hostname order has no effect", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.1" },
      })
      dnsA({ 
        { name = "getkong.org", address = "1.2.3.2" },
      })
      local b = balancer.new { 
        hosts = {"mashape.com", "getkong.org"},
        dns = client,
        wheelsize = 3,
      }
      local expected = count_slots(b)
      local b = check_balancer(balancer.new { 
        hosts = {"getkong.org", "mashape.com"},  -- changed host order
        dns = client,
        wheelsize = 3,
      })
      assert.are.same(expected, count_slots(b))
    end)
    it("adding a host (non-fitting slots)", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", weight = 5} },
        dns = client,
        wheelsize = 10,
      })
      b:addHost("getkong.org", 8080, 10 )
      check_balancer(b)
      local expected = {
        ["1.2.3.4:80"] = 2,
        ["1.2.3.5:80"] = 2,
        ["[::1]:8080"] = 6,
      }
      assert.are.same(expected, count_slots(b))
    end)
    it("adding a host (fitting slots)", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", port = 80, weight = 5 } },
        dns = client,
        wheelsize = 2000,
      })
      b:addHost("getkong.org", 8080, 10 )
      check_balancer(b)
      local expected = {
        ["1.2.3.4:80"] = 500,
        ["1.2.3.5:80"] = 500,
        ["[::1]:8080"] = 1000,
      }
      assert.are.same(expected, count_slots(b))
    end)
    it("removing a host, slots staying in place", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", port = 80, weight = 5 } },
        dns = client,
        wheelsize = 2000,
      })
      b:addHost("getkong.org", 8080, 10)
      check_balancer(b)
      
      -- copy the first 500 slots, they should not move
      expected1 = {}
      icopy(expected1, b.hosts[1].addresses[1].slots, 1, 1, 500)
      expected2 = {}
      icopy(expected2, b.hosts[1].addresses[2].slots, 1, 1, 500)
      
      b:removeHost("getkong.org")
      check_balancer(b)
      
      -- copy the new first 500 slots as well
      expected1a = {}
      icopy(expected1a, b.hosts[1].addresses[1].slots, 1, 1, 500)
      expected2a = {}
      icopy(expected2a, b.hosts[1].addresses[2].slots, 1, 1, 500)

      -- compare previous copy against current first 500 slots to make sure they are the same
      for i = 1,500 do
        assert(expected1[i] == expected1a[i])
        assert(expected1[i] == expected1a[i])
      end
    end)
    it("removing the last host", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", port = 80, weight = 5 } },
        dns = client,
        wheelsize = 20,
      })
--require("mobdebug").start()
      b:addHost("getkong.org", 8080, 10)
      b:removeHost("getkong.org", 8080)
      assert.has.error(
        function() b:removeHost("mashape.com") end,
        "cannot remove the last host, at least one must remain"
      )
    end)
    pending("renewed DNS A record; no changes", function()
    end)
    pending("renewed DNS AAAA record; no changes", function()
    end)
    pending("renewed DNS SRV record; no changes", function()
    end)
    pending("renewed DNS A record; address changes", function()
    end)
    pending("renewed DNS AAAA record; address changes", function()
    end)
    pending("renewed DNS SRV record; target changes", function()
    end)
    pending("renewed DNS A record; entry added", function()
    end)
    pending("renewed DNS AAAA record; entry added", function()
    end)
    pending("renewed DNS SRV record; entry added", function()
    end)
    pending("renewed DNS A record; entry removed", function()
    end)
    pending("renewed DNS AAAA record; entry removed", function()
    end)
    pending("renewed DNS SRV record; entry removed", function()
    end)
    pending("renewed DNS A record; record removed", function()
    end)
    pending("renewed DNS AAAA record; record removed", function()
    end)
    pending("renewed DNS SRV record; record removed", function()
    end)
    pending("renewed DNS A record; failed", function()
    end)
    pending("renewed DNS AAAA record; failed", function()
    end)
    pending("renewed DNS SRV record; failed", function()
    end)
    pending("DNS record failure", function()
    end)
    pending("DNS record failure, last host", function()
    end)
    pending("low weight with zero-slots assigned", function()
    end)
    pending("ttl of 0 inserts only a single unresolved address", function()
    end)
  end)
end)
