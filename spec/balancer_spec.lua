
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
  if balancer.weight == 0 then
    -- all hosts failed, so the balancer slots have no content
    assert.are.equal(balancer.wheelSize, #balancer.unassignedSlots)
    for i, slot in ipairs(balancer.wheel) do
      assert.is_nil(slot.address)
    end
  else
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

-- copies the wheel to a list with ip, port and hostname in the field values.
-- can be used for before/after comparison
local copyWheel = function(b)
  local copy = {}
  for i, slot in ipairs(b.wheel) do
    copy[i] = i.." - "..slot.address.ip.." @ "..slot.address.port.." ("..slot.address.host.hostname..")"
  end
  return copy
end

----------------------
-- END TEST HELPERS --
----------------------


describe("Loadbalancer", function()
  
  local snapshot
  
  setup(function()
    _G.package.loaded["dns.client"] = nil -- make sure module is reloaded
    _G._TEST = true  -- expose internals for test purposes
    balancer = require "dns.balancer"
    client = require "dns.client"
  end)
  
  before_each(function()
    assert(client:init {
      hosts = {}, 
      resolv_conf = {
        "nameserver 8.8.8.8"
      },
    })
    dnscache = client.getcache()  -- must be after 'init' call because it is replaced
    snapshot = assert:snapshot()
  end)
  
  after_each(function()
    snapshot:revert()  -- undo any spying/stubbing etc.
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
      it("fails without proper 'dns' option", function()
        assert.has.error(
          function() balancer.new({ hosts = {"mashape.com"} }) end,
          "expected option `dns` to be a configured dns client"
        )
      end)
      it("fails with inconsistent wheel and order sizes", function()
        assert.has.error(
          function() balancer.new({
              dns = client,
              hosts = {"mashape.com"},
              wheelsize = 10,
              order = {1,2,3},
            }) end,
          "mismatch between size of 'order' and 'wheelsize'"
        )
      end)
      it("fails with a bad order list", function()
        assert.has.error(
          function() balancer.new({
              dns = client,
              hosts = {"mashape.com"},
              order = {1,2,3,3}, --duplicate
            }) end,
          "the 'order' list contains duplicates"
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
          order = {1,2,3,4,5,6,7,8,9,10},
        })
      end)
      it("succeeds with the right sizes", function()
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        -- based on a given wheelsize
        local b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelsize = 10,
        })
        assert.are.equal(10, b.wheelSize)
        -- based on the order list size
        local b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          order = { 1,2,3 },
        })
        assert.are.equal(3, b.wheelSize)
        -- based on the order list size and wheel size
        local b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelsize = 4,
          order = { 1,2,3,4 },
        })
        assert.are.equal(4, b.wheelSize)
      end)
      it("succeeds without 'hosts' option", function()
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 10,
        })
        assert.are.equal(10, #b.unassignedSlots)
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 10,
          hosts = {},  -- empty hosts table hould work too
        })
        assert.are.equal(10, #b.unassignedSlots)
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
  
    describe("adding hosts", function()
      it("fails if hostname is not a string", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        local not_a_string = 10
        assert.has.error(
          function()
            b:addHost(not_a_string)
          end, 
          "expected a hostname (string), got "..tostring(not_a_string)
        )
        check_balancer(b)
      end)
      it("fails if weight is not a positive integer value", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        local bad_weights = { -1, 0 ,0.5, 1.5, 100.4 }
        for _, weight in ipairs(bad_weights) do 
          assert.has.error(
            function()
              b:addHost("just_a_name", nil, weight)
            end, 
            "Expected 'weight' to be an integer >= 1; got "..tostring(weight)
          )
        end
        check_balancer(b)
      end)
      it("accepts a hostname that does not resolve", function()
        -- weight should be 0, with no addresses
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        ok, err = b:addHost("really.really.does.not.exist.mashape.com", 80, 10)
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        assert.equals(0, b.weight) -- has one failed host, so weight must be 0
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        check_balancer(b:addHost("mashape.com", 80, 10))
        assert.equals(10, b.weight) -- has one succesful host, so weight must equal that one
      end)
      it("fails if the 'hostname:port' combo already exists", function()
        -- returns nil + error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        ok, err = b:addHost("just_a_name", 80)
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        
        ok, err = b:addHost("just_a_name", 81)  -- different port is ok
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        
        ok, err = b:addHost("just_a_name", 80)
        assert.is_nil(ok)
        assert.are.equal("duplicate entry, hostname entry already exists; 'just_a_name', port 80", err)
        check_balancer(b)
      end)
    end)
  
    describe("removing hosts", function()
      it("hostname must be a string", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        local not_a_string = 10
        assert.has.error(
          function()
            b:removeHost(not_a_string)
          end, 
          "expected a hostname (string), got "..tostring(not_a_string)
        )
        check_balancer(b)
      end)
      it("does not throw an error if it doesn't exist", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelsize = 15,
        })
        local ok, err = b:removeHost("not in there")
        assert.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
      end)
    end)
  end)
  
  describe("getting targets", function()
    it("gets an IP address and port number round-robin", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsA({ 
        { name = "getkong.org", address = "5.6.7.8" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          {name = "mashape.com", port = 123, weight = 100},
          {name = "getkong.org", port = 321, weight = 50},
        },
        dns = client,
        wheelsize = 15,
      })
      -- run down the wheel twice
      local res = {}
      for n = 1, 15*2 do
        local addr, port, host = b:getPeer()
        res[addr..":"..port] = (res[addr..":"..port] or 0) + 1
        res[host..":"..port] = (res[host..":"..port] or 0) + 1
      end
      assert.equal(20, res["1.2.3.4:123"])
      assert.equal(20, res["mashape.com:123"])
      assert.equal(10, res["5.6.7.8:321"])
      assert.equal(10, res["getkong.org:321"])
    end)
    it("gets an IP address and port number; consistent hashing", function()
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsA({ 
        { name = "getkong.org", address = "5.6.7.8" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          {name = "mashape.com", port = 123, weight = 100},
          {name = "getkong.org", port = 321, weight = 50},
        },
        dns = client,
        wheelsize = 15,
      })
      -- run down the wheel, hitting all slots once
      local res = {}
      for n = 1, 15 do
        local addr, port, host = b:getPeer(n)
        res[addr..":"..port] = (res[addr..":"..port] or 0) + 1
        res[host..":"..port] = (res[host..":"..port] or 0) + 1
      end
      assert.equal(10, res["1.2.3.4:123"])
      assert.equal(5, res["5.6.7.8:321"])
      -- hit one slot 15 times
      local res = {}
      local hash = 6  -- just pick one
      for n = 1, 15 do
        local addr, port, host = b:getPeer(hash)
        res[addr..":"..port] = (res[addr..":"..port] or 0) + 1
        res[host..":"..port] = (res[host..":"..port] or 0) + 1
      end
      assert(15 == res["1.2.3.4:123"] or nil == res["1.2.3.4:123"], "mismatch")
      assert(15 == res["mashape.com:123"] or nil == res["mashape.com:123"], "mismatch")
      assert(15 == res["5.6.7.8:321"] or nil == res["5.6.7.8:321"], "mismatch")
      assert(15 == res["getkong.org:321"] or nil == res["getkong.org:321"], "mismatch")
    end)
    it("does not hit the resolver when 'cache_only' is set", function()
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", port = 80, weight = 5 } },
        dns = client,
        wheelsize = 10,
      })
      record.expire = gettime() - 1 -- expire current dns cache record
      local rec_new = dnsA({   -- create a new record
        { name = "mashape.com", address = "5.6.7.8" },
      })
      -- create a spy to check whether dns was queried
      local s = spy.on(client, "resolve")
      local hash = nil
      local cache_only = true
      local ip, port, host = b:getPeer(hash, cache_only)
      assert.spy(client.resolve).Not.called_with("mashape.com",nil, nil)
      assert.equal("1.2.3.4", ip)  -- initial un-updated ip address
      assert.equal(80, port)
      assert.equal("mashape.com", host)
    end)
    it("fails if the balancer is 'empty'", function()
      local b = check_balancer(balancer.new { 
        hosts = {},  -- no hosts, so balancer is empty
        dns = client,
        wheelsize = 10,
      })
      local ip, port, host = b:getPeer()
      assert.is_nil(ip)
      assert.equals("No peers are available", port)
      assert.is_nil(host)
      
      local ip, port, host = b:getPeer(6) -- just pick a hash
      assert.is_nil(ip)
      assert.equals("No peers are available", port)
      assert.is_nil(host)
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
        dns = client,
        wheelsize = 20,
      })
      b:addHost("mashape.com", 80, 5)
      b:addHost("getkong.org", 8080, 10)
      b:removeHost("getkong.org", 8080)
      b:removeHost("mashape.com", 80)
    end)
    it("renewed DNS A record; no changes", function()
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 5 }, 
          { name = "getkong.org", port = 123, weight = 10 }, 
        },
        dns = client,
        wheelsize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      local rec_new = dnsA({   -- create a new record (identical)
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      -- create a spy to check whether dns was queried
      local s = spy.on(client, "resolve")
      for i = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      assert.same(state, copyWheel(b))
    end)
  
    pending("renewed DNS AAAA record; no changes", function()
    end)
    pending("renewed DNS SRV record; no changes", function()
    end)
    pending("renewed DNS A record; address changes", function()
        -- previously successful query, fails to resolve, and then succeeds again
        -- record type changed
        -- targets changed 
        -- fewer entries in dns record --> less addresses
        -- empty dns record --> all addresses gone, host weight = 0
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
    pending("low weight with zero-slots assigned", function()
    end)
    pending("ttl of 0 inserts only a single unresolved address", function()
    end)
  end)
end)
