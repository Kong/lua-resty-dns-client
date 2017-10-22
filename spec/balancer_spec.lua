
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
local dnsSRV = function(records, staleTtl)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for _, record in ipairs(records) do
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
  local key = records[1].type..":"..records[1].name
  dnscache:set(key, records, records[1].ttl + (staleTtl or 4))
  -- insert last-succesful lookup type
  dnscache:set(records[1].name, records[1].type)
  return records
end

-- creates an A record in the cache
local dnsA = function(records, staleTtl)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for _, record in ipairs(records) do
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
  local key = records[1].type..":"..records[1].name
  dnscache:set(key, records, records[1].ttl + (staleTtl or 4))
  -- insert last-succesful lookup type
  dnscache:set(records[1].name, records[1].type)
  return records
end

-- creates an AAAA record in the cache
local dnsAAAA = function(records, staleTtl)
  -- if single table, then insert into a new list
  if not records[1] then records = { records } end
  
  for _, record in ipairs(records) do
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
  local key = records[1].type..":"..records[1].name
  dnscache:set(key, records, records[1].ttl + (staleTtl or 4))
  -- insert last-succesful lookup type
  dnscache:set(records[1].name, records[1].type)
  return records
end

-- checks the integrity of a list, returns the length of list + number of non-array keys
local check_list = function(t)
  local size = 0
  local keys = 0
  for i, _ in pairs(t) do
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
    for _, slot in ipairs(balancer.wheel) do
      assert.is_nil(slot.address)
    end
  else
    -- addresses
    local addrlist = {}
    for _, slot in ipairs(balancer.wheel) do -- calculate slots per address based on the wheel
      addrlist[slot.address] = (addrlist[slot.address] or 0) + 1
    end
    for addr, count in pairs(addrlist) do
      assert.are.equal(#addr.slots, count)
    end
    for _, host in ipairs(balancer.hosts) do -- remove slots per address based on hosts (results in 0)
      for _, addr in ipairs(host.addresses) do
        if addr.weight > 0 then
          for _ in ipairs(addr.slots) do
            addrlist[addr] = addrlist[addr] - 1
          end
        end
      end
    end
    for _, count in pairs(addrlist) do
      assert.are.equal(0, count)
    end
  end
  return balancer
end

-- creates a hash table with "address:port" keys and as value the number of slots
local function count_slots(balancer)
  local r = {}
  for _, slot in ipairs(balancer.wheel) do
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

local updateWheelState = function(state, patt, repl)
  for i, entry in ipairs(state) do
    state[i] = entry:gsub(patt, repl, 1)
  end
  return state
end
----------------------
-- END TEST HELPERS --
----------------------


describe("Loadbalancer", function()
  
  local snapshot
  
  setup(function()
    _G.package.loaded["dns.client"] = nil -- make sure module is reloaded
    _G._TEST = true  -- expose internals for test purposes
    balancer = require "resty.dns.balancer"
    client = require "resty.dns.client"
  end)
  
  before_each(function()
    assert(client.init {
      hosts = {}, 
      resolvConf = {
        "nameserver 8.8.8.8"
      },
    })
    dnscache = client.getcache()  -- must be after 'init' call because it is replaced
    snapshot = assert:snapshot()
  end)
  
  after_each(function()
    snapshot:revert()  -- undo any spying/stubbing etc.
    collectgarbage()
    collectgarbage()
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
        wheelSize = 10,
      }
      local count = 0
      for _,_,_ in b:addressIter() do count = count + 1 end
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
      it("fails with a bad 'requery' option", function()
        assert.has.error(
          function() balancer.new({ 
                hosts = {"mashape.com"},
                dns = client,
                requery = -5,
            }) end,
          "expected 'requery' parameter to be > 0"
        )
      end)
      it("fails with a bad 'ttl0' option", function()
        assert.has.error(
          function() balancer.new({ 
                hosts = {"mashape.com"},
                dns = client,
                ttl0 = -5,
            }) end,
          "expected 'ttl0' parameter to be > 0"
        )
      end)
      it("fails with inconsistent wheel and order sizes", function()
        assert.has.error(
          function() balancer.new({
              dns = client,
              hosts = {"mashape.com"},
              wheelSize = 10,
              order = {1,2,3},
            }) end,
          "mismatch between size of 'order' and 'wheelSize'"
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
        check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelSize = 10,
          order = {1,2,3,4,5,6,7,8,9,10},
          requery = 2,
          ttl0 = 5,
        })
      end)
      it("succeeds with the right sizes", function()
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        -- based on a given wheelSize
        local b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelSize = 10,
        })
        assert.are.equal(10, b.wheelSize)
        -- based on the order list size
        b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          order = { 1,2,3 },
        })
        assert.are.equal(3, b.wheelSize)
        -- based on the order list size and wheel size
        b = check_balancer(balancer.new { 
          hosts = {"mashape.com"},
          dns = client,
          wheelSize = 4,
          order = { 1,2,3,4 },
        })
        assert.are.equal(4, b.wheelSize)
      end)
      it("succeeds without 'hosts' option", function()
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelSize = 10,
        })
        assert.are.equal(10, #b.unassignedSlots)
        b = check_balancer(balancer.new { 
          dns = client,
          wheelSize = 10,
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
          wheelSize = 10,
        }
        check_balancer(b)
      end)
    end)
  
    describe("adding hosts", function()
      it("fails if hostname is not a string", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelSize = 15,
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
          wheelSize = 15,
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
          wheelSize = 15,
        })
        assert(b:addHost("really.really.does.not.exist.mashape.com", 80, 10))
        check_balancer(b)
        assert.equals(0, b.weight) -- has one failed host, so weight must be 0
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        check_balancer(b:addHost("mashape.com", 80, 10))
        assert.equals(10, b.weight) -- has one succesful host, so weight must equal that one
      end)
      it("accepts a hostname when dns server is unavailable", function()
        -- This test might show some error output similar to the lines below. This is expected and ok.
        -- 2016/11/07 16:48:33 [error] 81932#0: *2 recv() failed (61: Connection refused), context: ngx.timer
        
        -- reconfigure the dns client to make sure query fails
        assert(client.init {
          hosts = {}, 
          resolvConf = {
            "nameserver 127.0.0.1:22000" -- make sure dns query fails
          },
        })
        -- create balancer
        local b = check_balancer(balancer.new { 
          hosts = { 
            { name = "mashape.com", port = 80, weight = 10 }, 
          },
          dns = client,
          wheelSize = 100,
        })
        assert.equal(0, b.weight)
      end)
      it("updates the weight when 'hostname:port' combo already exists", function()
        -- returns nil + error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelSize = 15,
        })
        dnsA({ 
          { name = "mashape.com", address = "1.2.3.4" },
        })
        local ok, err = b:addHost("mashape.com", 80, 10)
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        assert.equal(10, b.weight)
        
        ok, err = b:addHost("mashape.com", 81, 20)  -- different port
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        assert.equal(30, b.weight)
        
        ok, err = b:addHost("mashape.com", 80, 5)  -- reduce weight by 5
        assert.are.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
        assert.equal(25, b.weight)
      end)
    end)
  
    describe("removing hosts", function()
      it("hostname must be a string", function()
        -- throws an error
        local b = check_balancer(balancer.new { 
          dns = client,
          wheelSize = 15,
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
          wheelSize = 15,
        })
        local ok, err = b:removeHost("not in there")
        assert.equal(b, ok)
        assert.is_nil(err)
        check_balancer(b)
      end)
    end)
  end)
  it("ringbalancer with a running timer gets GC'ed", function()
    local b = check_balancer(balancer.new {
      dns = client,
      wheelSize = 15,
      requery = 0.1,
    })
    assert(b:addHost("this.will.not.be.found", 80, 10))

    local tracker = setmetatable({ b }, {__mode = "v"})
    local t = 0
    while t<10 do
      if t>0.5 then -- let the timer do its work, only dismiss after 0.5 seconds
        b = nil -- mark it for GC
      end
      sleep(0.1)
      collectgarbage()
      if not next(tracker) then
        break
      end
      t = t + 0.1
    end
    assert(t < 10, "timeout while waiting for balancer to be GC'ed")
  end)

  describe("getting targets", function()
    it("gets an IP address, port and hostname for named SRV entries", function()
      -- this case is special because it does a last-minute `toip` call and hence
      -- uses a different code branch
      -- See issue #17
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsSRV({ 
        { name = "gelato.io", target = "mashape.com", port = 8001 },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          {name = "gelato.io", port = 123, weight = 100},
        },
        dns = client,
      })
      -- run down the wheel twice
      local addr, port, host = b:getPeer()
      assert.equal("1.2.3.4", addr)
      assert.equal(8001, port)
      assert.equal("gelato.io", host)
    end)
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
        wheelSize = 15,
      })
      -- run down the wheel twice
      local res = {}
      for _ = 1, 15*2 do
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
        wheelSize = 15,
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
      res = {}
      local hash = 6  -- just pick one
      for _ = 1, 15 do
        local addr, port, host = b:getPeer(hash)
        res[addr..":"..port] = (res[addr..":"..port] or 0) + 1
        res[host..":"..port] = (res[host..":"..port] or 0) + 1
      end
      assert(15 == res["1.2.3.4:123"] or nil == res["1.2.3.4:123"], "mismatch")
      assert(15 == res["mashape.com:123"] or nil == res["mashape.com:123"], "mismatch")
      assert(15 == res["5.6.7.8:321"] or nil == res["5.6.7.8:321"], "mismatch")
      assert(15 == res["getkong.org:321"] or nil == res["getkong.org:321"], "mismatch")
    end)
    it("gets an IP address and port number; consistent hashing wraps (modulo)", function()
      local b = check_balancer(balancer.new {
        hosts = {
          {name = "1.2.3.1", port = 1, weight = 100},
          {name = "1.2.3.2", port = 1, weight = 100},
          {name = "1.2.3.3", port = 1, weight = 100},
          {name = "1.2.3.4", port = 1, weight = 100},
          {name = "1.2.3.5", port = 1, weight = 100},
          {name = "1.2.3.6", port = 1, weight = 100},
          {name = "1.2.3.7", port = 1, weight = 100},
          {name = "1.2.3.8", port = 1, weight = 100},
          {name = "1.2.3.9", port = 1, weight = 100},
          {name = "1.2.3.10", port = 1, weight = 100},
        },
        dns = client,
        wheelSize = 10,
      })
      -- run down the wheel, hitting all slots once
      for n = 0, 9 do
        local addr1, port1, host1 = b:getPeer(n)
        local addr2, port2, host2 = b:getPeer(n+10) -- wraps around, modulo
        assert.equal(addr1, addr2)
        assert.equal(port1, port2)
        assert.equal(host1, host2)
      end
    end)
    it("gets an IP address and port number; consistent hashing with retries", function()
      local b = check_balancer(balancer.new {
        hosts = {
          {name = "1.2.3.1", port = 1, weight = 100},
          {name = "1.2.3.2", port = 1, weight = 100},
          {name = "1.2.3.3", port = 1, weight = 100},
          {name = "1.2.3.4", port = 1, weight = 100},
          {name = "1.2.3.5", port = 1, weight = 100},
          {name = "1.2.3.6", port = 1, weight = 100},
          {name = "1.2.3.7", port = 1, weight = 100},
          {name = "1.2.3.8", port = 1, weight = 100},
          {name = "1.2.3.9", port = 1, weight = 100},
          {name = "1.2.3.10", port = 1, weight = 100},
        },
        dns = client,
        wheelSize = 10,
      })
      -- run down the wheel, slot 0, increasing retry
      local res = {}
      for n = 0, 9 do
        local addr, port, _ = b:getPeer(0, n)
        assert.string(addr)
        assert.number(port)
        local key = addr..":"..port
        res[key] = (res[key] or 0) + 1
      end
      local count = 0
      for _,_ in pairs(res) do count = count + 1 end
      assert.equal(10, count) -- 10 unique entries
    end)
    it("does not hit the resolver when 'cache_only' is set", function()
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { { name = "mashape.com", port = 80, weight = 5 } },
        dns = client,
        wheelSize = 10,
      })
      record.expire = gettime() - 1 -- expire current dns cache record
      dnsA({   -- create a new record
        { name = "mashape.com", address = "5.6.7.8" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      local hash = nil
      local cache_only = true
      local ip, port, host = b:getPeer(hash, nil, cache_only)
      assert.spy(client.resolve).Not.called_with("mashape.com",nil, nil)
      assert.equal("1.2.3.4", ip)  -- initial un-updated ip address
      assert.equal(80, port)
      assert.equal("mashape.com", host)
    end)
    it("fails if the balancer is 'empty'", function()
      local b = check_balancer(balancer.new { 
        hosts = {},  -- no hosts, so balancer is empty
        dns = client,
        wheelSize = 10,
      })
      local ip, port, host = b:getPeer()
      assert.is_nil(ip)
      assert.equals("No peers are available", port)
      assert.is_nil(host)
      
      ip, port, host = b:getPeer(6) -- just pick a hash
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
        wheelSize = 10,
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
        wheelSize = 19,
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
        wheelSize = 19,
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
      b = check_balancer(balancer.new { 
        hosts = {"mashape.com"},
        dns = client,
        wheelSize = 19,
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
        wheelSize = 3,
      }
      local expected = count_slots(b)
      b = check_balancer(balancer.new { 
        hosts = {"getkong.org", "mashape.com"},  -- changed host order
        dns = client,
        wheelSize = 3,
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
        wheelSize = 10,
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
        wheelSize = 2000,
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
        wheelSize = 2000,
      })
      b:addHost("getkong.org", 8080, 10)
      check_balancer(b)
      
      -- copy the first 500 slots, they should not move
      local expected1 = {}
      icopy(expected1, b.hosts[1].addresses[1].slots, 1, 1, 500)
      local expected2 = {}
      icopy(expected2, b.hosts[1].addresses[2].slots, 1, 1, 500)
      
      b:removeHost("getkong.org")
      check_balancer(b)
      
      -- copy the new first 500 slots as well
      local expected1a = {}
      icopy(expected1a, b.hosts[1].addresses[1].slots, 1, 1, 500)
      local expected2a = {}
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
        wheelSize = 20,
      })
      b:addHost("mashape.com", 80, 5)
      b:addHost("getkong.org", 8080, 10)
      b:removeHost("getkong.org", 8080)
      b:removeHost("mashape.com", 80)
    end)
    it("weight change updates properly", function() 
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        dns = client,
        wheelSize = 60,
      })
      b:addHost("mashape.com", 80, 10)
      b:addHost("getkong.org", 80, 10)
      local count = count_slots(b)
      assert.same({
          ["1.2.3.4:80"] = 20,
          ["1.2.3.5:80"] = 20,
          ["[::1]:80"]   = 20,
      }, count)
      
      b:addHost("mashape.com", 80, 25)
      count = count_slots(b)
      assert.same({
          ["1.2.3.4:80"] = 25,
          ["1.2.3.5:80"] = 25,
          ["[::1]:80"]   = 10,
      }, count)
    end)
    it("weight change ttl=0 record, updates properly", function() 
      -- mock the resolve/toip methods
      local old_resolve = client.resolve
      local old_toip = client.toip
      finally(function() 
          client.resolve = old_resolve 
          client.toip = old_toip
        end)
      client.resolve = function(name, ...)
        if name == "mashape.com" then
          local record = dnsA({ 
            { name = "mashape.com", address = "1.2.3.4", ttl = 0 },
          })
          return record
        else
          return old_resolve(name, ...)
        end
      end
      client.toip = function(name, ...)
        if name == "mashape.com" then
          return "1.2.3.4", ...
        else
          return old_toip(name, ...)
        end
      end

      -- insert 2nd address
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9", ttl = 60*60 },
      })

      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 50 }, 
          { name = "getkong.org", port = 123, weight = 50 }, 
        },
        dns = client,
        wheelSize = 100,
        ttl0 = 2,
      })
    
      local count = count_slots(b)
      assert.same({
          ["mashape.com:80"] = 50,
          ["9.9.9.9:123"] = 50,
      }, count)

      -- update weights
      b:addHost("mashape.com", 80, 150)
      
      count = count_slots(b)
      assert.same({
          ["mashape.com:80"] = 75,
          ["9.9.9.9:123"] = 25,
      }, count)
    end)
    it("weight change for unresolved record, updates properly", function() 
      local record = dnsA({ 
        { name = "does.not.exist.mashape.com", address = "1.2.3.4" },
      })
      dnsAAAA({ 
        { name = "getkong.org", address = "::1" },
      })
      local b = check_balancer(balancer.new { 
        dns = client,
        wheelSize = 60,
      })
      b:addHost("does.not.exist.mashape.com", 80, 10)
      b:addHost("getkong.org", 80, 10)
      local count = count_slots(b)
      assert.same({
          ["1.2.3.4:80"] = 30,
          ["[::1]:80"]   = 30,
      }, count)
      
      -- expire the existing record
      record.expire = 0
      record.expired = true
      -- do a lookup to trigger the async lookup
      client.resolve("does.not.exist.mashape.com", {qtype = client.TYPE_A})
      sleep(0.5) -- provide time for async lookup to complete
      
      for _ = 1, b.wheelSize do b:getPeer() end -- hit them all to force renewal
      
      count = count_slots(b)
      assert.same({
          --["1.2.3.4:80"] = 0,  --> failed to resolve, no more entries
          ["[::1]:80"]   = 60,
      }, count)
      
      -- update the failed record
      b:addHost("does.not.exist.mashape.com", 80, 20)
      -- reinsert a cache entry
      dnsA({ 
        { name = "does.not.exist.mashape.com", address = "1.2.3.4" },
      })
      sleep(2)  -- wait for timer to re-resolve the record
      
      count = count_slots(b)
      assert.same({
          ["1.2.3.4:80"] = 40,
          ["[::1]:80"]   = 20,
      }, count)
    end)
    it("weight change SRV record, has no effect", function() 
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsSRV({ 
        { name = "gelato.io", target = "1.2.3.6", port = 8001, weight = 5 },
        { name = "gelato.io", target = "1.2.3.6", port = 8002, weight = 5 },
      })
      local b = check_balancer(balancer.new { 
        dns = client,
        wheelSize = 120,
      })
      b:addHost("mashape.com", 80, 10)
      b:addHost("gelato.io", 80, 10)  --> port + weight will be ignored
      local count = count_slots(b)
      local state = copyWheel(b)
      assert.same({
          ["1.2.3.4:80"]   = 40,
          ["1.2.3.5:80"]   = 40,
          ["1.2.3.6:8001"] = 20,
          ["1.2.3.6:8002"] = 20,
      }, count)
      
      b:addHost("gelato.io", 80, 20)  --> port + weight will be ignored
      count = count_slots(b)
      assert.same({
          ["1.2.3.4:80"]   = 40,
          ["1.2.3.5:80"]   = 40,
          ["1.2.3.6:8001"] = 20,
          ["1.2.3.6:8002"] = 20,
      }, count)
      assert.same(state, copyWheel(b))
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
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsA({   -- create a new record (identical)
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      assert.same(state, copyWheel(b))
    end)
  
    it("renewed DNS AAAA record; no changes", function()
      local record = dnsAAAA({ 
        { name = "mashape.com", address = "::1" },
        { name = "mashape.com", address = "::2" },
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
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsAAAA({   -- create a new record (identical)
        { name = "mashape.com", address = "::1" },
        { name = "mashape.com", address = "::2" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS SRV record; no changes", function()
      local record = dnsSRV({ 
        { name = "gelato.io", target = "1.2.3.6", port = 8001, weight = 5 },
        { name = "gelato.io", target = "1.2.3.6", port = 8002, weight = 5 },
        { name = "gelato.io", target = "1.2.3.6", port = 8003, weight = 5 },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "gelato.io" }, 
          { name = "getkong.org", port = 123, weight = 10 }, 
        },
        dns = client,
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsSRV({    -- create a new record (identical)
        { name = "gelato.io", target = "1.2.3.6", port = 8001, weight = 5 },
        { name = "gelato.io", target = "1.2.3.6", port = 8002, weight = 5 },
        { name = "gelato.io", target = "1.2.3.6", port = 8003, weight = 5 },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("gelato.io",nil, nil)
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS record; different record type", function()
      local record = dnsAAAA({ 
        { name = "mashape.com", address = "::1" },
        { name = "mashape.com", address = "::2" },
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
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsA({   -- create a new record, different type
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      -- update 'state' to match the changes, slots should remain the same
      -- only the content has changed.
      -- Note: when the record changes, all addresses are deleted, in reverse
      -- order. So the slots from '::2' are freed first, followed by '::1'.
      -- So when 1.2.3.5 is added, it gets the ones last freed from '::1'
      -- So order is DETERMINISTIC!
      updateWheelState(state, " %- ::1 @ ", " - 1.2.3.5 @ ")
      updateWheelState(state, " %- ::2 @ ", " - 1.2.3.4 @ ")
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS record; targets changed", function()
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
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsA({   -- create a new record, different type
        { name = "mashape.com", address = "1.2.3.6" },
        { name = "mashape.com", address = "1.2.3.7" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      -- update 'state' to match the changes, slots should remain the same
      -- only the content has changed.
      -- Note: when the record changes, the addresses are deleted in order
      -- so 1.2.3.4 goes first, followed by 1.2.3.5. Adding is also in order
      -- so 1.2.3.6 gets the ones last released by 1.2.3.5
      -- So order is DETERMINISTIC!
      updateWheelState(state, " %- 1%.2%.3%.5 @ ", " - 1.2.3.6 @ ")
      updateWheelState(state, " %- 1%.2%.3%.4 @ ", " - 1.2.3.7 @ ")
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS record; 1 target changed", function()
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
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      dnsA({   -- create a new record, different type
        { name = "mashape.com", address = "1.2.3.5" },
        { name = "mashape.com", address = "1.2.3.6" },
      })
      -- create a spy to check whether dns was queried
      spy.on(client, "resolve")
      for _ = 1, b.wheelSize do -- call all, to make sure we hit the expired one
        b:getPeer()  -- invoke balancer, to expire record and re-query dns
      end
      assert.spy(client.resolve).was_called_with("mashape.com",nil, nil)
      -- update 'state' to match the changes, slots should remain the same
      -- only the content has changed.
      --
      -- Note: order was changed, 1.2.3.5 moved from 2nd to 1st position
      --
      -- One address is replaced by another.
      -- So order is DETERMINISTIC!
      updateWheelState(state, " %- 1%.2%.3%.4 @ ", " - 1.2.3.6 @ ")
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS A record; address changes", function()
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.5" },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
        { name = "getkong.org", address = "8.8.8.8" },
      })
      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 10 }, 
          { name = "getkong.org", port = 123, weight = 10 }, 
        },
        dns = client,
        wheelSize = 100,
      })
      local state = copyWheel(b)
      record.expire = gettime() -1 -- expire current dns cache record
      record = dnsA({              -- insert an updated record
        { name = "mashape.com", address = "1.2.3.4" },
        { name = "mashape.com", address = "1.2.3.6" },  -- target updated
      })
      -- run entire wheel to make sure the expired one is requested, and updated
      for _ = 1, b.wheelSize do b:getPeer() end 
      -- all old 'mashape.com @ 1.2.3.5' should now be 'mashape.com @ 1.2.3.6'
      -- and more important; all others should not have moved slot positions!
      updateWheelState(state, " %- 1%.2%.3%.5 @ ", " - 1.2.3.6 @ ")
      assert.same(state, copyWheel(b))
    end)
    it("renewed DNS A record; failed", function()
      -- This test might show some error output similar to the lines below. This is expected and ok.
      -- 2016/11/07 16:48:33 [error] 81932#0: *2 recv() failed (61: Connection refused), context: ngx.timer
      
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
      })
--print("setup fake record, now creating a balancer")
      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 10 }, 
          { name = "getkong.org", port = 123, weight = 10 }, 
        },
        dns = client,
        wheelSize = 20,
        requery = 1,   -- shorten default requery time for the test
      })
--print("balancer created, storing state1 + state2 here")
      local state1 = copyWheel(b)
      local state2 = copyWheel(b)
--print("reconfiguring dns client with bad nameserver...")
      -- reconfigure the dns client to make sure next query fails
      assert(client.init {
        hosts = {}, 
        resolvConf = {
          "nameserver 127.0.0.1:22000" -- make sure dns query fails
        },
      })
      -- refetch the cache, since the 'init' call above caused it to be replaced
      dnscache = client.getcache()
      record.expire = gettime() -1 -- expire current dns cache record
--print("dns client reconfigured, local cache updated (empty now). Running whole wheel...")
      -- run entire wheel to make sure the expired one is requested, so it can fail
      for _ = 1, b.wheelSize do b:getPeer() end
--print("ran down whole wheel. Now updating previous state2 to be the expected one")
      -- all slots are now getkong.org
      updateWheelState(state2, " %- 1%.2%.3%.4 @ 80 %(mashape%.com%)", " - 9.9.9.9 @ 123 (getkong.org)")
      
      assert.same(state2, copyWheel(b))
--print("asserted that the failure updated the wheel correctly by removing 'mashape.com'")
      -- reconfigure the dns client to make sure next query works again
      assert(client.init {
        hosts = {}, 
        resolvConf = {
          "nameserver 8.8.8.8"
        },
      })
      -- refetch the cache, since the 'init' call above caused it to be replaced
      dnscache = client.getcache()  
      dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
--print("client reconfigured, local cache updated, and inserted the fake record again")      
--print("waiting for timer to update the failed record...")
      sleep(b.requeryInterval + 1) --requery timer runs, so should be fixed after this

      -- wheel should be back in original state
--print("now checking the updated results")
      assert.same(state1, copyWheel(b))
    end)
    it("low weight with zero-slots assigned doesn't fail", function()
      -- depending on order of insertion it is either 1 or 0 slots
      -- but it may never error.
      local record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
      })
      check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 99999 }, 
          { name = "getkong.org", port = 123, weight = 1 }, 
        },
        dns = client,
        wheelSize = 100,
      })
      -- Now the order reversed (weights exchanged)
      record = dnsA({ 
        { name = "mashape.com", address = "1.2.3.4" },
      })
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9" },
      })
      check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 1 }, 
          { name = "getkong.org", port = 123, weight = 99999 }, 
        },
        dns = client,
        wheelSize = 100,
      })
    end)
    it("SRV record with 0 weight doesn't fail resolving", function()
      -- depending on order of insertion it is either 1 or 0 slots
      -- but it may never error.
      dnsSRV({
        { name = "gelato.io", target = "1.2.3.6", port = 8001, weight = 0 },
        { name = "gelato.io", target = "1.2.3.6", port = 8002, weight = 0 },
      })
      local b = check_balancer(balancer.new {
        hosts = {
          -- port and weight will be overridden by the above
          { name = "gelato.io", port = 80, weight = 99999 },
        },
        dns = client,
        wheelSize = 100,
      })
      local ip, port, host = b:getPeer()
      assert.equal("1.2.3.6", ip)
      assert(port == 8001 or port == 8002, "port expected 8001 or 8002")
    end)
    it("ttl of 0 inserts only a single unresolved address", function()
      local ttl = 0
      local resolve_count = 0
      local toip_count = 0
      
      -- mock the resolve/toip methods
      local old_resolve = client.resolve
      local old_toip = client.toip
      finally(function() 
          client.resolve = old_resolve 
          client.toip = old_toip
        end)
      client.resolve = function(name, ...)
        if name == "mashape.com" then
          local record = dnsA({ 
            { name = "mashape.com", address = "1.2.3.4", ttl = ttl },
          })
          resolve_count = resolve_count + 1
          return record
        else
          return old_resolve(name, ...)
        end
      end
      client.toip = function(name, ...)
        if name == "mashape.com" then
          toip_count = toip_count + 1
          return "1.2.3.4", ...
        else
          return old_toip(name, ...)
        end
      end

      -- insert 2nd address
      dnsA({ 
        { name = "getkong.org", address = "9.9.9.9", ttl = 60*60 },
      })

      local b = check_balancer(balancer.new { 
        hosts = { 
          { name = "mashape.com", port = 80, weight = 50 }, 
          { name = "getkong.org", port = 123, weight = 50 }, 
        },
        dns = client,
        wheelSize = 100,
        ttl0 = 2,
      })
      -- get current state
      local state = copyWheel(b)
      -- run it down, count the dns queries done
      for _ = 1, b.wheelSize do b:getPeer() end 
      assert.equal(b.wheelSize/2, toip_count)  -- one resolver hit for each slot entry
      assert.equal(1, resolve_count) -- hit once, when adding the host to the balancer
      
      -- wait for expiring the 0-ttl setting
      sleep(b.ttl0Interval + 1)  -- 0 ttl is requeried, to check for changed ttl
      
      ttl = 60 -- set our records ttl to 60 now, so we only get one extra hit now
      toip_count = 0  --reset counters
      resolve_count = 0
      -- run it down, count the dns queries done
      for _ = 1, b.wheelSize do b:getPeer() end 
      assert.equal(0, toip_count)
      assert.equal(1, resolve_count) -- hit once, when updating the 0-ttl entry
      
      -- finally check whether slots didn't move around
      updateWheelState(state, " %- mashape%.com @ ", " - 1.2.3.4 @ ")
      assert.same(state, copyWheel(b))
    end)
    it("recreate Kong issue #2131", function()
      -- erasing does not remove the address from the host
      -- so if the same address is added again, and then deleted again
      -- then upon erasing it will find the previous erased address object,
      -- and upon erasing again a nil-referencing issue then occurs
      local ttl = 1
      local record
      local hostname = "dnstest.mashape.com"

      -- mock the resolve/toip methods
      local old_resolve = client.resolve
      local old_toip = client.toip
      finally(function()
          client.resolve = old_resolve
          client.toip = old_toip
        end)
      client.resolve = function(name, ...)
        if name == hostname then
          record = dnsA({
            { name = hostname, address = "1.2.3.4", ttl = ttl },
          })
          return record
        else
          return old_resolve(name, ...)
        end
      end
      client.toip = function(name, ...)
        if name == hostname then
          return "1.2.3.4", ...
        else
          return old_toip(name, ...)
        end
      end

      -- create a new balancer
      local b = check_balancer(balancer.new {
        hosts = {
          { name = hostname, port = 80, weight = 50 },
        },
        dns = client,
        wheelSize = 10,
        ttl0 = 1,
      })

      sleep(1.1) -- wait for ttl to expire
      -- fetch a peer to reinvoke dns and update balancer, with a ttl=0
      ttl = 0
      b:getPeer()   --> force update internal from A to SRV
      sleep(1.1) -- wait for ttl0, as provided to balancer, to expire
      -- restore ttl to non-0, and fetch a peer to update balancer
      ttl = 1
      b:getPeer()   --> force update internal from SRV to A
      sleep(1.1) -- wait for ttl to expire
      -- fetch a peer to reinvoke dns and update balancer, with a ttl=0
      ttl = 0
      b:getPeer()   --> force update internal from A to SRV
    end)
  end)
end)
