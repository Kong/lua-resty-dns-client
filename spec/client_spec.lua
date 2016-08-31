local modname = "dns.client"
local writefile = require("pl.utils").writefile
local tempfilename = require("pl.path").tmpname

local gettime, sleep
if ngx then
  gettime = ngx.now
  sleep = ngx.sleep
else
  local socket = require("socket")
  gettime = socket.gettime
  sleep = socket.sleep
end


describe("Testing the DNS client", function()

  local client
  
  before_each(function()
      _G._TEST = true
    client = require(modname)
  end)
  
  after_each(function()
    package.loaded[modname] = nil
    client = nil
    _G._TEST = nil
  end)

  it("Tests fetching a TXT record", function()
    client:init()

    local host = "txttest.thijsschreijer.nl"
    local typ = client.TYPE_TXT

    local answers = client.resolve(host, { qtype = typ })
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
  end)

  it("Tests fetching a CNAME record", function()
    client:init()

    local host = "smtp.thijsschreijer.nl"
    local typ = client.TYPE_CNAME

    local answers = client.resolve(host, { qtype = typ })
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
  end)
  
  it("Tests expire and touch times", function()
    client:init()

    local host = "txttest.thijsschreijer.nl"
    local typ = client.TYPE_TXT

    local answers = client.resolve(host, { qtype = typ })

    local now = gettime()
    local touch_diff = math.abs(now - answers.touch)
    local ttl_diff = math.abs((now + answers[1].ttl) - answers.expire)
    assert(touch_diff < 0.01, "Expected difference to be near 0; "..
                                tostring(touch_diff))
    assert(ttl_diff < 0.01, "Expected difference to be near 0; "..
                                tostring(ttl_diff))
    
    sleep(1)

    -- fetch again, now from cache
    local oldtouch = answers.touch
    local answers2 = client.resolve(host, { qtype = typ })
    
    assert.are.equal(answers, answers2) -- cached table, so must be same
    assert.are.not_equal(oldtouch, answers.touch)    
    
    now = gettime()
    touch_diff = math.abs(now - answers.touch)
    ttl_diff = math.abs((now + answers[1].ttl) - answers.expire)
    assert(touch_diff < 0.01, "Expected difference to be near 0; "..
                                tostring(touch_diff))
    assert((0.990 < ttl_diff) and (ttl_diff < 1.01), 
              "Expected difference to be near 1; "..tostring(ttl_diff))

  end)

  it("Tests fetching multiple A records", function()
    client:init()

    local host = "atest.thijsschreijer.nl"
    local typ = client.TYPE_A

    local answers = client.resolve(host, { qtype = typ })
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(host, answers[2].name)
    assert.are.equal(typ, answers[2].type)
    assert.are.equal(#answers, 2)
  end)

  it("Tests fetching A record redirected through 2 CNAME records", function()
    client:init()

    local host = "smtp.thijsschreijer.nl"
    local typ = client.TYPE_A
    local answers = client.resolve(host, { qtype = typ })

    assert.are.not_equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
    
    -- check first CNAME
    local key1 = client.TYPE_CNAME..":"..host
    local entry1 = client.__cache[key1]
    assert.are.equal(host, entry1[1].name)
    assert.are.equal(client.TYPE_CNAME, entry1[1].type)
  
    -- check second CNAME
    local key2 = client.TYPE_CNAME..":"..entry1[1].cname
    local entry2 = client.__cache[key2]
    assert.are.equal(entry1[1].cname, entry2[1].name)
    assert.are.equal(client.TYPE_CNAME, entry2[1].type)
    
    -- check second target to match final record
    assert.are.equal(entry2[1].cname, answers[1].name)
  end)

  it("Tests fetching multiple SRV records (un-typed)", function()
    client:init()

    local host = "srvtest.thijsschreijer.nl"
    local typ = client.TYPE_SRV

    -- un-typed; so fetch using `resolve` method instead of `resolve_type`.
    local answers = client.resolve(host)
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(host, answers[2].name)
    assert.are.equal(typ, answers[2].type)
    assert.are.equal(host, answers[3].name)
    assert.are.equal(typ, answers[3].type)
    assert.are.equal(#answers, 3)
  end)

  it("Tests fetching multiple SRV records through CNAME (un-typed)", function()
    client:init()

    local host = "cname2srv.thijsschreijer.nl"
    local typ = client.TYPE_SRV

    -- un-typed; so fetch using `resolve` method instead of `resolve_type`.
    local answers = client.resolve(host)
    
    -- first check CNAME
    local key = client.TYPE_CNAME..":"..host
    local entry = client.__cache[key]
    assert.are.equal(host, entry[1].name)
    assert.are.equal(client.TYPE_CNAME, entry[1].type)
    
    -- check final target
    assert.are.equal(entry[1].cname, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(entry[1].cname, answers[2].name)
    assert.are.equal(typ, answers[2].type)
    assert.are.equal(entry[1].cname, answers[3].name)
    assert.are.equal(typ, answers[3].type)
    assert.are.equal(#answers, 3)
  end)

  it("Tests fetching non-type-matching records", function()
    client:init()

    local host = "srvtest.thijsschreijer.nl"
    local typ = client.TYPE_A   --> the entry is SRV not A

    local answers = client.resolve(host, {qtype = typ})
    assert.are.equal(#answers, 0)  -- returns empty table
  end)

  it("Tests fetching non-existing records", function()
    client:init()

    local host = "IsNotHere.thijsschreijer.nl"

    local answers = client.resolve(host)
    assert.are.equal(#answers, 0)  -- returns server error table
    assert.is.not_nil(answers.errcode)
    assert.is.not_nil(answers.errstr)    
  end)

  it("Tests fetching IPv4 address", function()
    client:init()

    local host = "1.2.3.4"

    local answers = client.resolve(host)
    assert.are.equal(#answers, 1)
    assert.are.equal(client.TYPE_A, answers[1].type)
    assert.are.equal(10*365*24*60*60, answers[1].ttl)  -- 10 year ttl
  end)

  it("Tests fetching IPv6 address", function()
    client:init()

    local host = "1:2::3:4"

    local answers = client.resolve(host)
    assert.are.equal(#answers, 1)
    assert.are.equal(client.TYPE_AAAA, answers[1].type)
    assert.are.equal(10*365*24*60*60, answers[1].ttl)  -- 10 year ttl
  end)

  it("Tests fetching invalid IPv6 address", function()
    client:init()

    local host = "1::2:3::4"  -- 2x double colons

    local answers = client.resolve(host)
    assert.are.equal(#answers, 0)  -- returns server error table
    assert.are.equal(3, answers.errcode)
    assert.are.equal("name error", answers.errstr)    
  end)
  
  pending("Tests fetching records from cache only",function()
  end)

  pending("Tests fetching records with ttl=0 from cache only",function()
  end)

  pending("Tests fetching expired records from cache only",function()
  end)

  it("Tests resolving from the /etc/hosts file", function()
    local f = tempfilename()
    writefile(f, [[

127.3.2.1 localhost
1::2 localhost

123.123.123.123 mashape
1234::1234 kong.for.president
      
]])
    client:init({hosts = f})
    os.remove(f)
    
    local answers, err
    answers, err = client.resolve("localhost", {qtype = client.TYPE_A})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "127.3.2.1")
    answers, err = client.resolve("localhost", {qtype = client.TYPE_AAAA})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "1::2")
    answers, err = client.resolve("mashape", {qtype = client.TYPE_A})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "123.123.123.123")
    answers, err = client.resolve("kong.for.president", {qtype = client.TYPE_AAAA})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "1234::1234")
    
  end)

  describe("Testing the toip() function", function()
    it("A/AAAA-record, round-robin",function()
      client:init()
      local host = "atest.thijsschreijer.nl"
      local answers = client.resolve(host)
      answers.last_index = nil -- make sure to clean
      local ips = {}
      for _,rec in ipairs(answers) do ips[rec.address] = true end
      local order = {}
      for n = 1, #answers do
        local ip = client.toip(host)
        ips[ip] = nil
        order[n] = ip
      end
      -- this table should be empty again
      assert.is_nil(next(ips))
      -- do again, and check same order
      for n = 1, #order do
        local ip = client.toip(host)
        assert.same(order[n], ip)
      end
    end)
    it("SRV-record, round-robin on lowest prio",function()
      client:init()
      local host = "srvtest.thijsschreijer.nl"
      local answers = client.resolve(host)
      local answers2
      local low_prio = 1
      local prio2
      -- there is one non-ip entry, forwarding to www.thijsschreijer.nl, go find it
      for i, rec in ipairs(answers) do
        if rec.target:find("thijsschreijer") then
          answers2 = client.resolve(rec.target)
          prio2 = i
        else
          -- record index of the ip address with the lowest priority
          if rec.priority <= answers[low_prio].priority then
            low_prio = i
          end
        end
      end
      assert(answers[prio2].priority == answers[low_prio].priority)
      local results = {}
      for _ = 1,20 do 
        local ip, port = client.toip(host)
        results[ip.."+"..port] = (results[ip.."+"..port] or 0) + 1
      end
      -- 20 passes, each should get 10
      assert(results[answers[low_prio].target.."+"..answers[low_prio].port] == 10)
      assert(results[answers2[1].address.."+"..answers[prio2].port] == 10)
      
      -- remove them, and check the results to be empty, as the higher priority field one should not have gotten any calls
      results[answers[low_prio].target.."+"..answers[low_prio].port] = nil
      results[answers2[1].address.."+"..answers[prio2].port] = nil
      assert.is_nil(next(results))
    end)
    it("port passing",function()
      client:init()
      local ip, port, host 
      host = "atest.thijsschreijer.nl"
      ip,port = client.toip(host)
      assert.is_string(ip)
      assert.is_nil(port)
      
      ip, port = client.toip(host, 1234)
      assert.is_string(ip)
      assert.equal(1234, port)
      
      host = "srvtest.thijsschreijer.nl"
      ip, port = client.toip(host)
      assert.is_string(ip)
      assert.is_number(port)
      
      ip, port = client.toip(host, 0)
      assert.is_string(ip)
      assert.is_number(port)
      assert.is_not.equal(0, port)
    end)
    pending("Tests resolving from cache only",function()
    end)

    pending("Tests resolving with ttl=0 from cache only",function()
    end)

    pending("Tests resolving expired records from cache only",function()
    end)

  end)

  it("Tests initialization without i/o access", function()
    local result, err = client:init({
        hosts = {},  -- empty tables to parse to prevent defaulting to /etc/hosts
        resolv_conf = {},   -- and resolv.conf files
      })
    assert.is.True(result)
    assert.is.Nil(err)
    assert.are.equal(#client.__cache, 0) -- no hosts file record should have been imported
  end)
  
  describe("the stdError function", function()
    it("Tests a valid record passed through", function()
      local rec = { { address = "1.2.3.4" } }
      local res, err = client.stdError(rec, nil)
      assert.are.equal(rec, res)
      assert.is_nil(err)
    end)
    it("Tests a server error returned as Lua error", function()
      local rec = {
        errcode = 3,
        errstr = "name error",
      }
      local res, err = client.stdError(rec, nil)
      assert.are.equal(err, "dns server error; 3 name error")
      assert.is_nil(res)
    end)
    it("Tests a Lua error passed through", function()
      local rec = "this is an error"
      local res, err = client.stdError(nil, rec)
      assert.are.equal(rec, err)
      assert.is_nil(res)
    end)
    it("Tests an empty response returned with message", function()
      local rec = {}
      local res, err = client.stdError(rec, nil)
      assert.are.equal(rec, res)
      assert.are.equal(err, "dns query returned no results")
    end)
  end)
  
  pending("verifies ttl and caching of errors and empty responses", function()
    --empty responses should be cached for a configurable time
    --error responses should be cached for a configurable time
  end)  
end)
