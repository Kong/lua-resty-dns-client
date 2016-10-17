local writefile = require("pl.utils").writefile
local tempfilename = require("pl.path").tmpname
local pretty = require("pl.pretty").write

local gettime, sleep
if ngx then
  gettime = ngx.now
  sleep = ngx.sleep
else
  local socket = require("socket")
  gettime = socket.gettime
  sleep = socket.sleep
end

-- simple debug function
local debug = function(...)
  print(pretty({...}))
end

describe("DNS client", function()

  local client
  
  before_each(function()
      _G._TEST = true
    client = require("dns.client")
  end)
  
  after_each(function()
    package.loaded["dns.client"] = nil
    package.loaded["resty.dns.resolver"] = nil
    client = nil
    _G._TEST = nil
  end)

  describe("initialization", function()

    it("fails with no nameservers", function()
      -- empty list fallsback on resolv.conf
      assert.has.no.error(function() client:init( {nameservers = {} } ) end)

      assert.has.error(function() client:init( {nameservers = {}, resolv_conf = {} } ) end)    
    end)

    it("fails with order being empty", function()
      -- fails with an empty one
      assert.has.error(
        function() client:init({order = {}}) end,
        "Invalid order list; cannot be empty"
      )
    end)
    
    it("fails with order containing an unknown type", function()
      -- fails with an unknown one
      assert.has.error(
        function() client:init({order = {"LAST", "a", "aa"}}) end,
        "Invalid dns record type in order array; aa"
      )
    end)
  
    it("succeeds with order unset", function()
      assert.is.True(client:init({order = nil}))
    end)
  
    it("succeeds without i/o access", function()
      local result, err = assert(client:init({
          nameservers = { "8.8.8.8:53" },
          hosts = {},  -- empty tables to parse to prevent defaulting to /etc/hosts
          resolv_conf = {},   -- and resolv.conf files
        }))
      assert.is.True(result)
      assert.is.Nil(err)
      assert.are.equal(#client.getcache(), 0) -- no hosts file record should have been imported
    end)

  end)

  it("fetching a TXT record", function()
    assert(client:init())

    local host = "txttest.thijsschreijer.nl"
    local typ = client.TYPE_TXT

    local answers = assert(client.resolve(host, { qtype = typ }))
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
  end)

  it("fetching a CNAME record", function()
    assert(client:init())

    local host = "smtp.thijsschreijer.nl"
    local typ = client.TYPE_CNAME

    local answers = assert(client.resolve(host, { qtype = typ }))
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
  end)
  
  it("expire and touch times", function()
    assert(client:init())

    local host = "txttest.thijsschreijer.nl"
    local typ = client.TYPE_TXT

    local answers = assert(client.resolve(host, { qtype = typ }))

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
    local answers2 = assert(client.resolve(host, { qtype = typ }))

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

  it("fetching multiple A records", function()
    assert(client:init())

    local host = "atest.thijsschreijer.nl"
    local typ = client.TYPE_A

    local answers = assert(client.resolve(host, { qtype = typ }))
    assert.are.equal(#answers, 2)
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(host, answers[2].name)
    assert.are.equal(typ, answers[2].type)
  end)

  it("fetching A record redirected through 2 CNAME records (un-typed)", function()
    assert(client:init())

    --[[
    This test might fail. Recurse flag is on by default. This means that the first return
    includes the cname records, but the second one (within the ttl) will only include the
    A-record.
    Note that this is not up to the client code, but it's done out of our control by the 
    dns server.
    If we turn on the 'no_recurse = true' option, then the dns server might refuse the request
    (error nr 5).
    So effectively the first time the test runs, it's ok. Immediately running it again will
    make it fail. Wait for the ttl to expire, then it will work again.

    This does not affect client side code, as the result is always the final A record.
    --]]

    local host = "smtp.thijsschreijer.nl"
    local typ = client.TYPE_A
    local answers = assert(client.resolve(host))

    assert.are.not_equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
    
    -- check first CNAME
    local key1 = client.TYPE_CNAME..":"..host
    local entry1 = client.getcache()[key1]
    assert.are.equal(host, entry1[1].name)
    assert.are.equal(client.TYPE_CNAME, entry1[1].type)
  
    -- check second CNAME
    local key2 = client.TYPE_CNAME..":"..entry1[1].cname
    local entry2 = client.getcache()[key2]
    assert.are.equal(entry1[1].cname, entry2[1].name)
    assert.are.equal(client.TYPE_CNAME, entry2[1].type)
    
    -- check second target to match final record
    assert.are.equal(entry2[1].cname, answers[1].name)
  end)

  it("fetching multiple SRV records (un-typed)", function()
    assert(client:init())

    local host = "srvtest.thijsschreijer.nl"
    local typ = client.TYPE_SRV

    -- un-typed; so fetch using `resolve` method instead of `resolve_type`.
    local answers = assert(client.resolve(host))
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(host, answers[2].name)
    assert.are.equal(typ, answers[2].type)
    assert.are.equal(host, answers[3].name)
    assert.are.equal(typ, answers[3].type)
    assert.are.equal(#answers, 3)
  end)

  it("fetching multiple SRV records through CNAME (un-typed)", function()
    assert(client:init())

    local host = "cname2srv.thijsschreijer.nl"
    local typ = client.TYPE_SRV

    -- un-typed; so fetch using `resolve` method instead of `resolve_type`.
    local answers = assert(client.resolve(host))

    -- first check CNAME
    local key = client.TYPE_CNAME..":"..host
    local entry = client.getcache()[key]
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

  it("fetching non-type-matching records", function()
    assert(client:init())

    local host = "srvtest.thijsschreijer.nl"
    local typ = client.TYPE_A   --> the entry is SRV not A

    local answers = assert(client.resolve(host, {qtype = typ}))
    assert.are.equal(#answers, 0)  -- returns empty table
  end)

  it("fetching non-existing records", function()
    assert(client:init())

    local host = "IsNotHere.thijsschreijer.nl"

    local answers = assert(client.resolve(host))
    assert.are.equal(#answers, 0)  -- returns server error table
    assert.is.not_nil(answers.errcode)
    assert.is.not_nil(answers.errstr)    
  end)

  it("fetching IPv4 address", function()
    assert(client:init())

    local host = "1.2.3.4"

    local answers = assert(client.resolve(host))
    assert.are.equal(#answers, 1)
    assert.are.equal(client.TYPE_A, answers[1].type)
    assert.are.equal(10*365*24*60*60, answers[1].ttl)  -- 10 year ttl
  end)

  it("fetching IPv6 address", function()
    assert(client:init())

    local host = "1:2::3:4"

    local answers = assert(client.resolve(host))
    assert.are.equal(#answers, 1)
    assert.are.equal(client.TYPE_AAAA, answers[1].type)
    assert.are.equal(10*365*24*60*60, answers[1].ttl)  -- 10 year ttl
  end)

  it("fetching invalid IPv6 address", function()
    assert(client:init())

    local host = "1::2:3::4"  -- 2x double colons

    local answers = assert(client.resolve(host))
    assert.are.equal(#answers, 0)  -- returns server error table
    assert.are.equal(3, answers.errcode)
    assert.are.equal("name error", answers.errstr)    
  end)
  
  it("fetching records from cache only, expired and ttl = 0",function()
    local expired_entry = {
      {
        type = client.TYPE_A,
        address = "1.2.3.4",
        class = 1,
        name = "1.2.3.4",
        ttl = 0, 
      },
      touch = 0,
      expire = 0,  -- definitely expired
    }
    -- insert in the cache
    client.getcache()[expired_entry[1].type..":"..expired_entry[1].name] = expired_entry
    local cache_count = #client.getcache()

    -- resolve this, cache only
    local result = client.resolve("1.2.3.4", {qtype = expired_entry[1].type}, true)
    
    assert.are.equal(expired_entry, result)
    assert.are.equal(cache_count, #client.getcache())  -- should not be deleted
    assert.are.equal(expired_entry, client.getcache()[expired_entry[1].type..":"..expired_entry[1].name])
  end)

  it("recursive lookups failure", function()
    assert(client:init())
    local entry1 = {
      {
        type = client.TYPE_CNAME,
        cname = "bye.bye.world",
        class = 1,
        name = "hello.world",
        ttl = 0, 
      },
      touch = 0,
      expire = 0,
    }
    local entry2 = {
      {
        type = client.TYPE_CNAME,
        cname = "hello.world",
        class = 1,
        name = "bye.bye.world",
        ttl = 0, 
      },
      touch = 0,
      expire = 0,
    }
    -- insert in the cache
    client.getcache()[entry1[1].type..":"..entry1[1].name] = entry1
    client.getcache()[entry2[1].type..":"..entry2[1].name] = entry2

    -- Note: the bad case would be that the below lookup would hang due to round-robin on an empty table
    local result, err = client.resolve("hello.world", nil, true)
    assert.is_nil(result)
    assert.are.equal("maximum dns recursion level reached", err)
  end)

  it("resolving from the /etc/hosts file", function()
    local f = tempfilename()
    writefile(f, [[

127.3.2.1 localhost
1::2 localhost

123.123.123.123 mashape
1234::1234 kong.for.president
      
]])
    assert(client:init({hosts = f}))
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

  describe("toip() function", function()
    it("A/AAAA-record, round-robin",function()
      assert(client:init())
      local host = "atest.thijsschreijer.nl"
      local answers = assert(client.resolve(host))
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
      assert(client:init())
      local host = "srvtest.thijsschreijer.nl"
      local answers = assert(client.resolve(host))
      local answers2
      local low_prio = 1
      local prio2
      -- there is one non-ip entry, forwarding to www.thijsschreijer.nl, go find it
      for i, rec in ipairs(answers) do
        if rec.target:find("thijsschreijer") then
          answers2 = assert(client.resolve(rec.target))
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
      assert(client:init())
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
    it("resolving in correct record-type order",function()
      local function config()
        -- function to insert 2 records in the cache
        local A_entry = {
          {
            type = client.TYPE_A,
            address = "5.6.7.8",
            class = 1,
            name = "hello.world",
            ttl = 10, 
          },
          touch = 0,
          expire = gettime()+10,  -- active
        }
        local AAAA_entry = {
          {
            type = client.TYPE_AAAA,
            address = "::1",
            class = 1,
            name = "hello.world",
            ttl = 10, 
          },
          touch = 0,
          expire = gettime()+10,  -- active
        }
        -- insert in the cache
        local cache = client.getcache()
        cache[A_entry[1].type..":"..A_entry[1].name] = A_entry
        cache[AAAA_entry[1].type..":"..AAAA_entry[1].name] = AAAA_entry
      end
      assert(client:init({order = {"AAAA", "A"}}))
      config()
      local ip = client.toip("hello.world")
      assert.equals(ip, "::1")
      assert(client:init({order = {"A", "AAAA"}}))
      config()
      ip = client.toip("hello.world")
      assert.equals(ip, "5.6.7.8")
    end)
    it("resolving from cache only, expired and ttl = 0",function()
      assert(client:init())
      local expired_entry = {
        {
          type = client.TYPE_A,
          address = "5.6.7.8",
          class = 1,
          name = "hello.world",
          ttl = 0, 
        },
        touch = 0,
        expire = 0,  -- definitely expired
      }
      -- insert in the cache
      client.getcache()[expired_entry[1].type..":"..expired_entry[1].name] = expired_entry
      local cache_count = #client.getcache()

      -- resolve this, cache only
      local result, port = assert(client.toip("hello.world", 9876, true))

      assert.are.equal(expired_entry[1].address, result)
      assert.are.equal(9876, port)
      assert.are.equal(cache_count, #client.getcache())  -- should not be deleted
      assert.are.equal(expired_entry, client.getcache()[expired_entry[1].type..":"..expired_entry[1].name])
    end)
    it("handling of empty responses", function()
      assert(client:init())
      local empty_entry = {
        touch = 0,
        expire = 0,
      }
      -- insert in the cache
      client.getcache()[client.TYPE_A..":".."hello.world"] = empty_entry

      -- Note: the bad case would be that the below lookup would hang due to round-robin on an empty table
      local ip, port = client.toip("hello.world", 123, true)
      assert.is_nil(ip)
      assert.is.string(port)  -- error message
    end)
    it("recursive lookups failure", function()
      assert(client:init())
      local entry1 = {
        {
          type = client.TYPE_CNAME,
          cname = "bye.bye.world",
          class = 1,
          name = "hello.world",
          ttl = 0, 
        },
        touch = 0,
        expire = 0,
      }
      local entry2 = {
        {
          type = client.TYPE_CNAME,
          cname = "hello.world",
          class = 1,
          name = "bye.bye.world",
          ttl = 0, 
        },
        touch = 0,
        expire = 0,
      }
      -- insert in the cache
      client.getcache()[entry1[1].type..":"..entry1[1].name] = entry1
      client.getcache()[entry2[1].type..":"..entry2[1].name] = entry2

      -- Note: the bad case would be that the below lookup would hang due to round-robin on an empty table
      local ip, port = client.toip("hello.world", 123, true)
      assert.is_nil(ip)
      assert.are.equal("maximum dns recursion level reached", port)
    end)
    it("passing through the resolver-object", function()
      assert(client:init())

      local _, err1, r1 = client.toip("google.com", 123, false)
      local q2, err2, r2 = client.toip("google.nl",  123, false, r1)
      assert.are.equal(123, err1)
      assert.are.equal(123, err2)
      assert.is.table(r1)
      assert.is.table(r2)
      assert.equal(r1, r2)
      
      -- when fetching from cache
      q2, err2, r2 = client.toip("google.nl",  123, false, r1)
      assert.equal(r1, r2)
      
      -- when its an IPv4 address
      q2, err2, r2 = client.toip("1.2.3.4",  123, false, r1)
      assert.equal(r1, r2)
      
      -- when its an IPv6 address
      q2, err2, r2 = client.toip("::1",  123, false, r1)
      assert.equal(r1, r2)
      
      -- when its a bad IPv6 address (ipv6 == more than 1 colon)
      q2, err2, r2 = client.toip("::1gdhgasga",  123, false, r1)
      assert.equal(r1, r2)
    end)
  end)

  describe("matrix;", function()
    local ip = "1.4.2.3"
    local name = "thijsschreijer.nl"
    local prep = function(ttl, expired)
      assert(client:init())
      if expired then
        expired = (-ttl - 2) -- expired by 2 seconds
      else
        expired = 0
      end
      local entry = {
        {
          type = client.TYPE_A,
          address = ip,
          class = 1,
          name = name,
          ttl = ttl, 
        },
        touch = 0,
        expire = gettime() + ttl + expired, 
      }
      -- insert in the cache
      client.getcache()[entry[1].type..":"..entry[1].name] = entry
      return entry
    end
    
    it("ttl=0, expired=true,  cache_only=true", function()
      -- returns the expired record, because cache_only is set
      local ttl, expired, cache_only = 0, true, true
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.are.equal(entry, record)
    end)
    it("ttl=0, expired=true,  cache_only=false", function()
      -- returns a new record, because it is expired and not cache_only
      local ttl, expired, cache_only = 0, true, false
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.is.table(record)
      assert.is.table(record[1])
      assert.are.equal(name, record[1].name)
      assert.are_not.equal(ip, record[1].address)
      assert.are.Not.equal(entry, record)
    end)
    it("ttl=0, expired=false, cache_only=true", function()
      -- returns the expired record, because cache_only is set
      local ttl, expired, cache_only = 0, false, true
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.are.equal(entry, record)
    end)
    it("ttl=0, expired=false, cache_only=false", function()
      -- returns a new record, because it has ttl=0, and not cache_only
      local ttl, expired, cache_only = 0, false, false
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.is.table(record)
      assert.is.table(record[1])
      assert.are.equal(name, record[1].name)
      assert.are_not.equal(ip, record[1].address)
      assert.are.Not.equal(entry, record)
    end)
    it("ttl>0, expired=true,  cache_only=true", function()
      -- returns the expired record, because it is cache_only
      local ttl, expired, cache_only = 10, true, true
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.are.equal(entry, record)
    end)
    it("ttl>0, expired=true,  cache_only=false", function()
      -- returns a new record, because it is expired, but not cache_only
      local ttl, expired, cache_only = 10, true, false
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.is.table(record)
      assert.is.table(record[1])
      assert.are.equal(name, record[1].name)
      assert.are_not.equal(ip, record[1].address)
      assert.are.Not.equal(entry, record)
    end)
    it("ttl>0, expired=false, cache_only=true", function()
      -- returns the active/valid record
      local ttl, expired, cache_only = 10, false, true
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.are.equal(entry, record)
    end)
    it("ttl>0, expired=false, cache_only=false", function()
      -- returns the active/valid record
      local ttl, expired, cache_only = 10, false, false
      
      local entry = prep(ttl, expired)
      local record = assert(client.resolve(name, nil, cache_only))
      assert.are.equal(entry, record)
    end)
  end)

  describe("stdError() function", function()
    it("Tests a valid record passed through", function()
      local rec = { { address = "1.2.3.4" } }
      local res, err = client.stdError(rec, nil)
      assert.are.equal(rec, res)
      assert.is_nil(err)
    end)
    it("server error returned as Lua error", function()
      local rec = {
        errcode = 3,
        errstr = "name error",
      }
      local res, err = client.stdError(rec, nil)
      assert.are.equal(err, "dns server error; 3 name error")
      assert.is_nil(res)
    end)
    it("Lua error passed through", function()
      local rec = "this is an error"
      local res, err = client.stdError(nil, rec)
      assert.are.equal(rec, err)
      assert.is_nil(res)
    end)
    it("empty response returned with message", function()
      local rec = {}
      local res, err = client.stdError(rec, nil)
      assert.are.equal(rec, res)
      assert.are.equal(err, "dns query returned no results")
    end)
  end)
  
  it("verifies ttl and caching of errors and empty responses", function()
    --empty/error responses should be cached for a configurable time
    local bad_ttl = 0.1
    assert(client:init({bad_ttl = bad_ttl}))

    -- do a query so we get a resolver object to spy on
    local _, _, r = client.toip("google.com", 123, false)
    spy.on(r, "query")
    
    local res1, res2, err1, err2
    res1, err1, r = client.resolve(
      "really.reall.really.does.not.exist.mashape.com", 
      { qtype = client.TYPE_A }, 
      false, r)
    assert.spy(r.query).was.called(1)
    assert.equal(3, res1.errcode)
    
    res2, err2, r = client.resolve(
      "really.reall.really.does.not.exist.mashape.com", 
      { qtype = client.TYPE_A }, 
      false, r)
    assert.are.equal(res1, res2)
    assert.spy(r.query).was.called(1)
    
    -- wait for expiry of ttl and retry
    sleep(bad_ttl+0.1)
    res2, err2, r = client.resolve(
      "really.reall.really.does.not.exist.mashape.com", 
      { qtype = client.TYPE_A }, 
      false, r)
    assert.are.Not.equal(res1, res2)
    assert.spy(r.query).was.called(2) 
  
  end)

  describe("verifies the polling of dns queries, retries, and wait times", function()
    
    it("simultaneous lookups are synchronized to 1 lookup", function()
      assert(client:init())
      local coros = {}
      local results = {}
      
      -- we're going to schedule a whole bunch of queries, all of this
      -- function, which does the same lookup and stores the result
      local x = function()
        -- the function is ran when started. So we must immediately yield
        -- so the scheduler loop can first schedule them all before actually
        -- starting resolving
        coroutine.yield(coroutine.running())
        local result = client.resolve("thijsschreijer.nl")
        table.insert(results, result)
      end
      
      -- schedule a bunch of the same lookups
      for _ = 1, 10 do
        local co = ngx.thread.spawn(x)
        table.insert(coros, co)
      end
      
      -- all scheduled and waiting to start due to the yielding done.
      -- now start them all
      for i = 1, #coros do
        ngx.thread.wait(coros[i]) -- this wait will resume the scheduled ones
      end
      
      -- now count the unique responses we got
      local counters = {}
      for _, r in ipairs(results) do
        r = tostring(r)
        counters[r] = (counters[r] or 0) + 1
      end
      local count = 0
      for _ in pairs(counters) do count = count + 1 end
      
      -- we should have a single result table, as all threads are supposed to
      -- return the exact same table.
      assert.equal(1,count)
    end)
  
    it("simultaneous lookups with ttl=0 are not synchronized to 1 lookup", function()
      assert(client:init())
      
      -- insert a ttl=0 record, so the resolver expects 0 and does not
      -- synchronize the lookups
      local ip = "1.4.2.3"
      local name = "thijsschreijer.nl"
      local entry = {
        {
          type = client.TYPE_A,
          address = ip,
          class = 1,
          name = name,
          ttl = 0,
        },
        touch = 0,
        expire = gettime() - 1, 
      }
      -- insert in the cache
      client.getcache()[entry[1].type..":"..entry[1].name] = entry
      
      local coros = {}
      local results = {}
      
      -- we're going to schedule a whole bunch of queries, all of this
      -- function, which does the same lookup and stores the result
      local x = function()
        -- the function is ran when started. So we must immediately yield
        -- so the scheduler loop can first schedule them all before actually
        -- starting resolving
        coroutine.yield(coroutine.running())
        local result = client.resolve("thijsschreijer.nl", {qtype = client.TYPE_A})
        table.insert(results, result)
      end
      
      -- schedule a bunch of the same lookups
      for _ = 1, 10 do
        local co = ngx.thread.spawn(x)
        table.insert(coros, co)
      end
      
      -- all scheduled and waiting to start due to the yielding done.
      -- now start them all
      for i = 1, #coros do
        ngx.thread.wait(coros[i]) -- this wait will resume the scheduled ones
      end
      
      -- now count the unique responses we got
      local counters = {}
      for _, r in ipairs(results) do
        r = tostring(r)
        counters[r] = (counters[r] or 0) + 1
      end
      local count = 0
      for _ in pairs(counters) do count = count + 1 end
      
      -- we should have a 10 individual result tables, as all threads are 
      -- supposed to do their own lookup.
      assert.equal(10,count)
    end)

    it("timeout while waiting", function()
      -- basically the local function _synchronized_query
      assert(client:init({timeout = 2000, retrans = 1, }))
      
      -- insert a stub thats waits and returns a fixed record
      local name = "thijsschreijer.nl"
      local resty = require("resty.dns.resolver")
      resty.new = function(...)
        return {
          query = function()
            local ip = "1.4.2.3"
            local entry = {
              {
                type = client.TYPE_A,
                address = ip,
                class = 1,
                name = name,
                ttl = 10,
              },
              touch = 0,
              expire = gettime() + 10, 
            }
            sleep(2) -- wait before we return the results
            return entry
          end
        }
      end
      
      
      local coros = {}
      local results = {}
      
      -- we're going to schedule a whole bunch of queries, all of this
      -- function, which does the same lookup and stores the result
      local x = function()
        -- the function is ran when started. So we must immediately yield
        -- so the scheduler loop can first schedule them all before actually
        -- starting resolving
        coroutine.yield(coroutine.running())
        local result, err = client.resolve("thijsschreijer.nl", {qtype = client.TYPE_A})
        table.insert(results, result or err)
      end
      
      -- schedule a bunch of the same lookups
      for _ = 1, 10 do
        local co = ngx.thread.spawn(x)
        table.insert(coros, co)
      end
      
      -- all scheduled and waiting to start due to the yielding done.
      -- now start them all
      for i = 1, #coros do
        ngx.thread.wait(coros[i]) -- this wait will resume the scheduled ones
      end
      
      -- the result should be 3 entries
      -- 1: a table  (first attempt)
      -- 2: a second table (the 1 retry, as hardcoded in `pool_max_retry` variable)
      -- 3-10: error message (returned by thread 3 to 10)
      assert.is.table(results[1])
      assert.is.table(results[1][1])
      assert.is.equal(results[1][1].name, name)
      results[1].touch = nil
      results[2].touch = nil
      results[1].expire = nil
      results[2].expire = nil
      assert.Not.equal(results[1], results[2])
      assert.same(results[1], results[2])
      for i = 3, 10 do
        assert.equal("dns lookup pool exceeded retries (1): timeout", results[i])
      end
    end)
  end)
end)
