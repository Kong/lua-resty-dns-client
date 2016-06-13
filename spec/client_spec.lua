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

    local answers = client.resolve_type(host, { qtype = typ })
    assert.are.equal(host, answers[1].name)
    assert.are.equal(typ, answers[1].type)
    assert.are.equal(#answers, 1)
  end)

  it("Tests expire and touch times", function()
    client:init()

    local host = "txttest.thijsschreijer.nl"
    local typ = client.TYPE_TXT

    local answers = client.resolve_type(host, { qtype = typ })

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
    local answers2 = client.resolve_type(host, { qtype = typ })
    
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

    local answers = client.resolve_type(host, { qtype = typ })
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
    local answers = client.resolve_type(host, { qtype = typ })

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

    local answers = client.resolve_type(host, {qtype = typ})
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
    answers, err = client.resolve_type("localhost", {qtype = client.TYPE_A})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "127.3.2.1")
    answers, err = client.resolve_type("localhost", {qtype = client.TYPE_AAAA})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "1::2")
    answers, err = client.resolve_type("mashape", {qtype = client.TYPE_A})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "123.123.123.123")
    answers, err = client.resolve_type("kong.for.president", {qtype = client.TYPE_AAAA})
    assert.is.Nil(err)
    assert.are.equal(answers[1].address, "1234::1234")
    
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
  
end)
