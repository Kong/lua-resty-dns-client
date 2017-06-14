local pretty = require("pl.pretty").write
local _

-- empty records and not found errors should be identical, hence we
-- define a constant for that error message
local NOT_FOUND_ERROR = "dns server error: 3 name error"

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
local dump = function(...)
  print(require("pl.pretty").write({...}))
end

describe("DNS client cache", function()

  local client, resolver, query_func
  
  before_each(function()
    _G._TEST = true
    client = require("resty.dns.client")
    resolver = require("resty.dns.resolver")

    -- you can replace this `query_func` upvalue to spy on resolver query calls.
    -- This default will just call the original resolver (hence is transparent)
    query_func = function(self, original_query_func, name, options)
      return original_query_func(self, name, options)
    end
  
    -- patch the resolver lib, such that any new resolver created will query
    -- using the `query_func` upvalue defined above
    local old_new = resolver.new
    resolver.new = function(...)
      local r = old_new(...)
      local original_query_func = r.query
      r.query = function(self, ...)
        if not query_func then
          print(debug.traceback("WARNING: query_func is not set"))
          dump(self, ...)
          return
        end
        return query_func(self, original_query_func, ...)
      end
      return r
    end
  end)
  
  after_each(function()
    package.loaded["resty.dns.client"] = nil
    package.loaded["resty.dns.resolver"] = nil
    client = nil
    resolver = nil
    query_func = nil
    _G._TEST = nil
  end)

  describe("shortnames", function()
    
    local lrucache, mock_records, config
    before_each(function()
      config = {
        nameservers = { "8.8.8.8" },
        ndots = 1,
        search = { "domain.com" },
        hosts = {},
        resolvConf = {},
        order = { "LAST", "SRV", "A", "AAAA", "CNAME" },
        badTtl = 0.5,
        staleTtl = 0.5,
        enable_ipv6 = false,
      }
      assert(client.init(config))
      lrucache = client.getcache()
      
      query_func = function(self, original_query_func, qname, opts)
        return mock_records[qname..":"..opts.qtype] or { errcode = 3, errstr = "name error" }
      end
    end)

    it("are stored in cache without type", function()
      mock_records = {
        ["myhost1.domain.com:"..client.TYPE_A] = {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "myhost1.domain.com",
          ttl = 30, 
        }}
      }

      local result = client.resolve("myhost1")
      assert.equal(result, lrucache:get("none:short:myhost1"))
    end)

    it("are stored in cache with type", function()
      mock_records = {
        ["myhost2.domain.com:"..client.TYPE_A] = {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "myhost2.domain.com",
          ttl = 30, 
        }}
      }

      local result = client.resolve("myhost2", { qtype = client.TYPE_A })
      assert.equal(result, lrucache:get(client.TYPE_A..":short:myhost2"))
    end)

    it("are resolved from cache without type", function()
      mock_records = {}
      lrucache:set("none:short:myhost3", {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "myhost3.domain.com",
          ttl = 30, 
        },
        ttl = 30,
        expire = gettime() + 30,
      }, 30+4)

      local result = client.resolve("myhost3")
      assert.equal(result, lrucache:get("none:short:myhost3"))
    end)

    it("are resolved from cache with type", function()
      mock_records = {}
      lrucache:set(client.TYPE_A..":short:myhost4", {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "myhost4.domain.com",
          ttl = 30, 
        },
        ttl = 30,
        expire = gettime() + 30,
      }, 30+4)

      local result = client.resolve("myhost4", { qtype = client.TYPE_A })
      assert.equal(result, lrucache:get(client.TYPE_A..":short:myhost4"))
    end)

    it("of dereferenced CNAME are stored in cache", function()
      mock_records = {
        ["myhost5.domain.com:"..client.TYPE_CNAME] = {{
          type = client.TYPE_CNAME,
          class = 1,
          name = "myhost5.domain.com",
          cname = "mytarget.domain.com",
          ttl = 30, 
        }},
        ["mytarget.domain.com:"..client.TYPE_A] = {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "mytarget.domain.com",
          ttl = 30, 
        }}
      }
      local result = client.resolve("myhost5")

      assert.same(mock_records["mytarget.domain.com:"..client.TYPE_A], result) -- not the test, intermediate validation

      -- the type un-specificc query was the CNAME, so that should be in the
      -- shorname cache
      assert.same(mock_records["myhost5.domain.com:"..client.TYPE_CNAME],
                  lrucache:get("none:short:myhost5"))
    end)
    
    it("ttl in cache is honored for short name entries", function()
      -- in the short name case the same record is inserted again in the cache
      -- and the lru-ttl has to be calculated, make sure it is correct
      mock_records = {
        ["myhost6.domain.com:"..client.TYPE_A] = {{
          type = client.TYPE_A,
          address = "1.2.3.4",
          class = 1,
          name = "myhost6.domain.com",
          ttl = 0.1, 
        }}
      }
      local mock_copy = require("pl.tablex").deepcopy(mock_records)
      
      -- resolve and check whether we got the mocked record
      local result = client.resolve("myhost6")
      assert.equal(result, mock_records["myhost6.domain.com:"..client.TYPE_A])
      
      -- replace our mocked list with the copy made (new table, so no equality)
      mock_records = mock_copy
      
      -- wait for expiring
      sleep(0.1 + config.staleTtl / 2)
      
      -- resolve again, now getting same record, but stale, this will trigger
      -- background refresh query
      local result2 = client.resolve("myhost6")
      assert.equal(result2, result)
      assert.is_true(result2.expired)  -- stale; marked as expired
      
      -- wait for refresh to complete
      sleep(0.1)
      
      -- resolve and check whether we got the new record from the mock copy
      local result3 = client.resolve("myhost6")
      assert.not_equal(result, result3)  -- must be a different record now
      assert.equal(result3, mock_records["myhost6.domain.com:"..client.TYPE_A])
      
      -- the 'result3' resolve call above will also trigger a new background query
      -- (because the sleep of 0.1 equals the records ttl of 0.1)
      -- so let's yield to activate that background thread now. If not done so,
      -- the `after_each` will clear `query_func` and an error will appear on the 
      -- next test after this one that will yield.
      sleep(0.1)
    end)

  end)

end)
