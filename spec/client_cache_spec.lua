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


-- ==============================================
--    Short-names caching
-- ==============================================


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

    it("errors are not stored", function()
      local rec = {
        errcode = 4,
        errstr = "server failure",
      }
      mock_records = {
        ["myhost7.domain.com:"..client.TYPE_A] = rec,
        ["myhost7:"..client.TYPE_A] = rec,
      }

      local result, err = client.resolve("myhost7", { qtype = client.TYPE_A })
      assert.is_nil(result)
      assert.equal("dns server error: 4 server failure", err)
      assert.is_nil(lrucache:get(client.TYPE_A..":short:myhost7"))
    end)

    it("name errors are not stored", function()
      local rec = {
        errcode = 3,
        errstr = "name error",
      }
      mock_records = {
        ["myhost8.domain.com:"..client.TYPE_A] = rec,
        ["myhost8:"..client.TYPE_A] = rec,
      }

      local result, err = client.resolve("myhost8", { qtype = client.TYPE_A })
      assert.is_nil(result)
      assert.equal("dns server error: 3 name error", err)
      assert.is_nil(lrucache:get(client.TYPE_A..":short:myhost8"))
    end)

  end)


-- ==============================================
--    fqdn caching
-- ==============================================


  describe("fqdn", function()
    
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

    it("errors do not replace stale records", function()
      local rec1 = {{
        type = client.TYPE_A,
        address = "1.2.3.4",
        class = 1,
        name = "myhost9.domain.com",
        ttl = 0.1, 
      }}
      mock_records = {
        ["myhost9.domain.com:"..client.TYPE_A] = rec1,
      }

      local result, err = client.resolve("myhost9", { qtype = client.TYPE_A })
      -- check that the cache is properly populated
      assert.equal(rec1, result)
      assert.is_nil(err)
      assert.equal(rec1, lrucache:get(client.TYPE_A..":myhost9.domain.com"))
      
      sleep(0.15) -- make sure we surpass the ttl of 0.1 of the record, so it is now stale.
      -- new mock records, such that we return server failures instaed of records
      local rec2 = {
        errcode = 4,
        errstr = "server failure",
      }
      mock_records = {
        ["myhost9.domain.com:"..client.TYPE_A] = rec2,
        ["myhost9:"..client.TYPE_A] = rec2,
      }
      -- doing a resolve will trigger the background query now
      result, err = client.resolve("myhost9", { qtype = client.TYPE_A })
      assert.is_true(result.expired)  -- we get the stale record, now marked as expired
      -- wait again for the background query to complete
      sleep(0.1)
      -- background resolve is now complete, check the cache, it should still have the 
      -- stale record, and it should not have been replaced by the error
      assert.equal(rec1, lrucache:get(client.TYPE_A..":myhost9.domain.com"))
    end)

    it("name errors do replace stale records", function()
      local rec1 = {{
        type = client.TYPE_A,
        address = "1.2.3.4",
        class = 1,
        name = "myhost9.domain.com",
        ttl = 0.1, 
      }}
      mock_records = {
        ["myhost9.domain.com:"..client.TYPE_A] = rec1,
      }

      local result, err = client.resolve("myhost9", { qtype = client.TYPE_A })
      -- check that the cache is properly populated
      assert.equal(rec1, result)
      assert.is_nil(err)
      assert.equal(rec1, lrucache:get(client.TYPE_A..":myhost9.domain.com"))
      
      sleep(0.15) -- make sure we surpass the ttl of 0.1 of the record, so it is now stale.
      -- clear mock records, such that we return name errors instead of records
      local rec2 = {
        errcode = 3,
        errstr = "name error",
      }
      mock_records = {
        ["myhost9.domain.com:"..client.TYPE_A] = rec2,
        ["myhost9:"..client.TYPE_A] = rec2,
      }
      -- doing a resolve will trigger the background query now
      result, err = client.resolve("myhost9", { qtype = client.TYPE_A })
      assert.is_true(result.expired)  -- we get the stale record, now marked as expired
      -- wait again for the background query to complete
      sleep(0.1)
      -- background resolve is now complete, check the cache, it should now have been
      -- replaced by the name error
      assert.equal(rec2, lrucache:get(client.TYPE_A..":myhost9.domain.com"))
    end)

  end)

-- ==============================================
--    success type caching
-- ==============================================


  describe("success types", function()

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

    it("in add. section are not stored for non-listed types", function()
      mock_records = {
        ["demo.service.consul:" .. client.TYPE_SRV] = {
          {
            type = client.TYPE_SRV,
            class = 1,
            name = "demo.service.consul",
            target = "192.168.5.232.node.api_test.consul",
            priority = 1,
            weight = 1,
            port = 32776,
            ttl = 0,
          }, {
            type = client.TYPE_TXT,  -- Not in the `order` as configured !
            class = 1,
            name = "192.168.5.232.node.api_test.consul",
            txt = "consul-network-segment=",
            ttl = 0,
          },
        }
      }
      client.toip("demo.service.consul")
      local success = client.getcache():get("192.168.5.232.node.api_test.consul")
      assert.not_equal(client.TYPE_TXT, success)
    end)

    it("in add. section are stored for listed types", function()
      mock_records = {
        ["demo.service.consul:" .. client.TYPE_SRV] = {
          {
            type = client.TYPE_SRV,
            class = 1,
            name = "demo.service.consul",
            target = "192.168.5.232.node.api_test.consul",
            priority = 1,
            weight = 1,
            port = 32776,
            ttl = 0,
          }, {
            type = client.TYPE_A,    -- In configured `order` !
            class = 1,
            name = "192.168.5.232.node.api_test.consul",
            address = "192.168.5.232",
            ttl = 0,
          }, {
            type = client.TYPE_TXT,  -- Not in the `order` as configured !
            class = 1,
            name = "192.168.5.232.node.api_test.consul",
            txt = "consul-network-segment=",
            ttl = 0,
          },
        }
      }
      client.toip("demo.service.consul")
      local success = client.getcache():get("192.168.5.232.node.api_test.consul")
      assert.equal(client.TYPE_A, success)
    end)

    it("are not overwritten by add. section info", function()
      mock_records = {
        ["demo.service.consul:" .. client.TYPE_SRV] = {
          {
            type = client.TYPE_SRV,
            class = 1,
            name = "demo.service.consul",
            target = "192.168.5.232.node.api_test.consul",
            priority = 1,
            weight = 1,
            port = 32776,
            ttl = 0,
          }, {
            type = client.TYPE_A,    -- In configured `order` !
            class = 1,
            name = "another.name.consul",
            address = "192.168.5.232",
            ttl = 0,
          },
        }
      }
      client.getcache():set("another.name.consul", client.TYPE_AAAA)
      client.toip("demo.service.consul")
      local success = client.getcache():get("another.name.consul")
      assert.equal(client.TYPE_AAAA, success)
    end)

  end)

end)
