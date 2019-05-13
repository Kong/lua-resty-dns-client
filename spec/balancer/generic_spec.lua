
local client -- forward declaration
local helpers = require "spec.test_helpers"
local dnsSRV = function(...) return helpers.dnsSRV(client, ...) end
local dnsA = function(...) return helpers.dnsA(client, ...) end
local dnsExpire = helpers.dnsExpire


for algorithm, balancer_module in helpers.balancer_types() do

  describe("[" .. algorithm .. "]", function()

    local snapshot

    setup(function()
      _G.package.loaded["resty.dns.client"] = nil -- make sure module is reloaded
      _G._TEST = true  -- expose internals for test purposes
      client = require "resty.dns.client"
    end)


    before_each(function()
      assert(client.init {
        hosts = {},
        resolvConf = {
          "nameserver 8.8.8.8"
        },
      })
      snapshot = assert:snapshot()
    end)


    after_each(function()
      snapshot:revert()  -- undo any spying/stubbing etc.
      collectgarbage()
      collectgarbage()
    end)


    describe("health:", function()

      local b

      before_each(function()
        b = balancer_module.new({
          dns = client,
          healthThreshold = 50,
        })
      end)

      after_each(function()
        b = nil
      end)

      it("empty balancer is unhealthy", function()
        assert.is_false((b:isHealthy()))
      end)

      it("adding first address marks healthy", function()
        assert.is_false((b:isHealthy()))
        b:addHost("127.0.0.1", 8000, 100)
        assert.is_true((b:isHealthy()))
      end)

      it("removing last address marks unhealthy", function()
        assert.is_false((b:isHealthy()))
        b:addHost("127.0.0.1", 8000, 100)
        assert.is_true((b:isHealthy()))
        b:removeHost("127.0.0.1", 8000)
        assert.is_false((b:isHealthy()))
      end)

      it("dropping below the health threshold marks unhealthy", function()
        assert.is_false((b:isHealthy()))
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        assert.is_true((b:isHealthy()))
        b:setPeerStatus(false, "127.0.0.2", 8000)
        assert.is_true((b:isHealthy()))
        b:setPeerStatus(false, "127.0.0.3", 8000)
        assert.is_false((b:isHealthy()))
      end)

      it("rising above the health threshold marks healthy", function()
        assert.is_false((b:isHealthy()))
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        b:setPeerStatus(false, "127.0.0.2", 8000)
        b:setPeerStatus(false, "127.0.0.3", 8000)
        assert.is_false((b:isHealthy()))
        b:setPeerStatus(true, "127.0.0.2", 8000)
        assert.is_true((b:isHealthy()))
      end)

    end)



    describe("weights:", function()

      local b

      before_each(function()
        b = balancer_module.new({
          dns = client,
        })
        b.getPeer = function(self)
          -- we do not really need to get a peer, just touch all addresses to
          -- potentially force DNS renewals
          for _, addr in ipairs(self.addresses) do
            addr:getPeer()
          end
        end
        b:addHost("127.0.0.1", 8000, 100)  -- add 1 initial host
      end)

      after_each(function()
        b = nil
      end)



      describe("(A)", function()

        it("adding a host",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)
        end)

        it("removing a host",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)

          b:removeHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)
        end)

        it("switching address availability",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)

          -- switch to unavailable
          assert(b:setPeerStatus(false, "1.2.3.4", 8001, "arecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(1 * 25, unavailable)

          -- switch to available
          assert(b:setPeerStatus(true, "1.2.3.4", 8001, "arecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)
        end)

        it("changing weight of an available address",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 50) -- adding again changes weight
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 50, weight)
          assert.are.equal(0, unavailable)
        end)

        it("changing weight of an unavailable address",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("arecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(0, unavailable)

          -- switch to unavailable
          assert(b:setPeerStatus(false, "1.2.3.4", 8001, "arecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 25, weight)
          assert.are.equal(1 * 25, unavailable)

          b:addHost("arecord.tst", 8001, 50) -- adding again changes weight
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 50, weight)
          assert.are.equal(1 * 50, unavailable)
        end)

      end)

      describe("(SRV)", function()

        it("adding a host",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("srvrecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)
        end)

        it("removing a host",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("srvrecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)

          b:removeHost("srvrecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)
        end)

        it("switching address availability",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("srvrecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)

          -- switch to unavailable
          assert(b:setPeerStatus(false, "1.1.1.1", 9000, "srvrecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(1 * 10, unavailable)

          -- switch to available
          assert(b:setPeerStatus(true, "1.1.1.1", 9000, "srvrecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)
        end)

        it("changing weight of an available address (dns update)",function()
          local record = dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("srvrecord.tst", 8001, 10)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)

          dnsExpire(record)
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 20 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 20 },
          })
          b:getPeer()  -- touch all adresses to force dns renewal

          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 20, weight)
          assert.are.equal(0, unavailable)
        end)

        it("changing weight of an unavailable address (dns update)",function()
          local record = dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          local _, weight, unavailable = b:isHealthy()
          assert.are.equal(100, weight)
          assert.are.equal(0, unavailable)

          b:addHost("srvrecord.tst", 8001, 25)
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(0, unavailable)

          -- switch to unavailable
          assert(b:setPeerStatus(false, "2.2.2.2", 9001, "srvrecord.tst"))
          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 10, weight)
          assert.are.equal(1 * 10, unavailable)

          -- update weight, through dns renewal
          dnsExpire(record)
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 20 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 20 },
          })
          b:getPeer()  -- touch all adresses to force dns renewal

          _, weight, unavailable = b:isHealthy()
          assert.are.equal(100 + 2 * 20, weight)
          assert.are.equal(1 * 20, unavailable)
        end)

      end)

    end)



    describe("getpeer()", function()

      local b

      before_each(function()
        b = balancer_module.new({
          dns = client,
          healthThreshold = 50,
        })
      end)

      after_each(function()
        b = nil
      end)


      it("returns expected results/types when using SRV", function()
        dnsSRV({
          { name = "konghq.com", target = "1.1.1.1", port = 2, weight = 3 },
        })
        b:addHost("konghq.com", 8000, 50)
        local ip, port, hostname, handle = b:getPeer()
        assert.equal("1.1.1.1", ip)
        assert.equal(2, port)
        assert.equal("konghq.com", hostname)
        assert.equal("userdata", type(handle.__udata))
      end)


      it("returns expected results/types when using A", function()
        dnsA({
          { name = "getkong.org", address = "1.2.3.4" },
        })
        b:addHost("getkong.org", 8000, 50)
        local ip, port, hostname, handle = b:getPeer()
        assert.equal("1.2.3.4", ip)
        assert.equal(8000, port)
        assert.equal("getkong.org", hostname)
        assert.equal("userdata", type(handle.__udata))
      end)


      it("fails when there are no addresses added", function()
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )
      end)


      it("fails when all addresses are unhealthy", function()
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        b:setPeerStatus(false, "127.0.0.1", 8000)
        b:setPeerStatus(false, "127.0.0.2", 8000)
        b:setPeerStatus(false, "127.0.0.3", 8000)
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )
      end)


      it("fails when balancer switches to unhealthy", function()
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        assert.not_nil(b:getPeer())

        b:setPeerStatus(false, "127.0.0.1", 8000)
        b:setPeerStatus(false, "127.0.0.2", 8000)
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )
      end)


      it("recovers when balancer switches to healthy", function()
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        assert.not_nil(b:getPeer())

        b:setPeerStatus(false, "127.0.0.1", 8000)
        b:setPeerStatus(false, "127.0.0.2", 8000)
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )

        b:setPeerStatus(true, "127.0.0.2", 8000)
        assert.not_nil(b:getPeer())
      end)

    end)

  end)

end