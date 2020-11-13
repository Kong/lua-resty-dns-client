
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
      assert:set_parameter("TableFormatLevel", 10)
      collectgarbage()
      collectgarbage()
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
        assert.is_false((b:getStatus().healthy))
      end)

      it("adding first address marks healthy", function()
        assert.is_false(b:getStatus().healthy)
        b:addHost("127.0.0.1", 8000, 100)
        assert.is_true(b:getStatus().healthy)
      end)

      it("removing last address marks unhealthy", function()
        assert.is_false(b:getStatus().healthy)
        b:addHost("127.0.0.1", 8000, 100)
        assert.is_true(b:getStatus().healthy)
        b:removeHost("127.0.0.1", 8000)
        assert.is_false(b:getStatus().healthy)
      end)

      it("dropping below the health threshold marks unhealthy", function()
        assert.is_false(b:getStatus().healthy)
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        assert.is_true(b:getStatus().healthy)
        b:setAddressStatus(false, "127.0.0.2", 8000)
        assert.is_true(b:getStatus().healthy)
        b:setAddressStatus(false, "127.0.0.3", 8000)
        assert.is_false(b:getStatus().healthy)
      end)

      it("rising above the health threshold marks healthy", function()
        assert.is_false(b:getStatus().healthy)
        b:addHost("127.0.0.1", 8000, 100)
        b:addHost("127.0.0.2", 8000, 100)
        b:addHost("127.0.0.3", 8000, 100)
        b:setAddressStatus(false, "127.0.0.2", 8000)
        b:setAddressStatus(false, "127.0.0.3", 8000)
        assert.is_false(b:getStatus().healthy)
        b:setAddressStatus(true, "127.0.0.2", 8000)
        assert.is_true(b:getStatus().healthy)
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

          assert.same({
            healthy = true,
            weight = {
              total = 100,
              available = 100,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
            },
          }, b:getStatus())

          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("removing a host",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          assert.same({
            healthy = true,
            weight = {
              total = 100,
              available = 100,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
            },
          }, b:getStatus())

          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          b:removeHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 100,
              available = 100,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
            },
          }, b:getStatus())

        end)

        it("switching address availability",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          assert.same({
            healthy = true,
            weight = {
              total = 100,
              available = 100,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
            },
          }, b:getStatus())

          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to unavailable
          assert(b:setAddressStatus(false, "1.2.3.4", 8001, "arecord.tst"))
          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 125,
              unavailable = 25
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 25,
                  unavailable = 25
                },
                addresses = {
                  {
                    healthy = false,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to available
          assert(b:setAddressStatus(true, "1.2.3.4", 8001, "arecord.tst"))
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("changing weight of an available address",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          b:addHost("arecord.tst", 8001, 50) -- adding again changes weight
          assert.same({
            healthy = true,
            weight = {
              total = 200,
              available = 200,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 50,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 50
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 50
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("changing weight of an unavailable address",function()
          dnsA({
            { name = "arecord.tst", address = "1.2.3.4" },
            { name = "arecord.tst", address = "5.6.7.8" },
          })

          b:addHost("arecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 150,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 50,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to unavailable
          assert(b:setAddressStatus(false, "1.2.3.4", 8001, "arecord.tst"))
          assert.same({
            healthy = true,
            weight = {
              total = 150,
              available = 125,
              unavailable = 25
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 50,
                  available = 25,
                  unavailable = 25
                },
                addresses = {
                  {
                    healthy = false,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 25
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 25
                  },
                },
              },
            },
          }, b:getStatus())

          b:addHost("arecord.tst", 8001, 50) -- adding again changes weight
          assert.same({
            healthy = true,
            weight = {
              total = 200,
              available = 150,
              unavailable = 50
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "arecord.tst",
                port = 8001,
                nodeWeight = 50,
                weight = {
                  total = 100,
                  available = 50,
                  unavailable = 50
                },
                addresses = {
                  {
                    healthy = false,
                    ip = "1.2.3.4",
                    port = 8001,
                    weight = 50
                  },
                  {
                    healthy = true,
                    ip = "5.6.7.8",
                    port = 8001,
                    weight = 50
                  },
                },
              },
            },
          }, b:getStatus())
        end)

      end)

      describe("(SRV)", function()

        it("adding a host",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          b:addHost("srvrecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("removing a host",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          b:addHost("srvrecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          b:removeHost("srvrecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 100,
              available = 100,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("switching address availability",function()
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          b:addHost("srvrecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to unavailable
          assert(b:setAddressStatus(false, "1.1.1.1", 9000, "srvrecord.tst"))
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 110,
              unavailable = 10
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 10,
                  unavailable = 10
                },
                addresses = {
                  {
                    healthy = false,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to available
          assert(b:setAddressStatus(true, "1.1.1.1", 9000, "srvrecord.tst"))
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("changing weight of an available address (dns update)",function()
          local record = dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          b:addHost("srvrecord.tst", 8001, 10)
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 10,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          dnsExpire(record)
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 20 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 20 },
          })
          b:getPeer()  -- touch all adresses to force dns renewal
          b:addHost("srvrecord.tst", 8001, 99) -- add again to update nodeWeight

          assert.same({
            healthy = true,
            weight = {
              total = 140,
              available = 140,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 99,
                weight = {
                  total = 40,
                  available = 40,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 20
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 20
                  },
                },
              },
            },
          }, b:getStatus())
        end)

        it("changing weight of an unavailable address (dns update)",function()
          local record = dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
          })

          b:addHost("srvrecord.tst", 8001, 25)
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 120,
              unavailable = 0
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 20,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = true,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          -- switch to unavailable
          assert(b:setAddressStatus(false, "2.2.2.2", 9001, "srvrecord.tst"))
          assert.same({
            healthy = true,
            weight = {
              total = 120,
              available = 110,
              unavailable = 10
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 25,
                weight = {
                  total = 20,
                  available = 10,
                  unavailable = 10
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 10
                  },
                  {
                    healthy = false,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 10
                  },
                },
              },
            },
          }, b:getStatus())

          -- update weight, through dns renewal
          dnsExpire(record)
          dnsSRV({
            { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 20 },
            { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 20 },
          })
          b:getPeer()  -- touch all adresses to force dns renewal
          b:addHost("srvrecord.tst", 8001, 99) -- add again to update nodeWeight

          assert.same({
            healthy = true,
            weight = {
              total = 140,
              available = 120,
              unavailable = 20
            },
            hosts = {
              {
                host = "127.0.0.1",
                port = 8000,
                nodeWeight = 100,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "127.0.0.1",
                    port = 8000,
                    weight = 100
                  },
                },
              },
              {
                host = "srvrecord.tst",
                port = 8001,
                nodeWeight = 99,
                weight = {
                  total = 40,
                  available = 20,
                  unavailable = 20
                },
                addresses = {
                  {
                    healthy = true,
                    ip = "1.1.1.1",
                    port = 9000,
                    weight = 20
                  },
                  {
                    healthy = false,
                    ip = "2.2.2.2",
                    port = 9001,
                    weight = 20
                  },
                },
              },
            },
          }, b:getStatus())
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
        b:setAddressStatus(false, "127.0.0.1", 8000)
        b:setAddressStatus(false, "127.0.0.2", 8000)
        b:setAddressStatus(false, "127.0.0.3", 8000)
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

        b:setAddressStatus(false, "127.0.0.1", 8000)
        b:setAddressStatus(false, "127.0.0.2", 8000)
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

        b:setAddressStatus(false, "127.0.0.1", 8000)
        b:setAddressStatus(false, "127.0.0.2", 8000)
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )

        b:setAddressStatus(true, "127.0.0.2", 8000)
        assert.not_nil(b:getPeer())
      end)


      it("recovers when dns entries are replaced by healthy ones", function()
        local record = dnsA({
          { name = "getkong.org", address = "1.2.3.4" },
        })
        b:addHost("getkong.org", 8000, 50)
        assert.not_nil(b:getPeer())

        -- mark it as unhealthy
        assert(b:setAddressStatus(false, "1.2.3.4", 8000, "getkong.org"))
        assert.same({
            nil, "Balancer is unhealthy", nil, nil,
          }, {
            b:getPeer()
          }
        )

        -- expire DNS and add a new backend IP
        -- balancer should now recover since a new healthy backend is available
        record.expire = 0
        dnsA({
          { name = "getkong.org", address = "5.6.7.8" },
        })

        local timeout = ngx.now() + 5   -- we'll try for 5 seconds
        while true do
          assert(ngx.now() < timeout, "timeout")

          local ip = b:getPeer()
          if ip == "5.6.7.8" then
            break  -- expected result, success!
          end

          ngx.sleep(0.1)  -- wait a bit before retrying
        end

      end)

    end)

    describe("GC:", function()

      it("removed Hosts get collected",function()
        local b = balancer_module.new({
          dns = client,
        })
        b:addHost("127.0.0.1", 8000, 100)

        local test_table = setmetatable({}, { __mode = "v" })
        test_table.key = b.hosts[1]
        assert.not_nil(next(test_table))

        -- destroy it
        b:removeHost("127.0.0.1", 8000)
        collectgarbage()
        collectgarbage()
        assert.is_nil(next(test_table))
      end)


      it("dropped balancers get collected",function()
        local b = balancer_module.new({
          dns = client,
        })
        b:addHost("127.0.0.1", 8000, 100)

        local test_table = setmetatable({}, { __mode = "k" })
        test_table[b] = true
        assert.not_nil(next(test_table))

        -- destroy it
        ngx.sleep(0)  -- without this it fails, why, why, why?
        b = nil       -- luacheck: ignore

        collectgarbage()
        collectgarbage()
        --assert.is_nil(next(test_table))  -- doesn't work, hangs if failed, luassert bug
        assert.equal("nil", tostring(next(test_table)))
      end)

    end)

  end)

end
