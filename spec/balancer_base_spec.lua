
local client, balancer_base


local helpers = require "spec.test_helpers"
--local gettime = helpers.gettime
--local sleep = helpers.sleep
local dnsSRV = function(...) return helpers.dnsSRV(client, ...) end
local dnsA = function(...) return helpers.dnsA(client, ...) end
--local dnsAAAA = function(...) return helpers.dnsAAAA(client, ...) end
local dnsExpire = helpers.dnsExpire


describe("[balancer_base]", function()

  local snapshot

  setup(function()
    _G.package.loaded["dns.client"] = nil -- make sure module is reloaded
    _G._TEST = true  -- expose internals for test purposes
    balancer_base = require "resty.dns.balancer_base"
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



  describe("event order", function()

    local event_list, b

    setup(function()
      b = balancer_base.new({
        dns = client,
      })
      local addrInfo = function(addr, event)
        return {
          _event = event,
          address = addr.ip..":"..addr.port,
          weight = addr.weight,
          disabled = addr.disabled,
          available = addr.available,
        }
      end
      local hostInfo = function(host, event)
        local info = {
          _event = event,
          host = host.hostname..":"..host.port,
          nodeWeight = host.nodeWeight,
        }
        for i, addr in ipairs(host.addresses) do
          info[i] = addrInfo(addr)
        end
        return info
      end

      function b:onAddAddress(addr)
        table.insert(event_list, addrInfo(addr, "onAddAddress"))
        self.super.onAddAddress(self, addr)
      end
      function b:onRemoveAddress(addr)
        table.insert(event_list, addrInfo(addr, "onRemoveAddress"))
        self.super.onRemoveAddress(self, addr)
      end
      function b:afterHostUpdate(host)
        table.insert(event_list, hostInfo(host, "afterHostUpdate"))
        self.super.afterHostUpdate(self, host)
      end
      function b:beforeHostDelete(host)
        table.insert(event_list, hostInfo(host, "beforeHostDelete"))
        self.super.beforeHostDelete(self, host)
      end
      function b:touch_all()
        for _, addr in ipairs(self.addresses) do
          addr:getPeer()  -- will force dns update of expired records
        end
      end
    end)


    local record
    before_each(function()
      event_list = {}
      record = dnsSRV({
        { name = "konghq.com", target = "1.1.1.1", port = 3, weight = 6 },
        { name = "konghq.com", target = "2.2.2.2", port = 5, weight = 7 },
      })
    end)


    after_each(function()
      -- clear all the hosts from the test balancer
      for _, host in ipairs(b.hosts) do
        b:removeHost(host.hostname, host.port)
      end
    end)



    it("when adding a host", function()
      b:addHost("konghq.com", 8000, 100)
      assert.same({
        {
          _event = 'onAddAddress',
          address = '1.1.1.1:3',
          available = true,
          disabled = false,
          weight = 6
        }, {
          _event = 'onAddAddress',
          address = '2.2.2.2:5',
          available = true,
          disabled = false,
          weight = 7,
        }, {
          _event = 'afterHostUpdate',
          host = 'konghq.com:8000',
          nodeWeight = 100,
          {
            address = '1.1.1.1:3',
            available = true,
            disabled = false,
            weight = 6
          }, {
            address = '2.2.2.2:5',
            available = true,
            disabled = false,
            weight = 7,
          },
        }
      }, event_list)
    end)


    it("when removing a host", function()
      b:addHost("konghq.com", 8000, 100)
      event_list = {}  -- clear the list so we only get relevant events
      b:removeHost("konghq.com", 8000)
      assert.same({
        {
          _event = 'beforeHostDelete',
          host = 'konghq.com:8000',
          nodeWeight = 100,
          {                    -- both addresses still here, but disabled!
            address = '1.1.1.1:3',
            available = true,
            disabled = true,   -- marked as disabled!
            weight = 0,        -- weight reduced to 0!
          }, {
            address = '2.2.2.2:5',
            available = true,
            disabled = true,   -- marked as disabled!
            weight = 0,        -- weight reduced to 0!
          },
        }, {
          _event = 'onRemoveAddress',
          address = '2.2.2.2:5',
          available = true,
          disabled = true,   -- marked as disabled!
          weight = 0,        -- weight reduced to 0!
        }, {
          _event = 'onRemoveAddress',
          address = '1.1.1.1:3',
          available = true,
          disabled = true,   -- marked as disabled!
          weight = 0,        -- weight reduced to 0!
        },
      }, event_list)
    end)


    it("when removing a DNS record entry", function()
      b:addHost("konghq.com", 8000, 100)
      dnsExpire(record)  -- expire initial record
      record = dnsSRV({  -- insert a new record, 1 entry removed
        { name = "konghq.com", target = "1.1.1.1", port = 3, weight = 6 },
      })
      event_list = {}  -- clear the list so we only get relevant events
      b:touch_all()    -- touch them and force dns updates
      assert.same({
        {
          _event = 'afterHostUpdate',
          host = 'konghq.com:8000',
          nodeWeight = 100,
          {
            address = '1.1.1.1:3',
            available = true,
            disabled = false,
            weight = 6,
          }, {
            address = '2.2.2.2:5',
            available = true,
            disabled = true,   -- marked as disabled!
            weight = 0,        -- weight reduced to 0!
          },
        }, {
          _event = 'onRemoveAddress',
          address = '2.2.2.2:5',
          available = true,
          disabled = true,   -- marked as disabled!
          weight = 0,        -- weight reduced to 0!
        } ,
      }, event_list)
    end)


    it("when adding a DNS record entry", function()
      b:addHost("konghq.com", 8000, 100)
      dnsExpire(record)  -- expire initial record
      record = dnsSRV({  -- insert a new record, 1 new weight
        { name = "konghq.com", target = "1.1.1.1", port = 3, weight = 6 },
        { name = "konghq.com", target = "2.2.2.2", port = 5, weight = 7 },
        { name = "konghq.com", target = "8.8.8.8", port = 9, weight = 10 },
      })
      event_list = {}  -- clear the list so we only get relevant events
      b:touch_all()    -- touch them and force dns updates
      assert.same({
        {
          _event = 'onAddAddress',
          address = '8.8.8.8:9',
          available = true,
          disabled = false,
          weight = 10,
        }, {
          _event = 'afterHostUpdate',
          host = 'konghq.com:8000',
          nodeWeight = 100,
          {
            address = '1.1.1.1:3',
            available = true,
            disabled = false,
            weight = 6,
          }, {
            address = '2.2.2.2:5',
            available = true,
            disabled = false,
            weight = 7,
          }, {
            address = '8.8.8.8:9',
            available = true,
            disabled = false,
            weight = 10,
          },
        },
      }, event_list)
    end)


    it("when changing an SRV weight", function()
      b:addHost("konghq.com", 8000, 100)
      dnsExpire(record)  -- expire initial record
      record = dnsSRV({  -- insert a new record, 1 new weight
        { name = "konghq.com", target = "1.1.1.1", port = 3, weight = 6 },
        { name = "konghq.com", target = "2.2.2.2", port = 5, weight = 50 },
      })
      event_list = {}  -- clear the list so we only get relevant events
      b:touch_all()    -- touch them and force dns updates
      assert.same({
        {
          _event = 'afterHostUpdate',
          host = 'konghq.com:8000',
          nodeWeight = 100,
          {
            address = '1.1.1.1:3',
            available = true,
            disabled = false,
            weight = 6,
          }, {
            address = '2.2.2.2:5',
            available = true,
            disabled = false,
            weight = 50,            -- Updated weight!
          },
        },
      }, event_list)
    end)


    it("when changing an non-SRV weight", function()
      record = dnsA({
        { name = "getkong.org", address = "1.2.3.4" },
        { name = "getkong.org", address = "5.6.7.8" },
      })
      b:addHost("getkong.org", 8000, 100)
      event_list = {}  -- clear the list so we only get relevant events
      b:addHost("getkong.org", 8000, 5)  -- change the weights
      assert.same({
        {
          _event = 'afterHostUpdate',
          host = 'getkong.org:8000',
          nodeWeight = 5,
          {
            address = '1.2.3.4:8000',
            available = true,
            disabled = false,
            weight = 5,          -- weight updated to 5!
          }, {
            address = '5.6.7.8:8000',
            available = true,
            disabled = false,
            weight = 5,          -- weight updated to 5!
          },
        },
      }, event_list)
    end)

  end)

end)