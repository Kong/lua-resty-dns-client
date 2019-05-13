local spy = require "luassert.spy"


describe("[handle]", function()


  local handle

  before_each(function()
    handle = require "resty.dns.balancer.handle"
  end)



  it("returning doesn't trigger __gc", function()
    local s = spy.new(function() end)
    local h = handle.get(s)
    handle.release(h)
    h = nil  --luacheck: ignore
    collectgarbage()
    collectgarbage()
    assert.spy(s).was_not.called()
  end)


  it("not returning triggers __gc", function()
    local s = spy.new(function() end)
    local h = handle.get(s)  --luacheck: ignore
    h = nil
    collectgarbage()
    collectgarbage()
    assert.spy(s).was.called()
  end)


  it("not returning doesn't fail without __gc", function()
    local h = handle.get()    --luacheck: ignore
    h = nil
    collectgarbage()
    collectgarbage()
  end)


  it("handles get re-used", function()
    local h = handle.get()
    local id = tostring(h)
    handle.release(h)
    h = handle.get()
    assert.equal(id, tostring(h))
  end)


  it("handles get cleared before re-use", function()
    local h = handle.get()
    local id = tostring(h)
    h.hello = "world"
    handle.release(h)
    h = handle.get()

    assert.equal(id, tostring(h))
    assert.is_nil(h.hello)
  end)


  it("beyond cache-size, handles are dropped", function()
    handle.setCacheSize(1)
    local h1 = handle.get()
    local h2 = handle.get()
    local id1 = tostring(h1)
    local id2 = tostring(h2)
    handle.release(h1)
    handle.release(h2)
    h1 = handle.get()
    h2 = handle.get()
    assert.equal(id1, tostring(h1))
    assert.not_equal(id2, tostring(h2))
  end)


  it("__gc is not invoked when handle beyond cache size is dropped", function()
    handle.setCacheSize(1)
    local s = spy.new(function() end)
    local h1 = handle.get(s)
    local h2 = handle.get(s)
    handle.release(h1)  -- returned to cache
    handle.release(h2)  -- dropped
    h1 = nil  --luacheck: ignore
    h2 = nil  --luacheck: ignore
    collectgarbage()
    collectgarbage()
    assert.spy(s).was_not.called()
  end)


  it("reducing cache-size drops whatever is too many", function()
    handle.setCacheSize(2)
    local h1 = handle.get()
    local h2 = handle.get()
    local id1 = tostring(h1)
    local id2 = tostring(h2)
    handle.release(h1)  -- returned to cache
    handle.release(h2)  -- returned to cache
    handle.setCacheSize(1)  -- the last one is now dropped
    h1 = handle.get()
    h2 = handle.get()
    assert.equal(id1, tostring(h1))
    assert.not_equal(id2, tostring(h2))
  end)

end)
