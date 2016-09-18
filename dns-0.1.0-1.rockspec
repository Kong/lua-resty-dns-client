package = "dns"
version = "0.1.0-1"
source = {
  url = "https://github.com/Mashape/dns.lua/archive/0.1.0.tar.gz",
  dir = "dns.lua-0.1.0"
}
description = {
  summary = "DNS library",
  detailed = [[
    DNS client library for resolving hostnames. Utilities for 
    parsing the `hosts` and `resolv.conf` configuration files and 
    optionally the accompanying environment variables.
  ]],
  homepage = "https://github.com/Mashape/dnsutils.lua",
  license = "Apache 2.0"
}
dependencies = {
  "lua >= 5.1, < 5.4",
  "penlight > 1.1, < 2.0",
}
build = {
  type = "builtin",
  modules = {
    ["dns.utils"] = "src/dns/utils.lua",
    ["dns.client"] = "src/dns/client.lua",
  },
}
