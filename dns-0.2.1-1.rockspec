package = "dns"
version = "0.2.1-1"
source = {
  url = "https://github.com/Mashape/dns.lua/archive/0.2.1.tar.gz",
  dir = "dns.lua-0.2.1"
}
description = {
  summary = "DNS library",
  detailed = [[
    DNS client library. Including utilities to parse configuration files and
    a ring-balancer for round-robin and consistent-hashing approaches.
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
    ["dns.balancer"] = "src/dns/balancer.lua",
  },
}
