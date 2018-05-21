package = "lua-resty-dns-client"
version = "2.1.0-1"
source = {
  url = "https://github.com/Kong/lua-resty-dns-client/archive/2.1.0.tar.gz",
  dir = "lua-resty-dns-client-2.1.0"
}
description = {
  summary = "DNS library",
  detailed = [[
    DNS client library. Including utilities to parse configuration files and
    a ring-balancer for round-robin and consistent-hashing approaches.
  ]],
  homepage = "https://github.com/Kong/lua-resty-dns-client",
  license = "Apache 2.0"
}
dependencies = {
  "lua >= 5.1, < 5.4",
  "penlight > 1.1, < 2.0",
  "lrandom",
}
build = {
  type = "builtin",
  modules = {
    ["resty.dns.utils"] = "src/resty/dns/utils.lua",
    ["resty.dns.client"] = "src/resty/dns/client.lua",
    ["resty.dns.balancer"] = "src/resty/dns/balancer.lua",
  },
}
