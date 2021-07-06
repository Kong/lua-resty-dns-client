local package_name = "lua-resty-dns-client"
local package_version = "6.0.2"
local rockspec_revision = "1"
local github_account_name = "Kong"
local github_repo_name = package_name


package = package_name
version = package_version.."-"..rockspec_revision
source = {
  url = "git://github.com/"..github_account_name.."/"..github_repo_name..".git",
  tag = package_version,
}
description = {
  summary = "DNS library",
  detailed = [[
    DNS client library. Including utilities to parse configuration files and
    a load balancers for round-robin, consistent-hashing, and least-
    connections approaches.
  ]],
  homepage = "https://github.com/"..github_account_name.."/"..github_repo_name,
  license = "Apache 2.0"
}
dependencies = {
  "lua >= 5.1, < 5.4",
  "penlight ~> 1",
  "lrandom",
  "lua-resty-timer ~> 1",
  "binaryheap >= 0.4",
  "luaxxhash >= 1.0",
}
build = {
  type = "builtin",
  modules = {
    ["resty.dns.utils"] = "src/resty/dns/utils.lua",
    ["resty.dns.client"] = "src/resty/dns/client.lua",
    ["resty.dns.balancer.base"] = "src/resty/dns/balancer/base.lua",
    ["resty.dns.balancer.consistent_hashing"] = "src/resty/dns/balancer/consistent_hashing.lua",
    ["resty.dns.balancer.least_connections"] = "src/resty/dns/balancer/least_connections.lua",
    ["resty.dns.balancer.handle"] = "src/resty/dns/balancer/handle.lua",
    ["resty.dns.balancer.round_robin"] = "src/resty/dns/balancer/round_robin.lua",
  },
}
