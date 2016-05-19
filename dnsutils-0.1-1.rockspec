package = "dnsutils"
version = "0.1-1"
source = {
  url = "https://github.com/Mashape/dnsutils.lua/archive/version_1.0.tar.gz",
  dir = "dnsutils-v.1"
}
description = {
  summary = "DNS config file parsing library",
  detailed = [[
    Parsed the `hosts` and `resolv.conf` configuration files and 
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
    ["dnsutils"] = "src/dnsutils.lua",
  },
}
