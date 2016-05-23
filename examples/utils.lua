local dnsutils = require "dns.utils"
local pretty = require("pl.pretty").write

print("resolv.conf file;")
print(pretty(dnsutils.parse_resolv_conf()))

print("\nresolv.conf environment settings;")
print(pretty(dnsutils.apply_env({})))

print("\nresolv.conf including environment settings;")
print(pretty(dnsutils.apply_env(dnsutils.parse_resolv_conf())))

local rev, all = dnsutils.parse_hosts()
print("\nHosts file (all entries);")
print(pretty(all))
print("\nHosts file (reverse lookup);")
print(pretty(rev))
