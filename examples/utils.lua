local dnsutils = require "dns.utils"
local pretty = require("pl.pretty").write

print("resolv.conf file;")
print(pretty(dnsutils.parseResolvConf()))

print("\nresolv.conf environment settings;")
print(pretty(dnsutils.applyEnv({})))

print("\nresolv.conf including environment settings;")
print(pretty(dnsutils.applyEnv(dnsutils.parseResolvConf())))

local rev, all = dnsutils.parseHosts()
print("\nHosts file (all entries);")
print(pretty(all))
print("\nHosts file (reverse lookup);")
print(pretty(rev))
