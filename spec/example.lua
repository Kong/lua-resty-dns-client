local dnsutils = require "dnsutils"
local pretty = require "pl.pretty"

print("resolv.conf file;")
print(pretty(dnsutils.parse_resolv_conf()))

print("\nresolv.conf environment settings;")
print(pretty(dnsutils.apply_env({})))

print("\nresolv.conf including environment settings;")
print(pretty(dnsutils.apply_env(dnsutils.parse_resolv_conf())))

print("\nHosts file;")
print(pretty(dnsutils.parse_hosts()))
