local pretty = require("pl.pretty").write
local client = require("resty.dns.client")
client.init()

local function go(host, typ)
  local resp, err
  if typ then
    resp, err = client.resolve(host, {qtype = client["TYPE_"..typ]})
  else
    resp, err = client.resolve(host)
  end

  if not resp then
    print("Query failed: "..tostring(err))
  end

  print(pretty(resp))
  return resp
end

print "A TXT record"
go ("txttest.thijsschreijer.nl", "TXT")

print "Multiple A records"
go "atest.thijsschreijer.nl"

print "AAAA record"
go ("google.com", "AAAA")

print "A record redirected through 2 CNAME records"
go "smtp.thijsschreijer.nl"

print "Multiple SRV records"
local resp = go "srvtest.thijsschreijer.nl"
print "Priorities for this SRV record;"
-- results will be sorted by priority
print "> PRIMARY SET:"
local last = resp[1].priority
local backup = 0
for i, rec in ipairs(resp) do
  if last ~= rec.priority then
    backup = backup + 1
    print("> BACKUP SET: "..backup)
  end
  print("      "..rec.priority, rec.target)
end

print "CNAME to multiple SRV records"
go "cname2srv.thijsschreijer.nl"

print "Non-matching type records (returns empty list)"
go ("srvtest.thijsschreijer.nl", "A") --> not an A but an SRV type

print "Non-existing records (returns server error, in a table)"
go "IsNotHere.thijsschreijer.nl"

print "From the /etc/hosts file; localhost"
go "localhost"

print "From the /etc/hosts file; localhost AAAA"
go ("localhost", "AAAA")

print "an IPv4 address"
go ("1.2.3.4")

print "an IPv6 address"
go ("::1")

print "an IPv4 address, as SRV"
go ("1.2.3.4", "SRV")

print "an IPv6 address, as SRV"
go ("::1", "SRV")

