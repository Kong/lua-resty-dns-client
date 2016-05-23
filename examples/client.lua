local pretty = require("pl.pretty").write
local client = require("dns.client")
client.init()

local function go(host, typ)
  local resp, err
  if typ then 
    resp, err = client.resolve_type(host, {qtype = client["TYPE_"..typ]})
  else
    resp, err = client.resolve(host)
  end
  
  if not resp then 
    print("Query failed: "..tostring(err))
  end
  
  print(pretty(resp))
end


print "Multiple A records"
go "atest.thijsschreijer.nl"

print "AAAA record" 
go ("google.com", "AAAA")

print "A record redirected through 2 CNAME records"
go "smtp.thijsschreijer.nl"

print "Multiple SRV records"
go "srvtest.thijsschreijer.nl"

print "CNAME to multiple SRV records"
go "cname2srv.thijsschreijer.nl"

print "Non-matching type records (returns empty list)"
go ("srvtest.thijsschreijer.nl", "A") --> not an A but an SRV type

print "Non-existing records (returns server error, in a table)"
go "IsNotHere.thijsschreijer.nl"
