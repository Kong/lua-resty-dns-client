Overview
========

Lua library containing a dns client, several utilities, and a load-balancer.

The module is currently OpenResty only, and builds on top of the 
[`lua-resty-dns`](https://github.com/openresty/lua-resty-dns) library

Features
========

 - resolves A, AAAA, CNAME and SRV records, including port
 - parses `/etc/hosts`
 - parses `/resolv.conf` and applies `LOCALDOMAIN` and `RES_OPTIONS` variables
 - caches dns query results in memory
 - synchronizes requests (a single request for many requestors, eg. when cached ttl expires under heavy load)
 - `toip` applies a local (weighted) round-robin scheme on the query results
 - ring-balancer for round-robin and consistent-hashing approaches

Copyright and license
=====================

Copyright: (c) 2016-2017 Mashape, Inc.

Author: Thijs Schreijer

License: [Apache 2.0](https://opensource.org/licenses/Apache-2.0)

Testing
=======

Tests are executed using `busted`, but because they run inside the `resty` cli tool, you must
use the `rbusted` script.

History
=======

###0.3.1 (22-Feb-2017) Bugfix

- Kubernetes dns returns an SRV record for individual nodes, where the target
  is the same name again (hence causing a recursive loop). Now those entries
  will be removed, and if nothing is left, it will fail the SRV lookup, causing
  a fall-through to the next record type.
- Kubernetes tends to return a port of 0 if none is provided/set, hence the
  `toip()` function now ignores a `port=0` and falls back on the port passed
  in.

###0.3.0 (8-Nov-2016) Major breaking update

- breaking: renamed a lot of things; method names, module names, etc. pretty
  much breaks everything... also releasing under a new name
- feature: udp function `setpeername` added (client)
- fix: do not synchronize dns queries for ttl=0 requests (client)
- fix: full test coverage and accompanying fixes (ring-balancer)
- feature: auto-retry for failed dns queries (ring-balancer)
- feature: updating weights is now supported without removing/re-adding (ring-balancer)
- change: auto-retry interval configurable for failed dns queries (ring-balancer)
- change: max life-time interval configurable for ttl=0 dns records (ring-balancer)

###0.2.1 (24-Oct-2016) Bugfix
 
- fix: `toip()` failed on SRV records with only 1 entry

###0.2 (18-Oct-2016) Added the balancer
 
- fix: was creating resolver objects even if serving from cache
- change: change resolver order (SRV is now first by default) for dns servers that create both SRV and A records for each entry
- feature: make resolver order configurable
- feature: ring-balancer (experimental, no full test coverage yet)
- other: more test coverage for the dns client
   
###0.1 (09-Sep-2016) Initial released version
