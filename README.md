Overview
========

Lua library containing a dns client, utilities, and a load-balancer.

The module is currently OpenResty only, and builds on top of the `resty-dns-resolver` library

Features
========

 - resolves A, AAAA, CNAME and SRV records, including port
 - parses `/etc/hosts`
 - parses `/resolv.conf` and applies `LOCALDOMAIN` and `RES_OPTIONS` variables
 - caches dns query results in memory
 - synchronizes requests (a single request for many requestors, eg. when cached ttl expires under heavy load)
 - `toip` applies a local (weighted) round-robin scheme on the query results
 - ring-balancer for round-robin and consistent-hashing approaches (experimental)

Copyright and license
=====================

Copyright: 2016 Mashape, Inc.

Author: Thijs Schreijer

License: [Apache 2.0](https://opensource.org/licenses/Apache-2.0)

Testing
=======

Tests are executed using `busted`, but because they run inside the `resty` cli tool, you must
use the `rbusted` script.

History
=======

###0.2.1 (24-Oct-2016) Bugfix
 
- fix: `toip()` failed on SRV records with only 1 entry

###0.2 (18-Oct-2016) Added the balancer
 
- fix: was creating resolver objects even if serving from cache
- change: change resolver order (SRV is now first by default) for dns servers that create both SRV and A records for each entry
- feature: make resolver order configurable
- feature: ring-balancer (experimental, no full test coverage yet)
- other: more test coverage for the dns client
   
###0.1 (09-Sep-2016) Initial released version
