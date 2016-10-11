Overview
========

Lua library containing a dns client and utilities to parse dns configuration files; `resolv.conf` and `/etc/hosts`.

The module is currently OpenResty only.

Features
========

 - resolves A, AAAA, CNAME and SRV records, including port
 - caches dns query results
 - synchronizes requests (a single request for many requestors, eg. when cached ttl expires under heavy load)
 - `toip` applies a local round-robin scheme on the query results
 - parses `/etc/hosts`
 - parses `/resolv.conf` and applies `LOCALDOMAIN` and `RES_OPTIONS` variables

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

 - 0.1 (09-Sep-2016) Initial released version
