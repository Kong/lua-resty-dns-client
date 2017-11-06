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

### Unreleased

- Added: flag to mark an address as failed/unhealthy, see `setPeerStatus`
- Added: callback to receive balancer updates; addresses added-to/removed-from
  the balancer (after DNS updates for example).
- fix: SRV record entries with a weight 0 are now supported
- fix: failure of the last hostname to resolve (balancer)

### 0.6.2 (04-Sep-2017) Fixes and refactor

- Fix: balancer not returning hostname for named SRV entries. See
  [issue #17](https://github.com/Mashape/lua-resty-dns-client/issues/17)
- Fix: fix an occasionally failing test
- Refactor: remove metadata from the records, instead store it in its own cache

### 0.6.1 (28-Jul-2017) Randomization adjusted

- Change: use a different randomizer for the ring-balancer to predictably
  recreate the balancer in the exact same state (adds the `lrandom` library as
  a new dependency)

### 0.6.0 (14-Jun-2017) Rewritten resolver core to resolve async

- Added: resolution will be done async whenever possible. For this to work a new
  setting has been introduced `staleTtl` which determines for how long stale
  records will returned while a query is in progress in the background.
- Change: BREAKING! several functions that previously returned and took a
  resolver object no longer do so.
- Fix: no longer lookup ip adresses as names if the query type is not A or AAAA
- Fix: normalize names to lowercase after query
- Fix: set last-success types for hosts-file entries and ip-addresses

### 0.5.0 (25-Apr-2017) implement SEARCH and NDOTS

- Removed: BREAKING! stdError function removed.
- Added: implemented the `search` and `ndots` options.
- Change: `resolve` no longer returns empty results or dns errors as a table
  but as lua errors (`nil + error`).
- Change: `toip()` and `resolve()` have an extra result; history. A table with
  the list of tried names/types/results.
- Fix: timeout and retrans options from `resolv.conf` were ignored by the
  `client` module.
- Fix: nameservers with an ipv6 address would not be used properly. Also
  added a flag `enable_ipv6` (default == `false`) to enable the useage of
  ipv6 nameservers.

### 0.4.1 (21-Apr-2017) Bugfix

- Fix: cname record caching causing excessive dns queries,
  see [Kong issue #2303](https://github.com/Mashape/kong/issues/2303).

### 0.4.0 (30-Mar-2017) Bugfixes

- Change: BREAKING! modified hash treatment, must now be an integer > 0
- Added: BREAKING! a retry counter to fall-through on hashed-retries (changes
  the `getpeer` signature)
- Fix: the MAXNS (3) was not honoured, so more than 3 nameservers would be parsed
  from the `resolv.conf` file. Fixes [Kong issue #2290](https://github.com/Mashape/kong/issues/2290).
- Added: two convenience hash functions
- Performance: some improvements (pre-allocated tables for the slot lists)

### 0.3.2 (6-Mar-2017) Bugfixes

- Fix: Cleanup disabled addresses but did not delete them, causing errors when
  they were repeatedly added/removed
- Fix: potential racecondition when re-querying dns records
- Fix: potential memoryleak when a balancer object was released with a running timer

### 0.3.1 (22-Feb-2017) Bugfixes

- Kubernetes dns returns an SRV record for individual nodes, where the target
  is the same name again (hence causing a recursive loop). Now those entries
  will be removed, and if nothing is left, it will fail the SRV lookup, causing
  a fall-through to the next record type.
- Kubernetes tends to return a port of 0 if none is provided/set, hence the
  `toip()` function now ignores a `port=0` and falls back on the port passed
  in.

### 0.3.0 (8-Nov-2016) Major breaking update

- breaking: renamed a lot of things; method names, module names, etc. pretty
  much breaks everything... also releasing under a new name
- feature: udp function `setpeername` added (client)
- fix: do not synchronize dns queries for ttl=0 requests (client)
- fix: full test coverage and accompanying fixes (ring-balancer)
- feature: auto-retry for failed dns queries (ring-balancer)
- feature: updating weights is now supported without removing/re-adding (ring-balancer)
- change: auto-retry interval configurable for failed dns queries (ring-balancer)
- change: max life-time interval configurable for ttl=0 dns records (ring-balancer)

### 0.2.1 (24-Oct-2016) Bugfix
 
- fix: `toip()` failed on SRV records with only 1 entry

### 0.2 (18-Oct-2016) Added the balancer
 
- fix: was creating resolver objects even if serving from cache
- change: change resolver order (SRV is now first by default) for dns servers that create both SRV and A records for each entry
- feature: make resolver order configurable
- feature: ring-balancer (experimental, no full test coverage yet)
- other: more test coverage for the dns client
   
### 0.1 (09-Sep-2016) Initial released version
