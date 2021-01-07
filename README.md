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
 - ring-balancer for:
   - (weighted) round-robin, and
   - consistent-hashing balancing
 - least-connections balancer


Copyright and license
=====================

Copyright: (c) 2016-2021 Kong, Inc.

Author: Thijs Schreijer

License: [Apache 2.0](https://opensource.org/licenses/Apache-2.0)

Testing
=======

Tests are executed using `busted`, but because they run inside the `resty` cli tool, you must
use the `rbusted` script.

For troubleshooting purposes: see the `/extra` folder for how to parse logs

History
=======

Versioning is strictly based on [Semantic Versioning](https://semver.org/)

Release process:

1. update the changelog below
2. update the rockspec file
3. generate the docs using `ldoc .`
4. commit and tag the release
5. upload rock to LuaRocks

### 5.2.0 (7-Jan-2021)

- Fix: now a single timer is used to check for expired records instead of one
  per host, significantly reducing the number of resources required for DNS
  resolution. [PR 112](https://github.com/Kong/lua-resty-dns-client/pull/112)


### 5.1.1 (7-Oct-2020)

- Dependency: Bump lua-resty-timer to 1.0

### 5.1.0 (28-Sep-2020)

- Fix: workaround for LuaJIT/ARM bug, see [Issue 93](https://github.com/Kong/lua-resty-dns-client/issues/93).
- Fix: table reduction was calculated wrong. Not a "functional" bug, just causing
  slightly less agressive memory releasing.
- Added: alternative implementation of the consistent-hashing balancing algorithm,
  which does not rely on the addresses addition and removal order to build the
  same request distribution among different instances. See
  [PR 97](https://github.com/Kong/lua-resty-dns-client/pull/97).

### 5.0.0 (14-May-2020)

- BREAKING: `getPeer` now returns the host-header value instead of the hostname
  that was used to add the address. This is only breaking if a host was added through
  `addHost` with an ip-address. In that case `getPeer` will no longer return the
  ip-address as the hostname, but will now return `nil`. See
  [PR 89](https://github.com/Kong/lua-resty-dns-client/pull/89).
- Added: option `useSRVname`, if truthy then `getPeer` will return the name as found
  in the SRV record, instead of the hostname as added to the balancer.
  See [PR 89](https://github.com/Kong/lua-resty-dns-client/pull/89).
- Added: callback return an extra parameter; the host-header for the address added/removed.
- Fix: using the module instance instead of the passed one for dns resolution
  in the balancer (only affected testing). See [PR 88](https://github.com/Kong/lua-resty-dns-client/pull/88).

### 4.2.0 (23-Mar-2020)

- Change: export DNS source type on status report. See [PR 86](https://github.com/Kong/lua-resty-dns-client/pull/86).

### 4.1.3 (24-Jan-2020)

- Fix: fix ttl-0 records issues with the balancer, see Kong issue
  https://github.com/Kong/kong/issues/5477
  * the previous record was not properly detected as a ttl=0 record
    by checking on the `__ttl0flag` we now do
  * since the "fake" SRV record wasn't updated with a new expiry
    time the expiry-check-timer would keep updating that record
    every second

### 4.1.2 (10-Dec-2019)

- Fix: handle cases when `lastQuery` is `nil`, see [PR 81](https://github.com/Kong/lua-resty-dns-client/pull/81)
and [PR 82](https://github.com/Kong/lua-resty-dns-client/pull/82).

### 4.1.1 (14-Nov-2019)

- Fix: added logging of try-list to the TCP/UDP wrappers, see [PR 75](https://github.com/Kong/lua-resty-dns-client/pull/75).
- Fix: reduce logging noise of the requery timer

### 4.1.0 (7-Aug-2019)

- Fix: unhealthy balancers would not recover because they would not refresh the
  DNS records used. See [PR 73](https://github.com/Kong/lua-resty-dns-client/pull/73).
- Added: automatic background resolving of hostnames, expiry will be checked
  every second, and if needed DNS (and balancer) will be updated. See [PR 73](https://github.com/Kong/lua-resty-dns-client/pull/73).

### 4.0.0 (26-Jun-2019)

- BREAKING: the balancer callback is called with a new event; "health" whenever
  the health status of the balancer changes.
- BREAKING: renamed `setPeerStatus` to `setAddressStatus` to be in line with the
  new `setHostStatus`, and prevent confusion.
- Added: keep track of unavailable weight. Added the `getStatus` method to
  return health, of the entire balancer structure. Health itself is determined
  based on the new property `healthThreshold`.
- Added: prevention of cascading failures when balancer is unhealthy. Use the
  `healthThreshold` value to set when the balancer is considered unhealthy.
- Added: method `setHostStatus`, to set the availability/health state of all
  addresses belonging to a host at once.
- Fix: when an asyncquery failed to create the timer, it would silently ignore
  the error. Error is now being logged.

### 3.0.2 (8-Mar-2019) Bugfix

- Fix: callback for adding an address did not pass the address object, but
  instead passed the balancer object twice.

### 3.0.1 (5-Mar-2019) Bugfix

- Fix: "balancer is nil" error, see issue #49.

### 3.0.0 (7-Nov-2018) Refactor & least-connections balancer

- Refactor: split the balancer in a base class (handling DNS resolution) and
  the ring-balancer, implementing the algorithm.
- Added: new least-connections balancer
- Fix: since addresses could occasionally hold names instead of IP addresses,
  it could happen that a call to `setPeerStatus` was unsuccessful, because the
  IP address would not match the name in the `address` object. Now a
  `handle` is returned by `getPeer`.
- BREAKING: `getPeer` signature (and return values) changed, making this a
  breaking change.

### 2.2.0 (28-Aug-2018) Fixes and a new option

- Added: a new option `validTtl` that, if set, will forcefully override the
  `ttl` value of any valid answer received. [Issue 48](https://github.com/Kong/lua-resty-dns-client/issues/48).
- Fix: remove multiline log entries, now encoded as single-line json. [Issue 52](https://github.com/Kong/lua-resty-dns-client/issues/52).
- Fix: always inject a `localhost` value, even if not in `/etc/hosts`. [Issue 54](https://github.com/Kong/lua-resty-dns-client/issues/54).
- Fix: added a workaround for Amazon Route 53 nameservers replying with a
  `ttl=0` whilst the record has a non-0 ttl. [Issue 56](https://github.com/Kong/lua-resty-dns-client/issues/56).

### 2.1.0 (21-May-2018) Fixes

- Fix: the round robin scheme for the balanceer starts at a randomized position
  to prevent all workers from starting with the same peer.
- Fix: the balancer no longer returns `port = 0` for SRV records without a
  port, the default port is now returned.
- Fix: ipv6 nameservers with a scope in their address are not supported. This
  fix will simply skip them instead of throwing errors upon resolving. Fixes
  [issue 43](https://github.com/Kong/lua-resty-dns-client/issues/43).
- Minor: improved logging in the balancer
- Minor: relax requery default interval for failed dns queries from 1 to 30
  seconds.

### 2.0.0 (22-Feb-2018) Major performance improvement (balancer) and bugfixes

- BREAKING: improved performance and memory footprint for large balancers.
  80-85% less memory will be used, while creation time dropped by 85-90%. Since
  the `host:getPeer()` function signature changed, this is a breaking change.
- Change: BREAKING the errors for cache-only lookup failures and empty records
  have been changed.
- Fix: do not fail initialization without nameservers.
- Fix: properly recognize IPv6 in square brackets from the /etc/hosts file.
- Fix: do not set success-type to types we're not looking for. Fixes
  [Kong issue #3210](https://github.com/Kong/kong/issues/3210).
- Fix: store records from the additional section in cache
- Fix: do not overwrite stale data in the client cache with empty records

### 1.0.0 (14-Dec-2017) Fixes and IPv6

- Change: BREAKING all IPv6 addresses are now returned with square brackets
- Fix: properly recognize IPv6 addresses in square brackets

### 0.6.3 (27-Nov-2017) Fixes and flagging unhealthy peers

- Added: flag to mark an address as failed/unhealthy, see `setPeerStatus`
- Added: callback to receive balancer updates; addresses added-to/removed-from
  the balancer (after DNS updates for example).
- fix: SRV record entries with a weight 0 are now supported
- fix: failure of the last hostname to resolve (balancer)

### 0.6.2 (04-Sep-2017) Fixes and refactor

- Fix: balancer not returning hostname for named SRV entries. See
  [issue #17](https://github.com/Kong/lua-resty-dns-client/issues/17)
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
  added a flag `enable_ipv6` (default == `false`) to enable the usage of
  ipv6 nameservers.

### 0.4.1 (21-Apr-2017) Bugfix

- Fix: cname record caching causing excessive dns queries,
  see [Kong issue #2303](https://github.com/Kong/kong/issues/2303).

### 0.4.0 (30-Mar-2017) Bugfixes

- Change: BREAKING! modified hash treatment, must now be an integer > 0
- Added: BREAKING! a retry counter to fall-through on hashed-retries (changes
  the `getpeer` signature)
- Fix: the MAXNS (3) was not honoured, so more than 3 nameservers would be parsed
  from the `resolv.conf` file. Fixes [Kong issue #2290](https://github.com/Kong/kong/issues/2290).
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
