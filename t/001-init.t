use Test::Nginx::Socket;

plan tests => repeat_each() * (blocks() * 3) + 8;

workers(6);

no_shuffle();
run_tests();

__DATA__

=== TEST 1: reuse timers for queries of same name, independent on # of workers
--- http_config eval
qq {
    init_worker_by_lua_block {
        local client = require("resty.dns.client")
        assert(client.init({
            nameservers = { "8.8.8.8" },
            hosts = {}, -- empty tables to parse to prevent defaulting to /etc/hosts
            resolvConf = {}, -- and resolv.conf files
            order = { "A" },
        }))
        local host = "httpbin.org"
        local typ = client.TYPE_A
        for i = 1, 10 do
            client.resolve(host, { qtype = typ })
        end

        local host = "mockbin.org"
        for i = 1, 10 do
            client.resolve(host, { qtype = typ })
        end

        workers = ngx.worker.count()
        timers = ngx.timer.pending_count()
    }
}
--- config
    location = /t {
        access_by_lua_block {
            local client = require("resty.dns.client")
            assert(client.init())
            local host = "httpbin.org"
            local typ = client.TYPE_A
            local answers, err = client.resolve(host, { qtype = typ })

            if not answers then
                ngx.say("failed to resolve: ", err)
            end

            ngx.say("first address name: ", answers[1].name)

            host = "mockbin.org"
            answers, err = client.resolve(host, { qtype = typ })

            if not answers then
                ngx.say("failed to resolve: ", err)
            end

            ngx.say("second address name: ", answers[1].name)

            ngx.say("workers: ", workers)

            -- should be 2 timers maximum (1 for each hostname)
            ngx.say("timers: ", timers)
        }
    }
--- request
GET /t
--- response_body
first address name: httpbin.org
second address name: mockbin.org
workers: 6
timers: 2
--- no_error_log
[error]
dns lookup pool exceeded retries
API disabled in the context of init_worker_by_lua



=== TEST 2: init_worker: phase not supported by `semaphore:wait`
--- http_config eval
qq {
    init_worker_by_lua_block {
        local client = require("resty.dns.client")
        assert(client.init())
        local host = "httpbin.org"
        local typ = client.TYPE_A
        answers, err = client.resolve(host, { qtype = typ })
    }
}
--- config
    location = /t {
        access_by_lua_block {
            ngx.say("answers: ", answers)
            ngx.say("err: ", err)
        }
    }
--- request
GET /t
--- response_body
answers: nil
err: dns client error: 101 empty record received
--- no_error_log
[error]
dns lookup pool exceeded retries
API disabled in the context of init_worker_by_lua



=== TEST 3: access: phase supported by `semaphore:wait`
--- config
    location = /t {
        access_by_lua_block {
            local client = require("resty.dns.client")
            assert(client.init())
            local host = "httpbin.org"
            local typ = client.TYPE_A
            local answers, err = client.resolve(host, { qtype = typ })

            if not answers then
                ngx.say("failed to resolve: ", err)
            end

            ngx.say("address name: ", answers[1].name)
        }
    }
--- request
GET /t
--- response_body
address name: httpbin.org
--- no_error_log
[error]
dns lookup pool exceeded retries
API disabled in the context of init_worker_by_lua



=== TEST 4: init_worker: phase not supported by lua-resty-dns `new` API
--- http_config eval
qq {
    init_worker_by_lua_block {
        local resolver = require "resty.dns.resolver"
        resolver:new({ nameservers = "8.8.8.8" })
    }
}
--- config
    location = /t {
        echo ok;
    }
--- request
GET /t
--- response_body
ok
--- error_log
API disabled in the context of init_worker_by_lua
in function 'udp'
in function 'new'
