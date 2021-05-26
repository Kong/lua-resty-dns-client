use Test::Nginx::Socket 'no_plan';

run_tests();

__DATA__

=== TEST 1: init_worker: phase not supported by `semaphore:wait`
--- http_config eval
qq {
    init_worker_by_lua_block {
        local client = require("resty.dns.client")
        assert(client.init())
        local host = "httpbin.org"
        local typ = client.TYPE_A
        local answers, err = client.resolve(host, { qtype = typ })
        ngx.log(ngx.ERR, "answers: ", answers)
        ngx.log(ngx.ERR, err)
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
answers: nil
error: 101 empty record received
--- no_error_log
dns lookup pool exceeded retries
API disabled in the context of init_worker_by_lua



=== TEST 2: access: phase supported by `semaphore:wait`
--- config
    location = /t {
        access_by_lua_block {
            local client = require("resty.dns.client")
            assert(client.init())
            local host = "httpbin.org"
            local typ = client.TYPE_A
            local answers, err = client.resolve(host, { qtype = typ })
            ngx.say("address name: ", answers[1].name)
        }
    }
--- request
GET /t
--- response_body
address name: httpbin.org



=== TEST 3: init_worker: phase not supported by lua-resty-dns `new` API
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
