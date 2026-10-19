# fiber-interop/predis

Cooperative [Predis](https://github.com/predis/predis) for
[fiber-interop/api](https://github.com/fiber-interop/api): inside `fiber\run()` a Redis command
suspends only the calling task, on whichever adapter is installed. Outside `fiber\run()` it is plain
blocking Predis.

```
composer require fiber-interop/predis
```

```php
use function fiber\{run, go, await_all};

$values = run(fn() => await_all(array_map(
    fn($key) => go(fn() => fiber\predis\client('tcp://127.0.0.1:6379')->get($key)),
    ['a', 'b', 'c'],
)));
```

## API

- `fiber\predis\client(array|string $parameters = [], array $options = []): Predis\Client` —
  `new Predis\Client($parameters, $options)` with the `tcp`, `tls` and `unix` schemes mapped to
  `fiber\predis\Connection`. A `connections` entry in `$options` overrides ours per scheme.
- `fiber\predis\Connection extends Predis\Connection\StreamConnection` — use it directly in your own
  `connections` option if you build the client yourself. Everything above the socket (commands,
  pipelines, transactions, pub/sub loops, replication/cluster wrappers, exceptions) is Predis's own.

## How it waits: on the socket

The connection's socket is non-blocking. Connecting uses `STREAM_CLIENT_ASYNC_CONNECT` and waits on
`fiber\writable()` (bounded by Predis's `timeout` parameter); the TLS handshake loops
`stream_socket_enable_crypto()` on `fiber\readable()`; writes loop `fwrite()` on `fiber\writable()`
(partial writes); replies are parsed from a buffer filled by `fread()` on `fiber\readable()`. Each
wait is bounded by `read_write_timeout` exactly as in Predis (unset: `default_socket_timeout`;
`<= 0`: forever). Refused, timed-out and lost connections raise Predis's `ConnectionException`.
Outside `fiber\run()` the same loops wait with `stream_select()`.

## Limits

- One connection is one socket: don't use the same `Predis\Client` from two tasks at once; give each
  task its own client (connections are cheap) or serialise access.
- `DEBUG SLEEP`, Lua scripts and other server-side work still block the single-threaded server for
  every client; only client-side waits (e.g. `BLPOP`) overlap.
- A connection made outside `fiber\run()` stays usable inside, and the reverse.
- Persistent connections (`persistent`) work but are shared per process, so the rule above applies.

## Tests

```
docker run -d --name fi-redis -p 127.0.0.1:16379:6379 redis:7
composer update
FI_RUNTIME=revolt vendor/bin/phpunit      # amp, react, phasync, swoole
```

`REDIS_URI` overrides the server (default `tcp://127.0.0.1:16379`). The suite covers overlap of ten
clients, other tasks running during `BLPOP`, pipelines, transactions, pub/sub, AUTH/SELECT,
connection refused inside and outside `run()`, a 1 MB round trip and plain blocking use.

## License

MIT
