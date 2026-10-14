<?php

namespace async\predis;

use Predis\Client;

/**
 * A Predis\Client whose tcp, tls and unix connections are async\predis\Connection: inside
 * async\run() every command suspends only the calling task; outside, it blocks as usual.
 * Same arguments as `new Predis\Client($parameters, $options)`; a `connections` entry in
 * $options overrides ours.
 */
function client(array|string $parameters = [], array $options = []): Client
{
    $options['connections'] = ($options['connections'] ?? []) + [
        'tcp' => Connection::class,
        'tls' => Connection::class,
        'unix' => Connection::class,
    ];
    return new Client($parameters ?: null, $options);
}
