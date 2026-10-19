<?php
/**
 * FI_RUNTIME=revolt|amp|react|phasync|swoole selects the adapter (default revolt).
 * REDIS_URI is the test server (default matches the README's docker command).
 */
require __DIR__ . '/../vendor/autoload.php';
if (!class_exists('Scope')) {
    require_once __DIR__ . '/../vendor/fiber-interop/api/tests/Support/ScopeStub.php';
}

$runtime = getenv('FI_RUNTIME') ?: 'revolt';
$class = 'FiberInterop\\' . ucfirst($runtime) . '\\Adapter';
fiber\setup(new $class());
fwrite(STDERR, "runtime=$runtime\n");

define('REDIS_URI', getenv('REDIS_URI') ?: 'tcp://127.0.0.1:16379');
$u = parse_url(REDIS_URI);
define('REDIS', ['scheme' => $u['scheme'], 'host' => $u['host'], 'port' => $u['port'] ?? 6379]);
