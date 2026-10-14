<?php

namespace async\predis;

use Predis\Command\CommandInterface;
use Predis\Connection\ParametersInterface;
use Predis\Connection\StreamConnection;
use Predis\Response\Error as ErrorResponse;
use Predis\Response\Status as StatusResponse;

/**
 * Predis's StreamConnection with cooperative I/O. Inside async\run() connecting, the TLS handshake,
 * writing and reading wait on the socket with async\writable()/async\readable(), so only the calling
 * task is suspended. Outside run() the same non-blocking loops wait with stream_select(), which
 * behaves like the blocking original. Errors are Predis's own: ConnectionException for refused,
 * timed out or lost connections, ProtocolException for garbage.
 *
 * Timeouts are Predis's parameters: `timeout` (connect, default 5 s) and `read_write_timeout`
 * (per wait; unset means default_socket_timeout, <= 0 waits forever), as in the original.
 */
class Connection extends StreamConnection
{
    private string $buffer = '';

    private static function inRun(): bool
    {
        return \async\Registry::$active !== [];
    }

    /** Wait until $socket is readable (or writable); false on timeout. */
    private function wait($socket, bool $write, ?float $timeout = null): bool
    {
        if ($timeout === null) {
            $t = (float) ($this->parameters->read_write_timeout ?? ini_get('default_socket_timeout'));
            $timeout = $t > 0 ? $t : \PHP_FLOAT_MAX;
        }
        if (self::inRun()) {
            try {
                $write ? \async\writable($socket, $timeout) : \async\readable($socket, $timeout);
            } catch (\async\TimeoutException) {
                return false;
            }
            return true;
        }
        $r = $write ? [] : [$socket];
        $w = $write ? [$socket] : [];
        $e = [];
        $sec = $timeout === \PHP_FLOAT_MAX ? null : (int) $timeout;
        $usec = $sec === null ? null : (int) (($timeout - $sec) * 1e6);
        return @stream_select($r, $w, $e, $sec, $usec) > 0;
    }

    protected function createStreamSocket(ParametersInterface $parameters, $address, $flags)
    {
        if (!self::inRun()) {
            $resource = parent::createStreamSocket($parameters, $address, $flags);
            stream_set_blocking($resource, false);
            return $resource;
        }

        $timeout = isset($parameters->timeout) ? (float) $parameters->timeout : 5.0;
        $context = stream_context_create(['socket' => ['tcp_nodelay' => (bool) $parameters->tcp_nodelay]]);
        if (!empty($parameters->persistent) && !empty($parameters->conn_uid)) {
            $address .= '/' . $parameters->conn_uid;
        }

        $resource = @stream_socket_client($address, $errno, $errstr, $timeout, $flags | STREAM_CLIENT_ASYNC_CONNECT, $context);
        if (!$resource) {
            $this->onConnectionError(trim($errstr), $errno);
        }
        stream_set_blocking($resource, false);
        if (!$this->wait($resource, true, $timeout)) {
            fclose($resource);
            $this->onConnectionError('Connection timed out', 110);
        }
        // A failed asynchronous connect reports writable; the peer name tells success from failure.
        if (@stream_socket_get_name($resource, true) === false) {
            fclose($resource);
            $this->onConnectionError('Connection refused', 111);
        }

        return $resource;
    }

    protected function tlsStreamInitializer(ParametersInterface $parameters)
    {
        $resource = $this->tcpStreamInitializer($parameters);
        if (isset(stream_get_meta_data($resource)['crypto'])) {
            return $resource;
        }

        $options = isset($parameters->ssl) && is_array($parameters->ssl) ? $parameters->ssl : [];
        $options['crypto_type'] ??= STREAM_CRYPTO_METHOD_TLS_CLIENT;
        if (!stream_context_set_option($resource, ['ssl' => $options])) {
            $this->onConnectionError('Error while setting SSL context options');
        }

        // Non-blocking handshake: 0 means "call again when the socket is ready".
        while (($ok = @stream_socket_enable_crypto($resource, true, $options['crypto_type'])) === 0) {
            if (!$this->wait($resource, false)) {
                break;
            }
        }
        if ($ok !== true) {
            $this->onConnectionError('Error while switching to encrypted communication');
        }

        return $resource;
    }

    public function disconnect()
    {
        $this->buffer = '';
        parent::disconnect();
    }

    protected function write($buffer)
    {
        $socket = $this->getResource();

        while ($buffer !== '') {
            $written = is_resource($socket) ? @fwrite($socket, $buffer) : false;
            if ($written === false) {
                $this->onConnectionError('Error while writing bytes to the server.');
            }
            if ($written === 0) {
                if (!$this->wait($socket, true)) {
                    $this->onConnectionError('Error while writing bytes to the server.');
                }
                continue;
            }
            $buffer = (string) substr($buffer, $written);
        }
    }

    /** Read more bytes into the buffer, waiting if none are available. */
    private function fill($socket, string $what): void
    {
        while (true) {
            $chunk = is_resource($socket) ? @fread($socket, 65536) : false;
            if ($chunk !== false && $chunk !== '') {
                $this->buffer .= $chunk;
                return;
            }
            if ($chunk === false || feof($socket) || !$this->wait($socket, false)) {
                $this->onConnectionError("Error while reading $what from the server.");
            }
        }
    }

    public function read()
    {
        $socket = $this->getResource();

        while (($eol = strpos($this->buffer, "\r\n")) === false) {
            $this->fill($socket, 'line');
        }
        $prefix = $this->buffer[0];
        $payload = substr($this->buffer, 1, $eol - 1);
        $this->buffer = (string) substr($this->buffer, $eol + 2);

        switch ($prefix) {
            case '+':
                return StatusResponse::get($payload);

            case '$':
                $size = (int) $payload;
                if ($size === -1) {
                    return;
                }
                while (strlen($this->buffer) < $size + 2) {
                    $this->fill($socket, 'bytes');
                }
                $data = substr($this->buffer, 0, $size);
                $this->buffer = (string) substr($this->buffer, $size + 2);
                return $data;

            case '*':
                $count = (int) $payload;
                if ($count === -1) {
                    return;
                }
                $multibulk = [];
                for ($i = 0; $i < $count; ++$i) {
                    $multibulk[$i] = $this->read();
                }
                return $multibulk;

            case ':':
                $integer = (int) $payload;
                return $integer == $payload ? $integer : $payload;

            case '-':
                return new ErrorResponse($payload);

            default:
                $this->onProtocolError("Unknown response prefix: '$prefix'.");
                return;
        }
    }
}
