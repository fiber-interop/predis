<?php

namespace fiber\predis\Tests;

use PHPUnit\Framework\TestCase;
use Predis\Connection\ConnectionException;
use function fiber\{run, go, await, await_all, sleep};
use function fiber\predis\client;

final class PredisTest extends TestCase
{
    public function testOutsideRunBlockingWorks(): void
    {
        $c = client(REDIS);
        $c->set('fi:outside', 'v');
        $this->assertSame('v', $c->get('fi:outside'));
        $this->assertInstanceOf(\fiber\predis\Connection::class, $c->getConnection());
    }

    public function testOverlap(): void
    {
        $t = microtime(true);
        // DEBUG SLEEP would block the (single-threaded) server itself; BLPOP blocks only its client.
        $r = run(fn() => await_all(array_map(
            fn($i) => go(fn() => client(REDIS)->blpop(["fi:empty:$i"], 0.2)),
            range(1, 10),
        )));
        $elapsed = microtime(true) - $t;
        $this->assertSame(array_fill(0, 10, null), $r);
        $this->assertLessThan(1.5, $elapsed, "10 x BLPOP 0.2 s took {$elapsed}s");
    }

    public function testOtherTasksRunDuringBlockingCommand(): void
    {
        [$popped, $ticks] = run(function () {
            $c = client(REDIS);
            $c->del('fi:empty');
            $ticker = go(function () {
                $n = 0;
                for ($i = 0; $i < 5; $i++) {
                    sleep(0.05);
                    $n++;
                }
                return $n;
            });
            $popped = $c->blpop(['fi:empty'], 1);
            return [$popped, await($ticker)];
        });
        $this->assertNull($popped);
        $this->assertSame(5, $ticks);
    }

    public function testPipelineAndTransaction(): void
    {
        [$pipe, $tx] = run(function () {
            $c = client(REDIS);
            $pipe = $c->pipeline(function ($p) {
                $p->set('fi:p', '1');
                $p->incr('fi:p');
                $p->get('fi:p');
            });
            $tx = $c->transaction(function ($t) {
                $t->set('fi:t', 'a');
                $t->append('fi:t', 'b');
                $t->get('fi:t');
            });
            return [$pipe, $tx];
        });
        $this->assertSame('2', $pipe[2]);
        $this->assertSame(2, $pipe[1]);
        $this->assertSame('ab', $tx[2]);
    }

    public function testPubSub(): void
    {
        if (\extension_loaded('phasync') && \version_compare(\phpversion('phasync'), '0.5.0-beta7', '<')) {
            // phasync extension <= 0.5.0-beta6 runs foreach() over an Iterator (the pub/sub loop) as a C-originated
            // call and blocks the whole process in readable(); fixed in phasync-ext 3eea031 (Chatter task ext-writable).
            $this->markTestSkipped('phasync extension < 0.5.0-beta7: writable() hang during foreach over an Iterator');
        }
        $got = run(function () {
            $sub = go(function () {
                $c = client(REDIS + ['read_write_timeout' => 0]);
                $loop = $c->pubSubLoop();
                $loop->subscribe('fi:chan');
                foreach ($loop as $msg) {
                    if ($msg->kind === 'message') {
                        $loop->stop(true);
                        return $msg->payload;
                    }
                }
            });
            $pub = client(REDIS);
            // Publish until the subscriber is listening.
            for ($i = 0; $i < 100 && $pub->publish('fi:chan', 'hello') === 0; $i++) {
                sleep(0.01);
            }
            return await($sub, 2);
        });
        $this->assertSame('hello', $got);
    }

    public function testAuthAndSelect(): void
    {
        $r = run(function () {
            $admin = client(REDIS);
            $admin->executeRaw(['ACL', 'SETUSER', 'fi', 'on', '>secret', '~*', '+@all']);
            $c = client(REDIS + ['username' => 'fi', 'password' => 'secret', 'database' => 3]);
            $c->set('fi:db', 'three');
            return [$c->get('fi:db'), $admin->get('fi:db'), $c->executeRaw(['ACL', 'WHOAMI'])];
        });
        $this->assertSame('three', $r[0]);
        $this->assertNotSame('three', $r[1]);
        $this->assertSame('fi', $r[2]);
    }

    public function testConnectionRefused(): void
    {
        foreach (['outside' => fn($f) => $f(), 'inside' => fn($f) => run($f)] as $where => $wrap) {
            try {
                $wrap(fn() => client('tcp://127.0.0.1:1')->ping());
                $this->fail("no exception $where run()");
            } catch (ConnectionException $e) {
                $this->assertStringContainsString('refused', strtolower($e->getMessage()), $where);
            }
        }
    }

    public function testLargeValueRoundTrip(): void
    {
        $value = random_bytes(1 << 20);
        $got = run(function () use ($value) {
            $c = client(REDIS);
            $c->set('fi:big', $value);
            return [$c->get('fi:big'), $c->strlen('fi:big')];
        });
        $this->assertSame(strlen($value), $got[1]);
        $this->assertTrue($value === $got[0]);
    }
}
