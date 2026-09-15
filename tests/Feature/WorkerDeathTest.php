<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Feature;

use Illuminate\Container\Container;
use PHPUnit\Framework\TestCase;
use Rostam\Kv\TcpClient;
use Rostam\Queue\RostamQueue;
use Rostam\Testing\FakeServer;

/**
 * The claim a queue is judged by, tested the way it actually fails.
 *
 * The unit suite simulates a dead worker by removing its lease, which is what
 * the engine would have done - but it is still this process deciding what
 * happened. Here a real child process claims a job over a real socket and is
 * killed mid-flight, and the question is whether the work comes back.
 *
 * Two answers are needed, and they pull in opposite directions: while the lease
 * is alive the job must NOT be handed to anyone else, and once it lapses it
 * must. A driver that gets only the first right loses work; one that gets only
 * the second runs it twice.
 */
class WorkerDeathTest extends TestCase
{
    private FakeServer $server;

    private TcpClient $client;

    /**
     * A key space of this test's own. The fake server is a fresh process each
     * time, but a real one (ROSTAM_TEST_SERVER) is shared and remembers - and a
     * queue is state, so a leftover cursor from the previous test would be
     * indistinguishable from a bug in this one.
     */
    private string $prefix;

    /** Short, so the test does not spend its life waiting for a lease. */
    private const LEASE_SECONDS = 2;

    protected function setUp(): void
    {
        parent::setUp();

        $this->server = FakeServer::start();
        $this->client = TcpClient::fromArray($this->server->connectionConfig());
        $this->prefix = 'itest:'.bin2hex(random_bytes(6)).':';
    }

    protected function tearDown(): void
    {
        $this->server->stop();

        parent::tearDown();
    }

    private function queue(?TcpClient $client = null): RostamQueue
    {
        $queue = new RostamQueue(
            $client ?? $this->client,
            $this->prefix,
            'default',
            retryAfter: self::LEASE_SECONDS,
        );
        $queue->setContainer(new Container);
        $queue->setConnectionName('rostam');

        return $queue;
    }

    /**
     * Runs a child PHP process that claims one job and exits without finishing
     * it - no delete, no release, exactly what a killed worker leaves behind.
     */
    private function claimAndDie(int $port): string
    {
        $script = <<<'PHP'
            <?php
            require %s;
            $client = \Rostam\Kv\TcpClient::fromArray(['host' => '127.0.0.1', 'port' => %d]);
            $queue = new \Rostam\Queue\RostamQueue($client, %s, 'default', retryAfter: %d);
            $queue->setContainer(new \Illuminate\Container\Container);
            $queue->setConnectionName('rostam');
            $job = $queue->pop();
            echo $job === null ? 'nothing' : json_decode($job->getRawBody(), true)['id'];
            exit(1);
            PHP;

        $file = tempnam(sys_get_temp_dir(), 'rq').'.php';
        file_put_contents($file, sprintf(
            $script,
            var_export(dirname(__DIR__, 2).'/vendor/autoload.php', true),
            $port,
            var_export($this->prefix, true),
            self::LEASE_SECONDS,
        ));

        // stderr goes to a file, not a pipe: a pipe nobody reads blocks the
        // child once it fills, and what the child said on the way down - a
        // fatal, a refused connection - is the only explanation a failure here
        // would have.
        $errors = (string) tempnam(sys_get_temp_dir(), 'rq-err');
        $process = proc_open([PHP_BINARY, $file], [1 => ['pipe', 'w'], 2 => ['file', $errors, 'w']], $pipes);
        $claimed = stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        $status = proc_close($process);
        $stderr = trim((string) file_get_contents($errors));
        @unlink($file);
        @unlink($errors);

        $this->assertSame(
            1,
            $status,
            'the child was supposed to die holding the job'.($stderr === '' ? '' : "\nchild stderr:\n".$stderr),
        );

        return $claimed;
    }

    public function test_a_job_outlives_the_worker_that_was_killed_holding_it(): void
    {
        $queue = $this->queue();
        $queue->pushRaw(json_encode(['id' => 'must-survive']));

        $this->assertSame('must-survive', $this->claimAndDie($this->server->port));

        // While the dead worker's lease is still running, the job is still
        // spoken for. Handing it out here would be a double run.
        $this->assertNull($queue->pop(), 'the job was handed out while its lease was still live');

        sleep(self::LEASE_SECONDS + 1);

        $recovered = $queue->pop();

        $this->assertNotNull($recovered, 'the job died with its worker');
        $this->assertSame('must-survive', json_decode($recovered->getRawBody(), true)['id']);
        $this->assertSame(1, $queue->redeliveredCount());
    }

    public function test_nothing_is_redelivered_while_every_worker_is_alive(): void
    {
        $queue = $this->queue();

        foreach (['a', 'b', 'c'] as $id) {
            $queue->pushRaw(json_encode(['id' => $id]));
        }

        $held = [];
        while ($job = $queue->pop()) {
            $held[] = $job;
        }

        $this->assertCount(3, $held);
        $this->assertSame(0, $queue->redeliveredCount());

        // Still nothing to redeliver a moment later: the leases are live.
        $this->assertNull($queue->pop());
        $this->assertSame(0, $queue->redeliveredCount());

        foreach ($held as $job) {
            $job->delete();
        }

        $this->assertSame(0, $queue->size());
    }

    /**
     * A job finished by its worker must never come back, however long the
     * queue runs afterwards - redelivery keys off the payload still being
     * there, and delete() is what removes it.
     */
    public function test_a_completed_job_is_never_redelivered(): void
    {
        $queue = $this->queue();
        $queue->pushRaw(json_encode(['id' => 'done']));

        $queue->pop()->delete();

        sleep(self::LEASE_SECONDS + 1);

        $this->assertNull($queue->pop());
        $this->assertSame(0, $queue->redeliveredCount());
    }
}
