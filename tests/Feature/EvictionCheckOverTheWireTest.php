<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Feature;

use Illuminate\Container\Container;
use PHPUnit\Framework\TestCase;
use Rostam\Kv\TcpClient;
use Rostam\Queue\EvictionWatch;
use Rostam\Queue\Exceptions\UnsafeQueueStore;
use Rostam\Queue\RostamQueue;
use Rostam\Testing\FakeServer;

/**
 * The eviction check end to end: a real socket, the client's metrics parser
 * and the server's own answer - including the error an older server gives for
 * an op it does not know, which is the one answer the check reads as "cannot
 * say" rather than as a failure.
 *
 * The unit tests drive EvictionWatch through a stand-in client. This is what
 * catches the stand-in and the wire disagreeing.
 */
class EvictionCheckOverTheWireTest extends TestCase
{
    private ?FakeServer $server = null;

    private string $prefix;

    protected function setUp(): void
    {
        parent::setUp();

        $this->prefix = 'itest:'.bin2hex(random_bytes(6)).':';
    }

    protected function tearDown(): void
    {
        $this->server?->stop();

        parent::tearDown();
    }

    private function fakeOnly(): void
    {
        if (FakeServer::isExternal()) {
            $this->markTestSkipped('a real server cannot be told to have evicted records, or to predate an op');
        }
    }

    private function client(bool $legacy = false, int $liveEvictions = 0): TcpClient
    {
        $this->server = FakeServer::start(legacy: $legacy, liveEvictions: $liveEvictions);

        return TcpClient::fromArray($this->server->connectionConfig());
    }

    private function queue(TcpClient $client, ?EvictionWatch $watch): RostamQueue
    {
        $queue = new RostamQueue($client, $this->prefix, 'default', retryAfter: 60, watch: $watch);
        $queue->setContainer(new Container);
        $queue->setConnectionName('rostam');

        return $queue;
    }

    public function test_a_node_that_has_evicted_live_records_refuses_the_push_and_nothing_is_written(): void
    {
        $this->fakeOnly();

        $client = $this->client(liveEvictions: 3);
        $queue = $this->queue($client, new EvictionWatch($client, required: true));

        foreach (['first', 'second'] as $attempt) {
            try {
                $queue->pushRaw(json_encode(['id' => $attempt]));
                $this->fail("the {$attempt} push was accepted by a node that has evicted live records");
            } catch (UnsafeQueueStore $exception) {
                $this->assertStringContainsString('has evicted 3 live record(s)', $exception->getMessage());
            }
        }

        $this->assertSame(0, $this->queue($client, null)->pendingSize());
    }

    public function test_a_node_that_has_evicted_live_records_does_not_hand_out_jobs(): void
    {
        $this->fakeOnly();

        $client = $this->client(liveEvictions: 1);
        $this->queue($client, null)->pushRaw(json_encode(['id' => 'waiting']));

        $this->expectException(UnsafeQueueStore::class);

        $this->queue($client, new EvictionWatch($client, required: true))->pop();
    }

    /**
     * An older server answers __kv_metrics__ with its generic error, and
     * `headroom` has nothing else to stand on: refused, on every operation.
     */
    public function test_a_server_that_predates_the_count_refuses_headroom(): void
    {
        $this->fakeOnly();

        $client = $this->client(legacy: true);
        $watch = new EvictionWatch($client, required: true);

        foreach ([1, 2] as $attempt) {
            try {
                $watch->check();
                $this->fail("headroom ran on a server that cannot report evictions (check {$attempt})");
            } catch (UnsafeQueueStore $exception) {
                $this->assertStringContainsString('rostam v0.7.0-beta3', $exception->getMessage());
            }
        }
    }

    /**
     * The same on a real server older than the op, where the error is the
     * server's and not the fake's idea of it. This is the stable lane in CI
     * while the stable rostam predates the count.
     */
    public function test_a_real_server_that_predates_the_count_refuses_headroom(): void
    {
        if (! FakeServer::isExternal() || FakeServer::supports('0.7.0-beta3')) {
            $this->markTestSkipped('needs a real rostam-server older than v0.7.0-beta3 (ROSTAM_TEST_SERVER_VERSION)');
        }

        $client = $this->client();
        $queue = $this->queue($client, new EvictionWatch($client, required: true));

        foreach (['push', 'pop'] as $operation) {
            try {
                $operation === 'push' ? $queue->pushRaw(json_encode(['id' => 'x'])) : $queue->pop();
                $this->fail("headroom let a {$operation} through on a server that cannot report evictions");
            } catch (UnsafeQueueStore $exception) {
                $this->assertStringContainsString('rostam v0.7.0-beta3', $exception->getMessage());
            }
        }

        $this->assertSame(0, $this->queue($client, null)->pendingSize());
    }

    /**
     * Against a real server this is the claim that matters in production: a
     * clean node of v0.7.0-beta3 or newer reports zero, and the queue runs.
     */
    public function test_a_clean_node_runs_a_headroom_queue(): void
    {
        if (! FakeServer::supports('0.7.0-beta3')) {
            $this->markTestSkipped('__kv_metrics__ arrived in rostam v0.7.0-beta3');
        }

        $client = $this->client();
        $evicted = $client->kvMetrics()->evictionsLive();

        // A server somebody has already filled up cannot show what a clean one
        // does. Refusing it is the behaviour two tests above; here it would only
        // look like a failure of the thing being demonstrated.
        if ($evicted > 0) {
            $this->markTestSkipped("this server has already evicted {$evicted} live record(s), so it is not a clean node");
        }

        $queue = $this->queue($client, new EvictionWatch($client, required: true));

        $queue->pushRaw(json_encode(['id' => 'kept']));
        $job = $queue->pop();

        $this->assertNotNull($job);
        $this->assertSame('kept', json_decode($job->getRawBody(), true)['id']);
        $job->delete();
    }
}
