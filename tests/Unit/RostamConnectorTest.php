<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use Illuminate\Config\Repository;
use PHPUnit\Framework\TestCase;
use Rostam\Queue\Connectors\RostamConnector;
use Rostam\Queue\Exceptions\UnsafeQueueStore;
use Rostam\Queue\RostamQueue;

/**
 * The connector's job is to refuse.
 *
 * A single-node rostam-server evicts at capacity, silently, and a queued job is
 * work that was accepted. A queue that starts on such a store and then loses
 * work is worse than one that will not start, so the guard is a hard failure
 * rather than a warning nobody reads.
 *
 * Which setup a server is cannot be read off the wire, so the operator
 * declares it; these tests pin that the declaration actually stops something
 * rather than being a config key with no teeth. What the wire CAN tell - a node
 * that has already evicted live records - is {@see EvictionWatchTest}'s.
 */
class RostamConnectorTest extends TestCase
{
    private function config(array $connections = []): Repository
    {
        return new Repository(['rostam' => [
            'default' => 'default',
            'connections' => $connections + ['default' => ['host' => '127.0.0.1', 'port' => 7000]],
        ]]);
    }

    public function test_it_refuses_a_connection_that_has_not_declared_its_policy(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/must declare at_cap_policy/');

        (new RostamConnector($this->config()))->connect(['driver' => 'rostam']);
    }

    public function test_it_refuses_a_policy_that_would_evict_jobs(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/got "ringbuf_evict"/');

        (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'at_cap_policy' => 'ringbuf_evict',
        ]);
    }

    /**
     * The message has to earn the refusal: an operator meeting it for the first
     * time needs to know what to change and why, not just that something is
     * wrong.
     *
     * It used to tell them to start the server with PolicyRejectWrites, which a
     * single-node rostam-server cannot be told to do. So it now names the two
     * setups that are actually safe, and says plainly which one a single node is.
     */
    public function test_the_refusal_explains_itself(): void
    {
        try {
            (new RostamConnector($this->config()))->connect(['driver' => 'rostam']);
            $this->fail('the connector accepted an undeclared policy');
        } catch (UnsafeQueueStore $e) {
            $this->assertStringContainsString('"reject_writes"', $e->getMessage());
            $this->assertStringContainsString('"headroom"', $e->getMessage());
            $this->assertStringContainsString('single-node rostam-server always evicts', $e->getMessage());
            $this->assertStringNotContainsString('Start the server with PolicyRejectWrites', $e->getMessage());
        }
    }

    public function test_a_single_node_with_headroom_can_be_declared(): void
    {
        $queue = (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'at_cap_policy' => 'headroom',
        ]);

        $this->assertInstanceOf(RostamQueue::class, $queue);
    }

    public function test_it_builds_a_queue_once_the_policy_is_declared(): void
    {
        $queue = (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'at_cap_policy' => 'reject_writes',
        ]);

        $this->assertInstanceOf(RostamQueue::class, $queue);
    }

    public function test_it_takes_the_server_from_the_shared_rostam_config(): void
    {
        // One server, described once - the cache driver already puts its
        // connections there and a queue should not need a second copy.
        $queue = (new RostamConnector($this->config(['other' => ['host' => '10.0.0.9', 'port' => 7001]])))
            ->connect([
                'driver' => 'rostam',
                'connection' => 'other',
                'at_cap_policy' => 'reject_writes',
            ]);

        $this->assertSame('tcp://10.0.0.9:7001', $queue->getClient()->config()->uri());
    }

    public function test_an_inline_host_wins_over_the_shared_config(): void
    {
        $queue = (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'host' => '10.0.0.5',
            'port' => 7002,
            'at_cap_policy' => 'reject_writes',
        ]);

        $this->assertSame('tcp://10.0.0.5:7002', $queue->getClient()->config()->uri());
    }

    /**
     * `retry_after` is the lease: how long a worker may hold a job before it
     * is handed to somebody else. The connector used to pass `sweep_seconds`
     * into that position, so an operator who set retry_after => 600 for a long
     * job got a 60-second lease and the same job running twice.
     */
    public function test_each_setting_reaches_the_queue_under_its_own_name(): void
    {
        $queue = (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'at_cap_policy' => 'reject_writes',
            'retry_after' => 600,
            'sweep_seconds' => 5,
        ]);

        $read = static fn (string $property) => (new \ReflectionProperty($queue, $property))->getValue($queue);

        $this->assertSame(600, $read('retryAfter'), 'retry_after did not become the lease');
        $this->assertSame(5, $read('sweepSeconds'), 'sweep_seconds did not reach the migration window');
    }

    public function test_it_says_which_connection_is_missing(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/no connection named \[nowhere\]/');

        (new RostamConnector($this->config()))->connect([
            'driver' => 'rostam',
            'connection' => 'nowhere',
            'at_cap_policy' => 'reject_writes',
        ]);
    }
}
