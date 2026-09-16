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
 * A single-node rostam-server throws live records away - at capacity, and by
 * default under churn well before it - and a queued job is work that was
 * accepted. A queue that starts on such a store and then loses work is worse
 * than one that will not start, so the guard is a hard failure rather than a
 * warning nobody reads.
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

    /**
     * @param  array<string, mixed>  $config
     */
    private function connect(array $config, ?Repository $shared = null): RostamQueue
    {
        $queue = (new RostamConnector($shared ?? $this->config()))->connect($config + [
            'driver' => 'rostam',
            'at_cap_policy' => 'headroom',
        ]);

        $this->assertInstanceOf(RostamQueue::class, $queue);

        return $queue;
    }

    private static function read(RostamQueue $queue, string $property): mixed
    {
        return (new \ReflectionProperty($queue, $property))->getValue($queue);
    }

    public function test_it_refuses_a_connection_that_has_not_declared_its_policy(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/must declare at_cap_policy "headroom"/');

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
     * wrong - and "keep max_memory above the backlog" alone was not enough, a
     * default node evicts waiting records under churn with room to spare.
     */
    public function test_the_refusal_explains_itself(): void
    {
        try {
            (new RostamConnector($this->config()))->connect(['driver' => 'rostam']);
            $this->fail('the connector accepted an undeclared policy');
        } catch (UnsafeQueueStore $e) {
            $this->assertStringContainsString('-relocating-eviction', $e->getMessage());
            $this->assertStringContainsString('eviction follows write order', $e->getMessage());
            $this->assertStringContainsString('v0.7.0-beta3', $e->getMessage());
        }
    }

    /**
     * A cluster refuses writes instead of evicting, which is what a queue wants
     * - but it serves reads from any replica, and a lagging replica's "not
     * found" reads here as a finished job.
     */
    public function test_a_cluster_is_refused_with_the_reason(): void
    {
        try {
            (new RostamConnector($this->config()))->connect([
                'driver' => 'rostam',
                'at_cap_policy' => 'reject_writes',
            ]);
            $this->fail('the connector built a queue on a -cluster');
        } catch (UnsafeQueueStore $e) {
            $this->assertStringContainsString('whichever replica', $e->getMessage());
        }
    }

    public function test_a_single_node_with_headroom_can_be_declared(): void
    {
        $this->connect([]);
    }

    public function test_it_takes_the_server_from_the_shared_rostam_config(): void
    {
        // One server, described once - the cache driver already puts its
        // connections there and a queue should not need a second copy.
        $queue = $this->connect(
            ['connection' => 'other'],
            $this->config(['other' => ['host' => '10.0.0.9', 'port' => 7001]]),
        );

        $this->assertSame('tcp://10.0.0.9:7001', $queue->getClient()->config()->uri());
    }

    public function test_an_inline_host_wins_over_the_shared_config(): void
    {
        $queue = $this->connect(['host' => '10.0.0.5', 'port' => 7002]);

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
        $queue = $this->connect([
            'retry_after' => 600,
            'sweep_seconds' => 5,
            'reclaim_batch' => 7,
            'tombstone_ttl' => 3600,
            'queue' => 'emails',
            'prefix' => 'app:',
        ]);

        $this->assertSame(600, self::read($queue, 'retryAfter'), 'retry_after did not become the lease');
        $this->assertSame(5, self::read($queue, 'sweepSeconds'), 'sweep_seconds did not reach the migration window');
        $this->assertSame(7, self::read($queue, 'reclaimBatch'));
        $this->assertSame(3600, self::read($queue, 'tombstoneTtl'));
        $this->assertSame('emails', self::read($queue, 'default'));
        $this->assertSame('app:', self::read($queue, 'prefix'));
    }

    /**
     * `after_commit` is Laravel's own connection option: a job dispatched inside
     * a database transaction waits for the commit. It used to be ignored.
     */
    public function test_after_commit_reaches_the_queue(): void
    {
        $this->assertTrue(self::read($this->connect(['after_commit' => true]), 'dispatchAfterCommit'));
        $this->assertNull(self::read($this->connect([]), 'dispatchAfterCommit'));
    }

    /**
     * A killed slot has to outlive the lease of a worker that may still be
     * writing into it. Both are free settings, and the relationship between
     * them is the one thing holding several of the driver's races closed.
     */
    public function test_a_tombstone_that_does_not_outlive_a_lease_is_refused(): void
    {
        foreach ([[90, 90], [90, 30], [3600, 600]] as [$retryAfter, $tombstoneTtl]) {
            try {
                $this->connect(['retry_after' => $retryAfter, 'tombstone_ttl' => $tombstoneTtl]);
                $this->fail("tombstone_ttl {$tombstoneTtl} was accepted against retry_after {$retryAfter}");
            } catch (UnsafeQueueStore $e) {
                $this->assertStringContainsString('must be longer than retry_after', $e->getMessage());
            }
        }

        $this->connect(['retry_after' => 600, 'tombstone_ttl' => 601]);

        // Zero is the engine's "no expiry", so it outlives every lease there
        // will ever be - the safest setting, and the one the guard used to
        // refuse while telling the operator it was too short.
        $this->assertSame(0, self::read($this->connect(['tombstone_ttl' => 0]), 'tombstoneTtl'));
    }

    /**
     * The eviction count is node-wide, and a queue refused by it stays refused
     * until the SERVER restarts. On a node shared with something else that is
     * somebody else's evictions, so there is a way to say so - and it says what
     * it costs.
     */
    public function test_the_eviction_check_can_be_turned_off_deliberately(): void
    {
        $watching = (new \ReflectionProperty(RostamQueue::class, 'watch'));

        $this->assertNotNull($watching->getValue($this->connect([])), 'the check is on by default');
        $this->assertNull($watching->getValue($this->connect(['on_evictions' => 'ignore'])));

        try {
            $this->connect(['on_evictions' => 'warn']);
            $this->fail('an unknown on_evictions value was accepted');
        } catch (UnsafeQueueStore $e) {
            $this->assertStringContainsString('must be "refuse" or "ignore"', $e->getMessage());
        }
    }

    public function test_it_says_which_connection_is_missing(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/no connection named \[nowhere\]/');

        $this->connect(['connection' => 'nowhere']);
    }
}
