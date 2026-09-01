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
 * Rostam evicts by write order under its default policy, and a queued job -
 * written once, read once - is the first thing that reaches. A queue that
 * starts on such a store and then loses work is worse than one that will not
 * start, so the guard is a hard failure rather than a warning nobody reads.
 *
 * It cannot check the server: no op reports the cache policy. What it can do is
 * make the operator say it, and these tests pin that it actually stops
 * something rather than being a config key with no teeth.
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
     */
    public function test_the_refusal_explains_itself(): void
    {
        try {
            (new RostamConnector($this->config()))->connect(['driver' => 'rostam']);
            $this->fail('the connector accepted an undeclared policy');
        } catch (UnsafeQueueStore $e) {
            $this->assertStringContainsString('PolicyRejectWrites', $e->getMessage());
            $this->assertStringContainsString('cannot check it and will not pretend to', $e->getMessage());
        }
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
