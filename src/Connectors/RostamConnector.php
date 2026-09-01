<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Connectors;

use Illuminate\Contracts\Config\Repository as Config;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rostam\Kv\TcpClient;
use Rostam\Queue\Exceptions\JobVanished;
use Rostam\Queue\Exceptions\UnsafeQueueStore;
use Rostam\Queue\RostamQueue;

/**
 * Builds the queue, and refuses to build one that would eat jobs.
 *
 * Rostam's default `AtCapPolicy` is `PolicyRingbufEvict`: at capacity it
 * overwrites the oldest entries in the oldest page. That is write order, not
 * LRU, and a queued job - written once, read once, then never touched again -
 * is precisely the shape eviction reaches first. A cache losing an entry is a
 * cache miss; a queue losing one is work that was accepted and never done.
 *
 * The obvious guard would be to ask the server which policy it runs. It cannot
 * be asked: no op reports the cache configuration or its statistics, so a
 * client has no way to see the policy at all. Rather than perform a check that
 * only looks like one, this asks the operator to state it, and starts only if
 * they do:
 *
 *     'queue.connections.rostam.at_cap_policy' => 'reject_writes'
 *
 * That is a declaration, not a detection, and the exception says so. What is
 * detectable is the damage after the fact - a hole in a dense id space - and
 * {@see JobVanished} covers that end.
 */
class RostamConnector implements ConnectorInterface
{
    public function __construct(private readonly ?Config $config = null) {}

    /**
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): QueueContract
    {
        $this->assertTheStoreWillKeepJobs($config);

        return new RostamQueue(
            TcpClient::fromArray($this->connection($config)),
            (string) ($config['prefix'] ?? 'queues:'),
            (string) ($config['queue'] ?? 'default'),
            (int) ($config['sweep_seconds'] ?? 60),
        );
    }

    /**
     * @param  array<string, mixed>  $config
     * @return array<string, mixed>
     */
    protected function connection(array $config): array
    {
        // The connection details live where the cache driver already puts them,
        // so one server is described once. An inline block wins if given.
        if (isset($config['host']) || isset($config['port'])) {
            return $config;
        }

        $name = (string) ($config['connection'] ?? $this->config?->get('rostam.default') ?? 'default');

        /** @var array<string, mixed>|null $connection */
        $connection = $this->config?->get('rostam.connections.'.$name);

        if (! is_array($connection)) {
            throw UnsafeQueueStore::noConnection($name);
        }

        return $connection;
    }

    /**
     * @param  array<string, mixed>  $config
     */
    protected function assertTheStoreWillKeepJobs(array $config): void
    {
        $declared = $config['at_cap_policy'] ?? null;

        if ($declared === 'reject_writes') {
            return;
        }

        throw UnsafeQueueStore::policyNotDeclared(is_string($declared) ? $declared : null);
    }
}
