<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Connectors;

use Illuminate\Contracts\Config\Repository as Config;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rostam\Kv\TcpClient;
use Rostam\Queue\EvictionWatch;
use Rostam\Queue\Exceptions\UnsafeQueueStore;
use Rostam\Queue\RostamQueue;

/**
 * Builds the queue, and refuses to build one that could eat jobs.
 *
 * A queue on Rostam is only as durable as the node never throwing away a record
 * it still holds, and a single-node `rostam-server` does exactly that under two
 * conditions, both measured:
 *
 * - at capacity, silently, every write still answering success (v0.7.0-beta6,
 *   256 MiB: 400 one-megabyte writes accepted, 235 readable);
 * - with plenty of room, by default: eviction is write-ordered, so a record that
 *   sits still while newer writes churn past it is evicted when the ring wraps.
 *   A queue churns by nature. On v0.7.0-beta7 with 32 MiB, put/delete churn of
 *   ten times the budget evicted two small keys that were never touched again,
 *   with nothing else live. `-relocating-eviction` rescued them.
 *
 * So the connection declares the one setup that holds, which cannot be read off
 * the wire:
 *
 *     'at_cap_policy' => 'headroom'   // one node, -relocating-eviction, live data well under max_memory
 *
 * What CAN be read, on rostam v0.7.0-beta3 and newer, is whether a node has
 * already evicted live records. {@see EvictionWatch} checks that before the
 * first job is accepted or taken, and again as the queue runs, and a server too
 * old to be checked is refused.
 *
 * "reject_writes" - a replicated `-cluster`, which refuses at capacity instead
 * of evicting - is refused too, for a different reason: a cluster answers a read
 * from whichever replica received it, and this driver reads a job's absence as
 * that job being finished. A lagging replica's answer would drop a job.
 *
 * Nothing here touches the network. The first queue operation does, and it
 * checks before it touches a job.
 */
class RostamConnector implements ConnectorInterface
{
    public function __construct(private readonly ?Config $config = null) {}

    /**
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): QueueContract
    {
        $this->declaration($config);
        $client = TcpClient::fromArray($this->connection($config));

        return new RostamQueue(
            $client,
            prefix: (string) ($config['prefix'] ?? 'queues:'),
            default: (string) ($config['queue'] ?? 'default'),
            retryAfter: (int) ($config['retry_after'] ?? 90),
            sweepSeconds: (int) ($config['sweep_seconds'] ?? 60),
            reclaimBatch: (int) ($config['reclaim_batch'] ?? 32),
            tombstoneTtl: (int) ($config['tombstone_ttl'] ?? 604800),
            watch: new EvictionWatch(
                $client,
                required: true,
                everySeconds: (int) ($config['verify_every'] ?? 60),
            ),
            dispatchAfterCommit: isset($config['after_commit']) ? (bool) $config['after_commit'] : null,
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
    protected function declaration(array $config): void
    {
        $declared = $config['at_cap_policy'] ?? null;

        if ($declared === 'headroom') {
            return;
        }

        if ($declared === 'reject_writes') {
            throw UnsafeQueueStore::clusterUnsupported();
        }

        throw UnsafeQueueStore::policyNotDeclared(is_string($declared) ? $declared : null);
    }
}
