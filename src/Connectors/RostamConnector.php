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
 *   with nothing else live. `-relocating-eviction` kept them - best-effort, as
 *   the server describes it: it never allocates a page, never triggers another
 *   eviction and never fails a write, so a record that does not fit the room a
 *   freed page leaves is dropped anyway. Hence the second half of the
 *   declaration, and the eviction count as a backstop.
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

        $retryAfter = (int) ($config['retry_after'] ?? 90);
        $tombstoneTtl = (int) ($config['tombstone_ttl'] ?? 604800);

        // A killed slot has to outlive the lease of the worker that might still
        // be writing into it, or a slot killed while a lease was held comes back
        // to life under a lease nobody will release. Both are free settings; the
        // relationship between them is not. Zero is the engine's "no expiry",
        // which outlives everything - the safest setting, and the most memory.
        if ($tombstoneTtl !== 0 && $tombstoneTtl <= $retryAfter) {
            throw UnsafeQueueStore::tombstonesOutliveLeases($tombstoneTtl, $retryAfter);
        }

        $client = TcpClient::fromArray($this->connection($config));

        return new RostamQueue(
            $client,
            prefix: (string) ($config['prefix'] ?? 'queues:'),
            default: (string) ($config['queue'] ?? 'default'),
            retryAfter: $retryAfter,
            sweepSeconds: (int) ($config['sweep_seconds'] ?? 60),
            reclaimBatch: (int) ($config['reclaim_batch'] ?? 32),
            tombstoneTtl: $tombstoneTtl,
            watch: $this->watch($config, $client),
            dispatchAfterCommit: isset($config['after_commit']) ? (bool) $config['after_commit'] : null,
        );
    }

    /**
     * The eviction check, unless the operator has taken it off.
     *
     * The count is node-wide and cumulative since the server started, so on a
     * node shared with anything else it is somebody else's evictions too, and a
     * queue refused by it stays refused until the SERVER restarts - which, on a
     * node without -data, is the backlog gone. That is the right default and a
     * bad surprise, so it is a setting: `on_evictions => 'ignore'` runs without
     * the check, and accepts what the check was there to catch.
     *
     * @param  array<string, mixed>  $config
     */
    protected function watch(array $config, TcpClient $client): ?EvictionWatch
    {
        $on = $config['on_evictions'] ?? 'refuse';

        if ($on === 'ignore') {
            return null;
        }

        if ($on !== 'refuse') {
            throw UnsafeQueueStore::unknownEvictionPolicy(is_string($on) ? $on : get_debug_type($on));
        }

        return new EvictionWatch(
            $client,
            required: true,
            everySeconds: (int) ($config['verify_every'] ?? 60),
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
