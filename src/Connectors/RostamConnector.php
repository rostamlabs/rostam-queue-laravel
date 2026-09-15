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
 * A queue on Rostam is only as durable as the node never throwing records away.
 * At capacity a node either evicts or refuses the write, and which one it does
 * is decided by topology rather than by any flag: a single-node `rostam-server`
 * always evicts - silently, every write still answering success - and only
 * replicated shards refuse. Measured on v0.7.0-beta6: with a 256 MiB budget,
 * 400 one-megabyte writes all succeeded and 235 read back.
 *
 * Which setup a server is cannot be read off the wire, so the operator says:
 *
 *     'at_cap_policy' => 'reject_writes'   // a -cluster: at capacity, pushes fail
 *     'at_cap_policy' => 'headroom'        // a single node sized never to fill up
 *
 * What CAN be read, on rostam v0.7.0-beta3 and newer, is whether a node has
 * already evicted live records. {@see EvictionWatch} checks that before the
 * first job is accepted or taken, and again as the queue runs; "headroom" is
 * refused on a server too old to be checked, since nothing else would stand
 * between a full single node and silent loss.
 *
 * Nothing here touches the network. The first queue operation does, and it
 * checks before it touches a job.
 */
class RostamConnector implements ConnectorInterface
{
    private const DECLARATIONS = ['reject_writes', 'headroom'];

    public function __construct(private readonly ?Config $config = null) {}

    /**
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): QueueContract
    {
        $declared = $this->declaration($config);
        $client = TcpClient::fromArray($this->connection($config));

        return new RostamQueue(
            $client,
            prefix: (string) ($config['prefix'] ?? 'queues:'),
            default: (string) ($config['queue'] ?? 'default'),
            retryAfter: (int) ($config['retry_after'] ?? 90),
            sweepSeconds: (int) ($config['sweep_seconds'] ?? 60),
            tombstoneTtl: (int) ($config['tombstone_ttl'] ?? 604800),
            watch: new EvictionWatch(
                $client,
                required: $declared === 'headroom',
                everySeconds: (int) ($config['verify_every'] ?? 60),
            ),
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
    protected function declaration(array $config): string
    {
        $declared = $config['at_cap_policy'] ?? null;

        if (in_array($declared, self::DECLARATIONS, true)) {
            return $declared;
        }

        throw UnsafeQueueStore::policyNotDeclared(is_string($declared) ? $declared : null);
    }
}
