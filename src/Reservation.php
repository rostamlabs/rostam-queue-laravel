<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Rostam\Contracts\KvClient;

/**
 * Who holds a job right now, and for how long.
 *
 * A queue that loses work is not a queue, so claiming a job must not destroy
 * it. The payload stays at its own key for the whole time a worker holds it;
 * what the worker takes is a LEASE - a separate key, created with `set_nx` so
 * exactly one of them can win, carrying a TTL so the engine expires it without
 * anybody having to sweep for it.
 *
 * That gives three states, readable from two keys and no scan at all:
 *
 *     job present, lease present   the worker is still on it
 *     job present, lease gone      the worker died; redeliver
 *     job gone                     finished
 *
 * The middle row is the one that matters. It is what a reservation index
 * exists for in every other driver - a sorted set in Redis, a column in the
 * database - and it is why this engine looked unable to host a queue at all:
 * finding expired reservations normally needs a scan, and there is none. It
 * does not need one, because the ids are dense. Walking them IS the index.
 */
final class Reservation
{
    public function __construct(
        private readonly KvClient $client,
        private readonly string $key,
        private readonly string $owner,
    ) {}

    /**
     * Take the lease, if nobody else holds it.
     *
     * @param  int  $seconds  how long before an unfinished job is redelivered
     */
    public function take(int $seconds): bool
    {
        return $this->client->setNx($this->key, $this->owner, max(1, $seconds));
    }

    /**
     * Is anyone holding it? A lapsed lease has already been removed by the
     * engine, so absence is the whole answer.
     */
    public function held(): bool
    {
        return $this->client->exists($this->key);
    }

    /**
     * Give it up, but only if it is still ours.
     *
     * Compare-and-delete rather than delete: a lease that lapsed while this
     * worker was slow now belongs to whoever took it next, and releasing it
     * out from under them would hand the same job to a third.
     */
    public function release(): bool
    {
        return $this->client->cad($this->key, $this->owner);
    }

    /**
     * Push the deadline out while the work is still going.
     */
    public function extend(int $seconds): bool
    {
        return $this->client->caex($this->key, $this->owner, max(1, $seconds));
    }

    public function owner(): string
    {
        return $this->owner;
    }
}
