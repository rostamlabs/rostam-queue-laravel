<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Rostam\Contracts\KvClient;

/**
 * The two counters a queue is built from, and the one rule that keeps it honest.
 *
 * Rostam's key-value engine has no list and no sorted set, so a queue is a pair
 * of integers over a dense id space: `tail` allocates the next id to write,
 * `head` marks the next id to read, and the job itself lives at its own key.
 * Both counters are moved server-side, so no two callers can ever draw the same
 * id or claim the same job.
 *
 * THE RULE: head advances only when a slot was actually consumed.
 *
 * It is tempting to draw from head with `incr` the way tail allocates - one op,
 * perfectly distributed, and a race test of six workers over two thousand jobs
 * passes. It is also wrong. Two workers polling an EMPTY queue both draw, so
 * head runs past tail, and every id they burned is a slot a later push will
 * occupy and no pop will ever read:
 *
 *     head is now 3 while tail is 1
 *     new job at id 2, but head already passed it => silently unreadable
 *
 * A queue may drop nothing silently, so head moves by compare-and-swap instead:
 * it goes from h to h+1 only if it is still h, and only after that slot has
 * been dealt with. A losing swap means another worker moved it first, which is
 * not an error - it is the next attempt.
 */
final class Cursor
{
    public function __construct(
        private readonly KvClient $client,
        private readonly string $key,
    ) {}

    /**
     * Where the cursor stands. A counter that was never written reads as 0,
     * which is the correct genesis value for both ends.
     */
    public function value(): int
    {
        $raw = $this->client->get($this->key);

        return ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;
    }

    /**
     * Allocate the next id. Used by the writing end, where drawing an id is the
     * whole point and there is nothing to be conditional about.
     */
    public function next(): int
    {
        return $this->client->increment($this->key);
    }

    /**
     * Move from $from to $from + 1, and report whether this caller was the one
     * who did it.
     *
     * The compare is the entire safety property: two workers that both examined
     * slot $from will both try this, exactly one will succeed, and the other
     * learns to look again rather than assuming the slot was its own.
     */
    public function advanceFrom(int $from): bool
    {
        return $this->client->cas(
            $this->key,
            pack('J', $from + 1),
            $from === 0 ? null : pack('J', $from),
        );
    }

    public function key(): string
    {
        return $this->key;
    }
}
