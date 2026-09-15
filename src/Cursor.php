<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Rostam\Contracts\KvClient;

/**
 * The counters a queue is built from, and the one rule that keeps it honest.
 *
 * Rostam's key-value engine has no list and no sorted set, so a queue is a pair
 * of integers over a dense id space: `tail` allocates the next id to write,
 * `head` marks the next id to read, and the job itself lives at its own key.
 * Both counters are moved server-side, so no two callers can ever draw the same
 * id or claim the same job.
 *
 * THE RULE: a reading cursor moves only by compare-and-swap.
 *
 * It is tempting to draw from head with `incr` the way tail allocates - one op,
 * perfectly distributed. It is also wrong. Two workers polling an EMPTY queue
 * both draw, so head runs past tail, and every id they burned is a slot a later
 * push will occupy and no pop will ever read. So head goes from h to h+1 only if
 * it is still h. A losing swap means another worker moved it first, which is not
 * an error - it is the next attempt.
 *
 * A counter that was never written and one holding an explicit zero are the
 * same position. Both are accepted as "at zero" everywhere: a cursor that could
 * only move from "absent" was wedged for good the first time anything stored a
 * zero into it, which is what clearing an empty queue did.
 */
final class Cursor
{
    public function __construct(
        private readonly KvClient $client,
        private readonly string $key,
    ) {}

    /**
     * Where the cursor stands. A counter that was never written reads as 0,
     * which is the correct genesis value for every end.
     */
    public function value(): int
    {
        return self::decode($this->client->get($this->key));
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
        $next = pack('J', $from + 1);

        if ($from !== 0) {
            return $this->client->cas($this->key, $next, pack('J', $from));
        }

        // Zero has two spellings on the wire: absent, and eight zero bytes.
        return $this->client->cas($this->key, $next, null)
            || $this->client->cas($this->key, $next, pack('J', 0));
    }

    /**
     * Move forward to $target if the cursor is behind it; never move it back.
     *
     * For writers that jump rather than step - clearing a queue, sweeping a
     * run of delayed seconds. An unconditional write here could drag a cursor
     * backwards over ground a concurrent worker had already covered.
     */
    public function advanceTo(int $target): void
    {
        while (true) {
            $raw = $this->client->get($this->key);

            if (self::decode($raw) >= $target) {
                return;
            }

            if ($this->client->cas($this->key, pack('J', $target), $raw)) {
                return;
            }
        }
    }

    public function key(): string
    {
        return $this->key;
    }

    private static function decode(?string $raw): int
    {
        if ($raw === null || strlen($raw) !== 8) {
            return 0;
        }

        /** @var array{1: int} $unpacked */
        $unpacked = unpack('J', $raw);

        return $unpacked[1];
    }
}
