<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Illuminate\Support\Carbon;
use Rostam\Contracts\KvClient;
use Rostam\Contracts\ReportsKvMetrics;
use Rostam\Exceptions\ServerException;
use Rostam\Kv\Protocol\Status;
use Rostam\Queue\Exceptions\UnsafeQueueStore;

/**
 * Catches a server that has started losing live records.
 *
 * A queue on Rostam is only as durable as the node never evicting, and whether
 * it evicts is not something the wire reports - a single-node `rostam-server`
 * always evicts at capacity, silently, with every write still answering
 * success. What rostam v0.7.0-beta3 does report is how many LIVE records it has
 * displaced, and on a node that keeps queued jobs that number has exactly one
 * acceptable value: zero.
 *
 * So it can catch a false declaration, never prove a true one. Zero before the
 * node ever filled up says nothing about what happens when it does. What it
 * does guarantee is that a worker does not go on taking jobs from a node that
 * has already thrown some away.
 *
 * The count is node-wide and cumulative since the server started. Evictions of
 * anyone's keys on that node count, not only jobs - on a node that was declared
 * never to evict, any eviction contradicts the declaration.
 */
final class EvictionWatch
{
    private ?int $checkedAt = null;

    /** Whether this server can report the count at all; null until asked. */
    private ?bool $supported = null;

    /**
     * @param  bool  $required  refuse a server that cannot report the count,
     *                          rather than run on the declaration alone
     * @param  int  $everySeconds  how often a running queue re-reads it
     */
    public function __construct(
        private readonly KvClient $client,
        private readonly bool $required,
        private readonly int $everySeconds = 60,
    ) {}

    /**
     * Read the count now, whatever the interval says.
     *
     * @throws UnsafeQueueStore when live records have been evicted, or when
     *                          the count is required and cannot be read
     */
    public function verify(): void
    {
        $this->checkedAt = (int) Carbon::now()->getTimestamp();

        $evicted = $this->evictionsLive();

        if ($evicted === null) {
            $this->supported = false;

            if ($this->required) {
                throw UnsafeQueueStore::cannotVerify();
            }

            return;
        }

        $this->supported = true;

        if ($evicted > 0) {
            throw UnsafeQueueStore::liveRecordsEvicted($evicted);
        }
    }

    /**
     * Re-read the count if the interval has passed. Cheap when it has not.
     *
     * @throws UnsafeQueueStore
     */
    public function check(): void
    {
        if ($this->supported === false) {
            return;
        }

        $now = (int) Carbon::now()->getTimestamp();

        if ($this->checkedAt !== null && $now - $this->checkedAt < $this->everySeconds) {
            return;
        }

        $this->verify();
    }

    /**
     * The count, or null when this server or client cannot report it.
     *
     * "Cannot report" is the generic error, which is what rostam answers for
     * an op it does not know. Every other failure - a refused token, a dead
     * connection - is not an answer about evictions, and is thrown.
     */
    private function evictionsLive(): ?int
    {
        if (! $this->client instanceof ReportsKvMetrics) {
            return null;
        }

        try {
            return $this->client->kvMetrics()->evictionsLive();
        } catch (ServerException $exception) {
            if ($exception->status === Status::ERROR) {
                return null;
            }

            throw $exception;
        }
    }
}
