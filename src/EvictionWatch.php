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
 * A queue on Rostam is only as durable as the node never evicting a record it
 * still holds, and whether it will is not something the wire reports. What
 * rostam v0.7.0-beta3 does report is how many LIVE records it has displaced,
 * and on a node that keeps queued jobs that number has exactly one acceptable
 * value: zero.
 *
 * So it can catch a node that is losing records, never prove one will not.
 * Zero before the node was ever under pressure says nothing about the day it
 * is. What it does guarantee is that a worker does not go on taking jobs from
 * a node that has already thrown some away.
 *
 * A refusal for evictions is final for this process. Laravel's worker catches
 * the exception and pops again a moment later; a check that refused once and
 * then let the next call through - because the interval had not passed yet -
 * was a guard that held for exactly one operation. The count cannot go back
 * down without a server restart, which does not bring back what was lost, so
 * a refused worker stays refused until it is restarted too.
 *
 * The count is node-wide and cumulative since the server started. Evictions of
 * anyone's keys on that node count, not only jobs: a node that evicted anything
 * live can evict a job.
 */
final class EvictionWatch
{
    private ?int $checkedAt = null;

    /** Set once live evictions have been seen, and thrown from then on. */
    private ?UnsafeQueueStore $refusal = null;

    /**
     * @param  bool  $required  refuse a server that cannot report the count,
     *                          rather than run without it
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
        if ($this->refusal !== null) {
            throw $this->refusal;
        }

        $evicted = $this->evictionsLive();

        if ($evicted === null) {
            if ($this->required) {
                // Not remembered as an answer: the next operation asks again,
                // so a server that failed to answer once is not refused for
                // good, and one that never answers is refused every time.
                $this->checkedAt = null;

                throw UnsafeQueueStore::cannotVerify();
            }

            $this->checkedAt = (int) Carbon::now()->getTimestamp();

            return;
        }

        $this->checkedAt = (int) Carbon::now()->getTimestamp();

        if ($evicted > 0) {
            throw $this->refusal = UnsafeQueueStore::liveRecordsEvicted($evicted);
        }
    }

    /**
     * Re-read the count if the interval has passed. Cheap when it has not -
     * unless a refusal stands, which is thrown every time.
     *
     * @throws UnsafeQueueStore
     */
    public function check(): void
    {
        if ($this->refusal !== null) {
            throw $this->refusal;
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
