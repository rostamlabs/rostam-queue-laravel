<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Carbon;
use Rostam\Contracts\KvClient;
use Rostam\Queue\Exceptions\JobVanished;

/**
 * A Laravel queue on Rostam's key-value engine.
 *
 * There is no list and no sorted set to build on, so the queue is a dense id
 * space governed by two server-side counters - see {@see Cursor} for why one of
 * them may only move by compare-and-swap. A job lives at its own key, and a
 * worker claims it by taking a LEASE beside it ({@see Reservation}) rather than
 * by taking the job itself.
 *
 * DELIVERY. At-least-once, which is what the lease buys. The payload stays put
 * for as long as a worker holds it, so a worker that dies does not take the
 * work with it - only the lease lapses, and the next pop finds a payload with
 * no lease and hands it back. The other half of that trade is real and is not
 * hidden here: a worker merely SLOW enough to outlive its lease can have its
 * job handed to somebody else, so handlers must be idempotent. Claiming
 * destructively would have removed that risk by losing the work instead, which
 * is the worse failure and not one a queue may choose quietly.
 *
 * Three states, read from two keys with no scan at all: payload with a lease is
 * being worked on, payload without one was abandoned, no payload means
 * finished. The middle row is what every other driver needs a reservation index
 * for - a sorted set in Redis, a column in the database - and finding expired
 * reservations normally needs a scan this engine does not have. It does not
 * need one: the ids are dense, so walking them IS the index.
 *
 * EVICTION. Rostam's default AtCapPolicy is PolicyRingbufEvict: at capacity it
 * overwrites the oldest entries. A queued job is written once and read once,
 * which is the aging profile eviction reaches first - it would eat jobs. The
 * policy is not visible to a client (no op reports it), so this driver cannot
 * detect it; it requires the operator to declare it instead, and refuses to
 * start otherwise. What it CAN do is notice the damage: ids are dense, so a
 * hole below the tail is observable, and {@see JobVanished} says so out loud
 * rather than letting a job disappear quietly.
 */
class RostamQueue extends Queue implements QueueContract
{
    /** @var array<string, int> */
    protected array $holes = [];

    /** @var array<string, int> */
    protected array $redelivered = [];

    /** Identifies this worker on the leases it takes. */
    protected string $owner;

    /**
     * @param  string  $default  the queue used when a caller names none
     * @param  int  $retryAfter  seconds a worker may hold a job before it is
     *                           handed to someone else; must exceed the longest
     *                           job or the same work runs twice
     * @param  int  $sweepSeconds  how many seconds of delayed buckets one pop
     *                             may migrate; bounds the work a long-idle
     *                             queue does when it wakes up
     * @param  int  $reclaimBatch  how many ids one pop may examine looking for
     *                             abandoned work
     */
    public function __construct(
        protected KvClient $client,
        protected string $prefix = 'queues:',
        protected string $default = 'default',
        protected int $retryAfter = 90,
        protected int $sweepSeconds = 60,
        protected int $reclaimBatch = 32,
        ?string $owner = null,
    ) {
        $this->owner = $owner ?? bin2hex(random_bytes(12));
    }

    /**
     * How many jobs are waiting, delayed ones included.
     */
    public function size($queue = null): int
    {
        $queue = $this->queueName($queue);

        return max(0, $this->tail($queue)->value() - $this->head($queue)->value())
            + $this->delayedCount($queue);
    }

    /**
     * How many jobs are ready to run right now.
     */
    public function pendingSize($queue = null): int
    {
        $queue = $this->queueName($queue);

        return max(0, $this->tail($queue)->value() - $this->head($queue)->value());
    }

    /**
     * How many are waiting for their second to come round.
     */
    public function delayedSize($queue = null): int
    {
        return $this->delayedCount($this->queueName($queue));
    }

    /**
     * Always zero, and that is a statement rather than a stub.
     *
     * A reserved job is one a worker holds but has not finished, and every
     * driver that reports them keeps an index it can sweep: Redis a sorted set,
     * the database a column. Rostam's key-value engine has no scan and no
     * ordered structure, so there is nowhere to keep such an index and nothing
     * to sweep it with - claiming a job removes it, and no third party can see
     * that it is in flight.
     *
     * Reporting a number this driver cannot know would be worse than reporting
     * none: `queue:monitor` would show a reassuring zero either way, but only
     * one of them is honest about why.
     */
    public function reservedSize($queue = null): int
    {
        return 0;
    }

    /**
     * When the oldest waiting job was created, or null if none is waiting.
     *
     * Read with `get` rather than `getdel`: this is a question, not a claim,
     * and a monitor asking it must not take work off the queue.
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        $queue = $this->queueName($queue);
        $at = $this->head($queue)->value();

        if ($at >= $this->tail($queue)->value()) {
            return null;
        }

        $payload = $this->client->get($this->jobKey($queue, $at + 1));

        if ($payload === null) {
            return null;
        }

        $decoded = json_decode($payload, true);

        return is_array($decoded) ? ($decoded['createdAt'] ?? null) : null;
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->queueName($queue), $data),
            $queue,
            null,
            fn ($payload, $queue) => $this->pushRaw($payload, $queue),
        );
    }

    /**
     * @param  array<string, mixed>  $options
     */
    public function pushRaw($payload, $queue = null, array $options = []): string
    {
        $queue = $this->queueName($queue);
        $id = $this->tail($queue)->next();

        // No TTL, ever. A job that expires is a job that vanishes, and the
        // caller was promised it would run.
        $this->client->put($this->jobKey($queue, $id), $payload);

        return (string) $id;
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->queueName($queue), $data),
            $queue,
            $delay,
            fn ($payload, $queue, $delay) => $this->laterRaw($delay, $payload, $queue),
        );
    }

    /**
     * A delayed job waits in the bucket for the second it comes due.
     *
     * Bucketing by due-second is what replaces the sorted set this engine does
     * not have: migration reads the buckets between where it last swept and
     * now, which is a handful of keys in steady state, instead of scanning for
     * jobs whose time has come.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     */
    public function laterRaw($delay, string $payload, $queue = null): string
    {
        $queue = $this->queueName($queue);
        $due = $this->availableAt($delay);

        $id = $this->client->increment($this->bucketTailKey($queue, $due));
        $this->client->put($this->bucketJobKey($queue, $due, $id), $payload);

        return (string) $id;
    }

    /**
     * Take the next job, or null when there is nothing to take.
     *
     * The claim is a LEASE, not a removal: the payload stays where it is for as
     * long as a worker holds it, so a worker that dies does not take the job
     * with it. See {@see Reservation}.
     */
    public function pop($queue = null): ?RostamJob
    {
        $queue = $this->queueName($queue);

        $this->migrateDueJobs($queue);
        $this->redeliverAbandonedJobs($queue);

        $head = $this->head($queue);
        $tail = $this->tail($queue);

        for ($attempt = 0; $attempt < 64; $attempt++) {
            $at = $head->value();

            if ($at >= $tail->value()) {
                return null;
            }

            $id = $at + 1;
            $payload = $this->client->get($this->jobKey($queue, $id));

            if ($payload === null) {
                // Nothing here. Either a push is still in flight for this slot
                // - it was allocated before its payload landed - or the engine
                // evicted it. Step over it; the counter below tells them apart
                // over time, since one push in flight is not a run of holes.
                $head->advanceFrom($at);
                $this->holes[$queue] = ($this->holes[$queue] ?? 0) + 1;

                continue;
            }

            $reservation = $this->reservation($queue, $id);

            // The cursor moves whether or not this worker won the lease: the
            // slot has been dealt with either way, and the loser must not sit
            // on it. Losing the compare-and-swap simply means somebody else
            // moved it first.
            if (! $reservation->take($this->retryAfter)) {
                $head->advanceFrom($at);

                continue;
            }

            $head->advanceFrom($at);

            return new RostamJob(
                $this->container, $this, $payload, $this->connectionName, $queue, $id, $reservation,
            );
        }

        throw JobVanished::tooManyHoles($queue, $this->holes[$queue] ?? 0);
    }

    /**
     * Hand back any job whose worker died holding it.
     *
     * This is the half that makes the queue safe to rely on. A lease is a key
     * with a TTL, so the engine removes it on its own; a job whose payload is
     * still present with no lease over it is work that was accepted, started,
     * and abandoned. It goes back on the queue.
     *
     * The walk needs no scan because the ids are dense: everything below the
     * reader is either finished (payload gone) or accounted for. The cursor
     * stops at the first job still legitimately in flight and resumes from
     * there, so the pass is short and never revisits settled ground.
     */
    protected function redeliverAbandonedJobs(string $queue): void
    {
        $cursor = new Cursor($this->client, $this->key($queue, 'reclaim'));
        $upTo = $this->head($queue)->value();

        for ($seen = 0; $seen < $this->reclaimBatch; $seen++) {
            $at = $cursor->value();

            if ($at >= $upTo) {
                return;
            }

            $id = $at + 1;
            $payload = $this->client->get($this->jobKey($queue, $id));

            if ($payload === null) {
                $cursor->advanceFrom($at);          // finished, or never arrived

                continue;
            }

            if ($this->reservation($queue, $id)->held()) {
                return;                              // still being worked on
            }

            // Abandoned. Put it back at the end rather than in place: the ids
            // behind the reader are spent, and a retry is a new attempt rather
            // than a resumption.
            $this->pushRaw($payload, $queue);
            $this->client->del($this->jobKey($queue, $id));
            $cursor->advanceFrom($at);

            $this->redelivered[$queue] = ($this->redelivered[$queue] ?? 0) + 1;
        }
    }

    /**
     * The lease key for one job. The owner token is per worker process, so a
     * lease can only be released or extended by the worker that took it.
     */
    public function reservation(string $queue, int $id): Reservation
    {
        return new Reservation($this->client, $this->prefix.$queue.':lease:'.$id, $this->owner);
    }

    /**
     * Mark a job finished: the payload goes, and with it the last trace that it
     * was ever in flight.
     */
    public function complete(string $queue, int $id, Reservation $reservation): void
    {
        $this->client->del($this->jobKey($queue, $id));
        $reservation->release();
    }

    /**
     * How many abandoned jobs this instance has handed back.
     */
    public function redeliveredCount(?string $queue = null): int
    {
        return $this->redelivered[$this->queueName($queue)] ?? 0;
    }

    /**
     * Delete every job on a queue, waiting and delayed. Returns how many went.
     */
    public function clear(?string $queue = null): int
    {
        $queue = $this->queueName($queue);
        $head = $this->head($queue);
        $tail = $this->tail($queue);
        $at = $tail->value();

        $keys = [];
        for ($id = $head->value() + 1; $id <= $at; $id++) {
            $keys[] = $this->jobKey($queue, $id);
        }

        $removed = $keys === [] ? 0 : count(array_filter($this->client->delMany($keys)));

        // Move the reader to the writer rather than resetting both: the ids
        // already handed out stay spent, so a push still in flight cannot land
        // in a slot the reader has passed.
        $this->client->put($head->key(), pack('J', $at));

        return $removed;
    }

    /**
     * Move any delayed jobs whose second has arrived onto the ready queue.
     */
    protected function migrateDueJobs(string $queue): void
    {
        $now = (int) Carbon::now()->getTimestamp();
        $sweptKey = $this->key($queue, 'swept');
        $raw = $this->client->get($sweptKey);
        $swept = ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : $now - 1;

        // A queue that has been idle for a day would otherwise sweep a day of
        // buckets in one pop. Catching up over several pops keeps any single
        // one bounded.
        $until = min($now, $swept + $this->sweepSeconds);

        for ($second = $swept + 1; $second <= $until; $second++) {
            $count = $this->bucketSize($queue, $second);

            for ($id = 1; $id <= $count; $id++) {
                $payload = $this->client->getdel($this->bucketJobKey($queue, $second, $id));

                if ($payload !== null) {
                    $this->pushRaw($payload, $queue);
                }
            }
        }

        if ($until > $swept) {
            $this->client->put($sweptKey, pack('J', $until));
        }
    }

    protected function bucketSize(string $queue, int $second): int
    {
        $raw = $this->client->get($this->bucketTailKey($queue, $second));

        return ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;
    }

    /**
     * How many delayed jobs are still waiting in buckets that have not been
     * swept. Only the window a pop would look at, for the same reason.
     */
    protected function delayedCount(string $queue): int
    {
        $now = (int) Carbon::now()->getTimestamp();
        $raw = $this->client->get($this->key($queue, 'swept'));
        $swept = ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : $now - 1;

        $total = 0;
        for ($second = $swept + 1; $second <= $now + $this->sweepSeconds; $second++) {
            $total += $this->bucketSize($queue, $second);
        }

        return $total;
    }

    public function getClient(): KvClient
    {
        return $this->client;
    }

    /**
     * How many empty slots this instance has stepped over, per queue. A
     * non-zero count on a healthy queue means jobs were evicted.
     */
    public function holesSeen(?string $queue = null): int
    {
        return $this->holes[$this->queueName($queue)] ?? 0;
    }

    protected function head(string $queue): Cursor
    {
        return new Cursor($this->client, $this->key($queue, 'head'));
    }

    protected function tail(string $queue): Cursor
    {
        return new Cursor($this->client, $this->key($queue, 'tail'));
    }

    protected function queueName($queue): string
    {
        return (string) ($queue ?: $this->default);
    }

    protected function key(string $queue, string $part): string
    {
        return $this->prefix.$queue.':'.$part;
    }

    protected function jobKey(string $queue, int $id): string
    {
        return $this->prefix.$queue.':job:'.$id;
    }

    protected function bucketTailKey(string $queue, int $second): string
    {
        return $this->prefix.$queue.':d:'.$second.':tail';
    }

    protected function bucketJobKey(string $queue, int $second, int $id): string
    {
        return $this->prefix.$queue.':d:'.$second.':job:'.$id;
    }
}
