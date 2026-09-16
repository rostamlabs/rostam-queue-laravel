<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Carbon;
use Rostam\Contracts\KvClient;
use Rostam\Queue\Exceptions\JobVanished;

use function Illuminate\Support\enum_value;

/**
 * A Laravel queue on Rostam's key-value engine.
 *
 * There is no list and no sorted set to build on, so the queue is a dense id
 * space governed by server-side counters - see {@see Cursor} for why the reading
 * ones only move by compare-and-swap. A job lives at its own key, and a worker
 * claims it by taking a LEASE beside it ({@see Reservation}) rather than by
 * taking the job itself.
 *
 * DELIVERY. At-least-once. A job the queue accepted is delivered, including when
 * the worker holding it dies. A worker merely SLOW enough to outlive its lease
 * can have its job handed to somebody else, so handlers must be idempotent.
 *
 * WHERE JOBS USED TO BE LOST, AND WHY THEY ARE NOT. Every loss this driver has
 * had lived in the gap between two round trips, and every one is closed by
 * making the deciding step a single atomic op rather than a check followed by an
 * action - a check always leaves a window between reading and acting.
 *
 * - A push draws its id, then writes its payload. A worker reaching that slot
 *   in between used to step over it, and the payload landed where nothing would
 *   read. Now the worker kills the slot with `set_nx` - a TOMBSTONE - and the
 *   push writes with `set_nx` too, so exactly one of them owns the slot. If the
 *   worker won, the push simply draws another id. clear() kills the slots it
 *   passes the same way, so a push straddling a clear cannot land behind it.
 * - A delayed job used to be read destructively on its way to the ready queue,
 *   so a worker dying in between took the only copy. Now it is copied first
 *   and deleted after, under a per-item lease, and a duplicate is the worst a
 *   crash can cause.
 * - A delayed push could land in a second the sweep was passing, or had
 *   passed. The sweep SEALS a second's id counter before it counts it, so a
 *   push that draws an id meanwhile knows it was too late; and a push that
 *   stored its job checks afterwards whether the sweep has passed its second,
 *   and moves the job itself if so.
 *
 * EVICTION. A queue is only as durable as the node never throwing records away.
 * {@see EvictionWatch} refuses a node that has evicted live records; what this
 * class can see on its own is the damage - a record that vanished after it was
 * written - and {@see JobVanished} reports a run of it.
 */
class RostamQueue extends Queue implements ClearableQueue, QueueContract
{
    /**
     * What a killed slot holds. No Laravel payload - a JSON object - can equal
     * it.
     */
    public const TOMBSTONE = 'rostam-queue:tombstone:v1';

    /**
     * Set on a delayed second's id counter once the sweep has counted it.
     * Increments keep it, so every id drawn afterwards carries it too.
     */
    public const SEALED = 1 << 62;

    /** Prefixes a stored delayed job, ahead of the generation it was written under. */
    private const DELAYED_MAGIC = 'RQD1';

    /**
     * How many empty slots one pop may kill before it says jobs are vanishing.
     * A push caught mid-write leaves one; a run this long is records gone.
     */
    private const HOLE_RUN = 64;

    /**
     * How many slots one pop looks at before giving up for now. Losing a
     * lease race is not a hole - another worker got the job - so it counts
     * here, and running out means come back later, not that anything is lost.
     */
    private const MAX_STEPS = 1024;

    /**
     * How many slots one clear reads and kills per round trip. A queue cleared
     * with a million ids outstanding is two round trips per this many, not two
     * per id.
     */
    private const CLEAR_BATCH = 512;

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
     * @param  int  $reclaimBatch  how many ids one pop examines, above the
     *                             oldest job still in flight, for work whose
     *                             worker died
     * @param  int  $tombstoneTtl  how long a killed slot stays killed; a push
     *                             paused longer than this between drawing its id
     *                             and writing its payload is the one delay the
     *                             queue does not cover
     */
    public function __construct(
        protected KvClient $client,
        protected string $prefix = 'queues:',
        protected string $default = 'default',
        protected int $retryAfter = 90,
        protected int $sweepSeconds = 60,
        protected int $reclaimBatch = 32,
        ?string $owner = null,
        protected int $tombstoneTtl = 604800,
        protected ?EvictionWatch $watch = null,
        ?bool $dispatchAfterCommit = null,
    ) {
        $this->owner = $owner ?? bin2hex(random_bytes(12));
        $this->dispatchAfterCommit = $dispatchAfterCommit;
    }

    /**
     * How many jobs are waiting, delayed ones included.
     */
    public function size($queue = null): int
    {
        return $this->pendingSize($queue) + $this->delayedSize($queue);
    }

    /**
     * How many jobs are ready to run right now.
     *
     * An upper bound: the ids between the reader and the writer, which include
     * slots killed while their push re-routed and not yet passed. Jobs being
     * worked on are not counted - the reader has passed them.
     */
    public function pendingSize($queue = null): int
    {
        $queue = $this->queueName($queue);

        return max(0, $this->tail($queue)->value() - $this->head($queue)->value());
    }

    /**
     * How many delayed jobs are waiting, however far in the future.
     *
     * A gauge per delayed generation, kept beside the buckets, since the
     * buckets themselves cannot be enumerated. A worker killed between storing
     * a delayed job and counting it leaves it off by one.
     */
    public function delayedSize($queue = null): int
    {
        $queue = $this->queueName($queue);
        $raw = $this->client->get($this->gaugeKey($queue, $this->delayedGeneration($queue)));

        return ($raw !== null && strlen($raw) === 8) ? max(0, unpack('J', $raw)[1]) : 0;
    }

    /**
     * Always zero, and that is a statement rather than a stub.
     *
     * A reserved job is one a worker holds but has not finished, and every
     * driver that reports them keeps an index it can count: Redis a sorted set,
     * the database a column. The only record here is a lease key per job, and
     * nothing on this engine can count keys by pattern.
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
     * Read with `get`: this is a question, not a claim, and a monitor asking
     * it must not take work off the queue.
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        $queue = $this->queueName($queue);
        $at = $this->head($queue)->value();

        if ($at >= $this->tail($queue)->value()) {
            return null;
        }

        $payload = $this->client->get($this->jobKey($queue, $at + 1));

        if ($payload === null || $payload === self::TOMBSTONE) {
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
     * Put a job on the ready queue.
     *
     * The id is drawn, then the slot is claimed with `set_nx`. Losing that claim
     * means a worker reached the slot first, found it empty and killed it; the
     * job is then written under a fresh id, and nothing was lost in between.
     *
     * @param  array<string, mixed>  $options
     */
    public function pushRaw($payload, $queue = null, array $options = []): string
    {
        $this->watch?->check();

        return (string) $this->enqueue($this->queueName($queue), (string) $payload);
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
     * not have. Three things send a job straight to the ready queue instead:
     * a second the sweep has already passed (a job due in the past, or a clock
     * behind the worker's), an id drawn after the sweep sealed that second, and
     * a slot the sweep reached and killed before the payload landed. And once
     * the job is stored, the push looks at the sweep once more: if it has passed
     * this second in the meantime, the push moves the job itself.
     *
     * The id it returns is the delayed bucket's, not the ready queue's - a
     * different id space from pushRaw()'s, and meaningful only inside the
     * second the job is due. Except when the job goes to the ready queue after
     * all (every branch below), where it is a ready-queue id. Laravel ignores
     * it either way.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     */
    public function laterRaw($delay, string $payload, $queue = null): string
    {
        $this->watch?->check();

        $queue = $this->queueName($queue);
        $due = $this->availableAt($delay);

        if ($due <= $this->swept($queue)) {
            return (string) $this->enqueue($queue, $payload);
        }

        $id = $this->client->increment($this->bucketTailKey($queue, $due));

        if (($id & self::SEALED) !== 0) {
            return (string) $this->enqueue($queue, $payload);
        }

        $generation = $this->delayedGeneration($queue);

        if (! $this->client->setNx($this->bucketJobKey($queue, $due, $id), self::DELAYED_MAGIC.pack('J', $generation).$payload)) {
            // The sweep reached this slot before the payload did and killed it.
            // It has therefore passed this second, so the job is due: run it.
            return (string) $this->enqueue($queue, $payload);
        }

        $this->client->increment($this->gaugeKey($queue, $generation));

        // The sweep may have passed this second while the job was on its way -
        // counted the bucket before this id existed, and finished, or finished
        // so long ago that its seal is gone. Either way no sweep will come back
        // for it, so this push moves it itself.
        //
        // Losing the move lease here is not somebody else finishing the job:
        // the lease may be a dead worker's, held for the rest of retry_after,
        // and no sweep will return to this second. So the job goes to the ready
        // queue and the bucket copy is dropped. If the lease holder is alive and
        // moves it too, that is a duplicate - which at-least-once allows, and
        // losing the job is not.
        if ($due <= $this->swept($queue)) {
            $moved = $this->moveDelayed($queue, $due, $id);

            // Drawing that id re-created the second's counter, and no sweep will
            // pass this second again to delete it. Left alone it is a key with
            // no TTL and nothing to remove it. A producer drawing from it in the
            // meantime gets a fresh id, stores its job, and lands here too.
            $this->client->del($this->bucketTailKey($queue, $due));

            if (! $moved) {
                $placed = $this->enqueue($queue, $payload);
                $this->client->del($this->bucketJobKey($queue, $due, $id));
                $this->client->increment($this->gaugeKey($queue, $generation), -1, $this->tombstoneTtl);

                return (string) $placed;
            }
        }

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
        $this->watch?->check();

        $name = $this->unrouted($queue);
        $queue = $this->queueName($queue);

        $this->migrateDueJobs($queue);
        $this->redeliverAbandonedJobs($queue);

        $head = $this->head($queue);
        $tail = $this->tail($queue);
        $killed = 0;

        for ($step = 0; $step < self::MAX_STEPS; $step++) {
            $at = $head->value();

            if ($at >= $tail->value()) {
                return null;
            }

            $id = $at + 1;
            $key = $this->jobKey($queue, $id);
            $payload = $this->client->get($key);

            if ($payload === null) {
                // Either a push still on its way, or a record that vanished.
                // Kill the slot atomically: if this wins, the push - if there
                // is one - finds it taken and draws another id; if it loses,
                // the payload has just landed and is read again.
                //
                // Counted as a hole only when this worker is also the one that
                // moves past it. A worker reading a stale head can reach a slot
                // another worker already claimed and finished; killing that
                // empty slot is harmless, but it was never a hole.
                if ($this->client->setNx($key, self::TOMBSTONE, $this->tombstoneTtl) && $head->advanceFrom($at)) {
                    $this->holes[$queue] = ($this->holes[$queue] ?? 0) + 1;

                    if (++$killed >= self::HOLE_RUN) {
                        throw JobVanished::tooManyHoles($queue, $killed);
                    }
                }

                continue;
            }

            if ($payload === self::TOMBSTONE) {
                $head->advanceFrom($at);

                continue;
            }

            $reservation = $this->reservation($queue, $id);

            // The cursor moves whether or not this worker won the lease: the
            // slot has been dealt with either way, and the loser must not sit
            // on it.
            if (! $reservation->take($this->retryAfter)) {
                $head->advanceFrom($at);

                continue;
            }

            // Between reading the payload and taking the lease, another worker
            // may have claimed this job, finished it and let its lease go.
            // Holding a lease on a job that is already done would run it twice.
            $current = $this->client->get($key);

            if ($current === null || $current === self::TOMBSTONE) {
                $reservation->release();
                $head->advanceFrom($at);

                continue;
            }

            $head->advanceFrom($at);

            return new RostamJob(
                $this->container, $this, $current, $this->connectionName, $name, $id, $reservation,
            );
        }

        // Every step lost a race to another worker. That worker has the job;
        // this one comes back on its next poll.
        return null;
    }

    /**
     * Hand back any job whose worker died holding it.
     *
     * A lease is a key with a TTL, so the engine removes it on its own; a job
     * whose payload is still present with no lease over it is work that was
     * accepted, started, and abandoned. It goes back on the queue.
     *
     * The walk needs no scan because the ids are dense: every slot below the
     * reader was either claimed or killed, so an empty one there is a finished
     * job. Each pop reads a window of `reclaimBatch` ids in two round trips -
     * payloads, then leases - and looks past a job still in flight, so one
     * long-running job does not hold back the abandoned ones above it WITHIN
     * that window. The cursor itself only moves across slots that are settled,
     * so a job still running does hold the window in place: anything above
     * `reclaim + reclaimBatch` waits for it to finish. Nothing is lost by
     * waiting - the payload and its lapsed lease stay exactly as they are.
     *
     * Putting a job back is claimed first, with the job's own lease, so two
     * workers passing the same abandoned job do not both requeue it. The copy
     * is written before the original is removed, so a worker dying in between
     * leaves two - never none.
     */
    protected function redeliverAbandonedJobs(string $queue): void
    {
        $cursor = new Cursor($this->client, $this->key($queue, 'reclaim'));
        $start = $cursor->value();
        $end = min($start + $this->reclaimBatch, $this->head($queue)->value());

        if ($start >= $end) {
            return;
        }

        $ids = range($start + 1, $end);
        $jobKeys = array_map(fn (int $id) => $this->jobKey($queue, $id), $ids);
        $leaseKeys = array_map(fn (int $id) => $this->leaseKey($queue, $id), $ids);

        $payloads = $this->client->getMany($jobKeys);
        $leases = $this->client->getMany($leaseKeys);
        $settledSoFar = true;

        foreach ($ids as $index => $id) {
            $payload = $payloads[$jobKeys[$index]] ?? null;
            $settled = $payload === null || $payload === self::TOMBSTONE;

            if (! $settled && ($leases[$leaseKeys[$index]] ?? null) === null) {
                $settled = $this->redeliver($queue, $id);
            }

            if (! $settled) {
                // Still worked on, or being put back by somebody else. Look
                // past it, but the cursor stays behind it.
                $settledSoFar = false;

                continue;
            }

            if ($settledSoFar) {
                $cursor->advanceFrom($id - 1);
            }
        }
    }

    /**
     * Put one abandoned job back. True once the slot is settled - put back, or
     * found finished after all; false when another worker holds its lease.
     */
    protected function redeliver(string $queue, int $id): bool
    {
        $key = $this->jobKey($queue, $id);
        $lease = $this->reservation($queue, $id);

        if (! $lease->take($this->retryAfter)) {
            return false;
        }

        // Read again under the lease: its original worker may have finished it
        // after all, just late, between the look and the lease.
        $payload = $this->client->get($key);

        if ($payload !== null && $payload !== self::TOMBSTONE) {
            // A worker died holding this job. That is an attempt, or a job
            // that kills its worker would be retried forever and never reach
            // maxTries.
            $this->enqueue($queue, self::withAnotherAttempt($payload));
            $this->client->del($key);

            $this->redelivered[$queue] = ($this->redelivered[$queue] ?? 0) + 1;
        }

        $lease->release();

        return true;
    }

    /**
     * The lease key for one job. The owner token is per worker process, so a
     * lease can only be released or extended by the worker that took it.
     */
    public function reservation(string $queue, int $id): Reservation
    {
        return new Reservation($this->client, $this->leaseKey($queue, $id), $this->owner);
    }

    /**
     * Mark a job finished: the payload goes, and with it the last trace that it
     * was ever in flight.
     */
    public function complete(string $queue, int $id, Reservation $reservation): void
    {
        $this->client->del($this->jobKey($this->queueName($queue), $id));
        $reservation->release();
    }

    /**
     * How many abandoned jobs this instance has handed back.
     */
    public function redeliveredCount($queue = null): int
    {
        return $this->redelivered[$this->queueName($queue)] ?? 0;
    }

    /**
     * Delete every job on a queue, waiting and delayed. Returns how many went.
     *
     * Waiting jobs are overwritten with tombstones rather than deleted, from the
     * redelivery cursor up to the writer: a push that has drawn one of those ids
     * and not yet written finds its slot killed and draws a fresh one past the
     * clear, where it is delivered. Deleting instead left such a push free to
     * land behind the reader, delivered by nobody and cleared by nobody.
     *
     * Unlike pop()'s `set_nx`, this overwrites - it is the one place the driver
     * destroys a payload, which is what clearing a queue is.
     *
     * Delayed jobs cannot be found - their buckets cannot be enumerated - so
     * they are orphaned instead: the delayed generation moves on, and a job
     * stored under an older one is deleted rather than delivered when the sweep
     * reaches its second.
     *
     * A job enqueued while the clear is running may land on either side of it -
     * including a copy that redelivery is writing back for a dead worker at that
     * moment, which lands above the writer this clear read and survives it. The
     * count is what the clear READ: a payload that lands between the read and
     * the kill is destroyed with the rest and not counted.
     *
     * A job a worker is running when the queue is cleared finishes normally: it
     * holds the payload already, and the tombstone only stops the slot being
     * handed out again - so it is counted as removed and then completes anyway.
     * A job whose worker died is cleared like any other.
     *
     * @param  \UnitEnum|string|null  $queue
     */
    public function clear($queue = null): int
    {
        $queue = $this->queueName($queue);
        $at = $this->tail($queue)->value();
        $reclaim = new Cursor($this->client, $this->key($queue, 'reclaim'));
        $reader = $this->head($queue)->value();
        $removed = 0;

        // From the REDELIVERY cursor, not the reader: a job whose worker died is
        // below the reader with its payload still there, waiting to be handed
        // back. Clearing from the reader left those to reappear minutes later on
        // a queue that had just reported itself empty.
        for ($id = $reclaim->value() + 1; $id <= $at; $id += self::CLEAR_BATCH) {
            $keys = [];

            for ($slot = $id; $slot <= min($id + self::CLEAR_BATCH - 1, $at); $slot++) {
                $keys[$slot] = $this->jobKey($queue, $slot);
            }

            $kill = [];

            // Keyed by slot, not walked beside the answer: a client that
            // answered in another order, or dropped an entry, would otherwise
            // put a slot on the wrong side of the reader - and the wrong side
            // is a job left where a push can bury it.
            $payloads = $this->client->getMany(array_values($keys));

            foreach ($keys as $slot => $key) {
                $payload = $payloads[$key] ?? null;

                if ($payload !== null && $payload !== self::TOMBSTONE) {
                    $removed++;
                }

                // Above the reader, every slot is killed whether or not it holds
                // anything: an id drawn but not yet written is exactly the push
                // that must not land behind this clear. At or below the reader
                // no push can arrive - ids come from the writer, which is ahead
                // of the reader - so a slot that holds nothing there is a job
                // that finished, and a tombstone on it is a key invented for
                // nothing. Writing one per id since the redelivery cursor turned
                // clearing an empty queue into twenty thousand new keys.
                if ($slot > $reader || $payload !== null) {
                    $kill[] = [$key, self::TOMBSTONE, $this->tombstoneTtl];
                }
            }

            if ($kill !== []) {
                $this->client->putMany($kill);
            }
        }

        $generation = $this->delayedGeneration($queue);
        $delayed = $this->delayedSize($queue);

        // Move the reader and the redelivery cursor up to the writer, and never
        // back: a concurrent worker may already be further along.
        $this->head($queue)->advanceTo($at);
        $reclaim->advanceTo($at);

        $this->client->increment($this->key($queue, 'dgen'));
        $this->client->del($this->gaugeKey($queue, $generation));

        return $removed + $delayed;
    }

    /**
     * Move any delayed jobs whose second has arrived onto the ready queue.
     */
    protected function migrateDueJobs(string $queue): void
    {
        $now = (int) Carbon::now()->getTimestamp();
        $swept = $this->swept($queue);

        // A queue that has been idle for a day would otherwise sweep a day of
        // buckets in one pop. Catching up over several pops keeps any single
        // one bounded.
        $until = min($now, $swept + $this->sweepSeconds);

        if ($until <= $swept) {
            return;
        }

        $cursor = new Cursor($this->client, $this->key($queue, 'swept'));

        for ($second = $swept + 1; $second <= $until; $second++) {
            if (! $this->migrateSecond($queue, $second)) {
                // A job in this second belongs to another worker's move. Stop
                // here rather than mark the second done under it.
                return;
            }

            $cursor->advanceTo($second);

            // The seal was needed only until the watermark passed this second.
            // From here a push that draws an id for it - even a fresh one - sees
            // the watermark after storing and moves its own job. Deleting it
            // keeps the keyspace to the seconds still ahead; a worker dying
            // right here leaves one sealed counter behind.
            $this->client->del($this->bucketTailKey($queue, $second));
        }
    }

    /**
     * Seal one second, then move every job in it. True when nothing is left.
     */
    protected function migrateSecond(string $queue, int $second): bool
    {
        $count = $this->seal($queue, $second);
        $resolved = true;

        for ($id = 1; $id <= $count; $id++) {
            $key = $this->bucketJobKey($queue, $second, $id);
            $stored = $this->client->get($key);

            if ($stored === null) {
                // Drawn, not yet written. Kill it; the push re-routes itself.
                if ($this->client->setNx($key, self::TOMBSTONE, $this->tombstoneTtl)) {
                    continue;
                }

                $stored = $this->client->get($key);
            }

            if ($stored === null || $stored === self::TOMBSTONE) {
                continue;
            }

            if (! $this->moveDelayed($queue, $second, $id)) {
                $resolved = false;                   // another worker is moving it
            }
        }

        return $resolved;
    }

    /**
     * Move one stored delayed job to the ready queue - or, if a clear came after
     * it, delete it. True when this worker settled it; false when another holds
     * its move.
     */
    protected function moveDelayed(string $queue, int $second, int $id): bool
    {
        $move = new Reservation($this->client, $this->prefix.$queue.':mig:'.$second.':'.$id, $this->owner);

        if (! $move->take($this->retryAfter)) {
            return false;
        }

        // Read again under the lease: another worker may have moved it between
        // the look and the lease, and moving it again would run it twice.
        $key = $this->bucketJobKey($queue, $second, $id);
        $stored = $this->client->get($key);

        if ($stored !== null && $stored !== self::TOMBSTONE) {
            [$writtenUnder, $payload] = self::unwrapDelayed($stored);

            // Two guards, either of which is enough today, kept because they
            // fail differently: the generation is read HERE, under the lease
            // rather than once per sweep, and only a generation BELOW the
            // current one is deleted. A sweep that read the generation before a
            // clear and compared it for inequality would take a job delayed
            // after that clear for one written before it, and delete it.
            if ($writtenUnder !== null && $writtenUnder < $this->delayedGeneration($queue)) {
                $this->client->del($key);            // cleared before it came due
            } else {
                // Copy first, delete after: a worker dying in between leaves
                // the original for the next sweep to move again, never nothing.
                $this->enqueue($queue, $payload);
                $this->client->del($key);

                if ($writtenUnder !== null) {
                    // With a TTL, which applies only if this creates the key: a
                    // clear may have removed this generation's gauge meanwhile.
                    $this->client->increment($this->gaugeKey($queue, $writtenUnder), -1, $this->tombstoneTtl);
                }
            }
        }

        $move->release();

        return true;
    }

    /**
     * Seal a second's id counter and return how many ids it had handed out.
     *
     * After this, any id drawn for that second carries SEALED, and the push
     * that drew it sends its job to the ready queue. No TTL here: the counter
     * is the only record of how many jobs the second holds, so it goes only
     * once the watermark has passed the second - see migrateDueJobs().
     */
    protected function seal(string $queue, int $second): int
    {
        $key = $this->bucketTailKey($queue, $second);

        while (true) {
            $raw = $this->client->get($key);
            $value = ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;

            if (($value & self::SEALED) !== 0) {
                return $value & ~self::SEALED;
            }

            if ($this->client->cas($key, pack('J', $value | self::SEALED), $raw)) {
                return $value;
            }

            // A push drew an id between the read and the seal. Read again.
        }
    }

    /**
     * Write a job to the ready queue, drawing new ids until one is not a slot
     * a worker already killed.
     */
    protected function enqueue(string $queue, string $payload): int
    {
        $tail = $this->tail($queue);

        for ($attempt = 0; $attempt < 64; $attempt++) {
            $id = $tail->next();

            // No TTL, ever. A job that expires is a job that vanishes.
            if ($this->client->setNx($this->jobKey($queue, $id), $payload)) {
                return $id;
            }
        }

        throw JobVanished::cannotPlace($queue);
    }

    public function getClient(): KvClient
    {
        return $this->client;
    }

    /**
     * How many empty slots this instance has killed and moved past, per queue.
     *
     * Each is either a push that was caught between drawing its id and writing
     * its payload - it re-routed, and lost nothing - or a record that vanished
     * after it was written. A steady trickle is the first; a run is the second.
     */
    public function holesSeen($queue = null): int
    {
        return $this->holes[$this->queueName($queue)] ?? 0;
    }

    /**
     * Bump a job's attempt count, for a job whose worker died holding it.
     * A payload that is not JSON has no count to carry and goes back as it was.
     */
    private static function withAnotherAttempt(string $payload): string
    {
        $decoded = json_decode($payload, true);

        if (! is_array($decoded)) {
            return $payload;
        }

        $decoded['attempts'] = (int) ($decoded['attempts'] ?? 0) + 1;

        return (string) json_encode($decoded, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /**
     * @return array{0: int|null, 1: string} the generation it was stored under - null when
     *                                       it carries none - and the payload
     */
    private static function unwrapDelayed(string $stored): array
    {
        if (strlen($stored) < 12 || ! str_starts_with($stored, self::DELAYED_MAGIC)) {
            // Not written by this driver's laterRaw, so no generation says it
            // was cleared. Deliver it rather than guess it away.
            return [null, $stored];
        }

        /** @var array{1: int} $generation */
        $generation = unpack('J', substr($stored, 4, 8));

        return [$generation[1], substr($stored, 12)];
    }

    /**
     * The last second the sweep has finished, written down the first time
     * anything asks.
     *
     * It used to default to "a second ago" without being stored. On a fresh
     * queue that is a moving target: a job delayed before any worker ever ran
     * sat in a bucket for a second the first worker - starting later, from its
     * own "a second ago" - had already decided was in the past, and it was
     * never swept. Pinning the watermark with `set_nx` on first use means every
     * producer and worker agrees where the sweep starts.
     */
    protected function swept(string $queue): int
    {
        $key = $this->key($queue, 'swept');
        $raw = $this->client->get($key);

        if ($raw === null) {
            $this->client->setNx($key, pack('J', (int) Carbon::now()->getTimestamp() - 1));
            $raw = $this->client->get($key);
        }

        return ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;
    }

    protected function delayedGeneration(string $queue): int
    {
        $raw = $this->client->get($this->key($queue, 'dgen'));

        return ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;
    }

    protected function head(string $queue): Cursor
    {
        return new Cursor($this->client, $this->key($queue, 'head'));
    }

    protected function tail(string $queue): Cursor
    {
        return new Cursor($this->client, $this->key($queue, 'tail'));
    }

    /**
     * The queue a caller named - an enum's value, or the default - as jobs and
     * events report it.
     *
     * @param  \UnitEnum|string|null  $queue
     */
    protected function unrouted($queue): string
    {
        return (string) (enum_value($queue) ?: $this->default);
    }

    /**
     * The queue whose keys are used: the name after Laravel's queue routes
     * have forwarded it, as the Redis driver resolves it.
     *
     * @param  \UnitEnum|string|null  $queue
     */
    protected function queueName($queue): string
    {
        $name = $this->unrouted($queue);

        // Queue routes arrived during Laravel 12; older releases have none.
        return method_exists($this, 'resolveQueue') ? (string) $this->resolveQueue($name) : $name;
    }

    protected function key(string $queue, string $part): string
    {
        return $this->prefix.$queue.':'.$part;
    }

    protected function jobKey(string $queue, int $id): string
    {
        return $this->prefix.$queue.':job:'.$id;
    }

    protected function leaseKey(string $queue, int $id): string
    {
        return $this->prefix.$queue.':lease:'.$id;
    }

    protected function gaugeKey(string $queue, int $generation): string
    {
        return $this->prefix.$queue.':delayed:'.$generation;
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
