<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;

/**
 * One job, held under a lease.
 *
 * The payload is still in the store while this worker works on it - claiming
 * took a lease, not the job itself. That is what makes the queue survive a
 * worker dying: the lease expires on its own and {@see
 * RostamQueue::redeliverAbandonedJobs()} hands the work to somebody else.
 *
 * Which puts the weight on delete(). Until it runs, this job is still owed to
 * someone, and Laravel's worker calls it exactly once a job has been handled.
 */
class RostamJob extends Job implements JobContract
{
    /** @var array<string, mixed> */
    protected array $decoded;

    public function __construct(
        Container $container,
        protected RostamQueue $rostam,
        protected string $payload,
        string $connectionName,
        string $queue,
        protected int $id,
        protected Reservation $reservation,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->decoded = $this->payload();
    }

    public function getRawBody(): string
    {
        return $this->payload;
    }

    public function getJobId(): mixed
    {
        return $this->decoded['id'] ?? (string) $this->id;
    }

    /**
     * How many times this job has been handed to a worker.
     *
     * The count travels inside the payload. It cannot live beside the job,
     * because a redelivery is a new push at a new id and anything keyed to the
     * old one would be left behind.
     */
    public function attempts(): int
    {
        return ($this->decoded['attempts'] ?? 0) + 1;
    }

    /**
     * Finished. The payload goes and the lease with it.
     */
    public function delete(): void
    {
        parent::delete();

        $this->rostam->complete($this->queue, $this->id, $this->reservation);
    }

    /**
     * Put the job back for another attempt.
     *
     * A fresh copy is pushed with the attempt count carried forward, rather
     * than the lease simply being dropped. Dropping it would work - redelivery
     * would find the job unheld and requeue it - but only after the lease
     * expired, which would turn an immediate retry into a retry_after-long wait
     * nobody asked for.
     *
     * The new copy is written BEFORE the old one is finished. The other order
     * deleted the job first, so a retry that failed to land - a dead connection,
     * a killed worker - left neither copy. This way a failure part-way leaves
     * the old job under a lease that lapses, and it comes back.
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $payload = (string) json_encode(array_merge($this->decoded, [
            'attempts' => $this->attempts(),
        ]), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        if ($delay > 0) {
            $this->rostam->laterRaw($delay, $payload, $this->queue);
        } else {
            $this->rostam->pushRaw($payload, $this->queue);
        }

        $this->rostam->complete($this->queue, $this->id, $this->reservation);
    }

    /**
     * Hold the job for longer, for work that outlives its lease.
     */
    public function extendLease(int $seconds): bool
    {
        return $this->reservation->extend($seconds);
    }

    public function reservation(): Reservation
    {
        return $this->reservation;
    }
}
