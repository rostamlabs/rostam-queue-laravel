<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use Illuminate\Container\Container;
use PHPUnit\Framework\TestCase;
use Rostam\Queue\Exceptions\JobVanished;
use Rostam\Queue\RostamJob;
use Rostam\Queue\RostamQueue;
use Rostam\Testing\ArrayKvClient;

class RostamQueueTest extends TestCase
{
    private ArrayKvClient $client;

    private RostamQueue $queue;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = new ArrayKvClient;
        $this->queue = new RostamQueue($this->client, 'q:', 'default');
        $this->queue->setContainer(new Container);
        $this->queue->setConnectionName('rostam');
    }

    public function test_a_job_comes_back_out(): void
    {
        $this->queue->pushRaw('{"id":"a","displayName":"Job"}');

        $job = $this->queue->pop();

        $this->assertNotNull($job);
        $this->assertSame('{"id":"a","displayName":"Job"}', $job->getRawBody());
        $this->assertSame('a', $job->getJobId());
    }

    public function test_an_empty_queue_returns_nothing(): void
    {
        $this->assertNull($this->queue->pop());
    }

    public function test_jobs_come_out_in_the_order_they_went_in(): void
    {
        foreach (['one', 'two', 'three'] as $body) {
            $this->queue->pushRaw(json_encode(['id' => $body]));
        }

        $seen = [];
        while ($job = $this->queue->pop()) {
            $seen[] = json_decode($job->getRawBody(), true)['id'];
        }

        $this->assertSame(['one', 'two', 'three'], $seen);
    }

    public function test_a_job_is_not_handed_to_a_second_worker_while_the_first_holds_it(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'only']));

        $held = $this->queue->pop();

        $this->assertNotNull($held);
        $this->assertNull($this->queue->pop(), 'the same job was handed out while still held');
    }

    public function test_a_finished_job_does_not_come_back(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'only']));

        $this->queue->pop()->delete();

        $this->assertNull($this->queue->pop());
        $this->assertSame(0, $this->queue->size());
    }

    /**
     * THE property. A worker that dies holding a job must not take the job with
     * it - that is the difference between a queue and a place work goes to
     * disappear.
     *
     * The death is simulated the way it actually happens: the job was claimed,
     * nothing was deleted, and the lease lapsed because nobody was alive to
     * renew it.
     */
    public function test_a_job_survives_the_worker_that_was_holding_it(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'must-not-be-lost']));

        $held = $this->queue->pop();
        $this->assertNotNull($held);

        // The worker dies here: no delete(), no release(). Its lease runs out.
        $this->client->del('q:default:lease:1');

        $again = $this->queue->pop();

        $this->assertNotNull($again, 'the job died with its worker');
        $this->assertSame('must-not-be-lost', json_decode($again->getRawBody(), true)['id']);
        $this->assertSame(1, $this->queue->redeliveredCount());
    }

    /**
     * A redelivery is a new attempt, and the count has to survive the trip -
     * otherwise a job that always crashes its worker is retried for ever
     * instead of eventually failing.
     */
    public function test_a_redelivered_job_is_not_a_fresh_one(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'x', 'attempts' => 2]));

        $held = $this->queue->pop();
        $this->assertSame(3, $held->attempts());

        $held->release();

        $this->assertSame(4, $this->queue->pop()->attempts());
    }

    /**
     * A worker that is merely slow, not dead, still owns its job. Redelivery
     * must leave it alone, or the same work runs twice.
     */
    public function test_a_job_still_under_a_live_lease_is_left_alone(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'in-progress']));
        $this->queue->pushRaw(json_encode(['id' => 'next']));

        $held = $this->queue->pop();
        $this->assertNotNull($held);

        // Another worker comes along while the first is still going.
        $other = $this->queue->pop();

        $this->assertNotNull($other);
        $this->assertSame('next', json_decode($other->getRawBody(), true)['id'],
            'a job under a live lease was handed to a second worker');
        $this->assertSame(0, $this->queue->redeliveredCount());
    }

    /**
     * Releasing must not strand the job under a lease nobody will renew: it
     * comes straight back rather than waiting for the lease to lapse.
     */
    public function test_a_released_job_is_available_immediately(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'retry-me']));

        $this->queue->pop()->release();

        $again = $this->queue->pop();

        $this->assertNotNull($again, 'a released job had to wait for its lease to expire');
        $this->assertSame('retry-me', json_decode($again->getRawBody(), true)['id']);
    }

    /**
     * A lease belongs to the worker that took it. One worker must not be able
     * to release another's, or the job would be handed out twice.
     */
    public function test_a_lease_can_only_be_released_by_its_holder(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'held']));
        $held = $this->queue->pop();

        $impostor = new RostamQueue($this->client, 'q:', 'default');

        $this->assertFalse(
            $impostor->reservation('default', 1)->release(),
            'another worker released a lease it did not hold',
        );
        $this->assertTrue($held->reservation()->release());
    }

    /**
     * The design's load-bearing property, and the reason head moves by
     * compare-and-swap rather than by incr. Two workers polling an empty queue
     * must not consume ids that a later push will occupy - with an unconditional
     * incr they do, and that push becomes unreadable.
     */
    public function test_polling_an_empty_queue_does_not_swallow_the_next_push(): void
    {
        $this->assertNull($this->queue->pop());
        $this->assertNull($this->queue->pop());
        $this->assertNull($this->queue->pop());

        $this->queue->pushRaw(json_encode(['id' => 'arrived-after-the-polling']));

        $job = $this->queue->pop();

        $this->assertNotNull($job, 'a job pushed after idle polling was lost');
        $this->assertSame('arrived-after-the-polling', json_decode($job->getRawBody(), true)['id']);
    }

    public function test_size_counts_what_is_waiting(): void
    {
        $this->assertSame(0, $this->queue->size());

        $this->queue->pushRaw(json_encode(['id' => 'a']));
        $this->queue->pushRaw(json_encode(['id' => 'b']));

        $this->assertSame(2, $this->queue->size());

        $this->queue->pop()->delete();

        $this->assertSame(1, $this->queue->size());
    }

    /**
     * A worker that dies between claiming a job and moving the cursor leaves an
     * empty slot. Nothing else will step over it, so the next pop must - or the
     * queue stalls on a hole for good.
     */
    public function test_a_hole_left_by_a_dead_worker_is_stepped_over(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'abandoned']));
        $this->queue->pushRaw(json_encode(['id' => 'next']));

        // The claim happened; the cursor never moved.
        $this->client->getdel('q:default:job:1');

        $job = $this->queue->pop();

        $this->assertNotNull($job, 'the queue stalled on a hole');
        $this->assertSame('next', json_decode($job->getRawBody(), true)['id']);
        $this->assertSame(1, $this->queue->holesSeen());
    }

    /**
     * One hole is a dead worker. A run of them is the store dropping jobs, and
     * a queue that quietly skips missing work looks healthy while it loses.
     */
    public function test_a_run_of_holes_is_reported_rather_than_skipped(): void
    {
        for ($i = 0; $i < 100; $i++) {
            $this->queue->pushRaw(json_encode(['id' => $i]));
        }

        // Everything evicted, as PolicyRingbufEvict would.
        for ($id = 1; $id <= 100; $id++) {
            $this->client->del('q:default:job:'.$id);
        }

        $this->expectException(JobVanished::class);
        $this->expectExceptionMessageMatches('/stepped over \d+ empty slots/');

        $this->queue->pop();
    }

    public function test_clear_empties_the_queue(): void
    {
        foreach (range(1, 5) as $i) {
            $this->queue->pushRaw(json_encode(['id' => $i]));
        }

        $this->assertSame(5, $this->queue->clear());
        $this->assertSame(0, $this->queue->size());
        $this->assertNull($this->queue->pop());
    }

    /**
     * Clearing moves the reader up to the writer rather than resetting both, so
     * a push already in flight cannot land in a slot the reader has passed.
     */
    public function test_clear_does_not_strand_a_later_push(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'old']));
        $this->queue->clear();

        $this->queue->pushRaw(json_encode(['id' => 'new']));

        $job = $this->queue->pop();

        $this->assertNotNull($job);
        $this->assertSame('new', json_decode($job->getRawBody(), true)['id']);
    }

    public function test_a_job_carries_its_queue_and_connection(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'a']), 'emails');

        $job = $this->queue->pop('emails');

        $this->assertNotNull($job);
        $this->assertSame('emails', $job->getQueue());
        $this->assertSame('rostam', $job->getConnectionName());
    }

    public function test_queues_do_not_see_each_other(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'for-emails']), 'emails');

        $this->assertNull($this->queue->pop('reports'));
        $this->assertNotNull($this->queue->pop('emails'));
    }

    /**
     * The property the compare-and-swap actually buys, and the one a
     * single-threaded test cannot see.
     *
     * Two workers can examine the same slot at the same moment: both read the
     * same head, both try to claim it, one wins the getdel and one gets
     * nothing. Then BOTH advance. If the cursor moved with an unconditional
     * incr, head would jump by two and the very next job would be stepped over
     * without ever being handed to anyone - a queue that silently drops the
     * job after the one it just delivered.
     *
     * With a compare-and-swap only one of the two advances; the loser learns
     * the slot was not its own and looks again.
     *
     * The rival pop is wedged in through the fake client's write hook, at the
     * exact instant the first one is about to move the cursor.
     */
    public function test_two_workers_on_the_same_slot_do_not_skip_the_next_job(): void
    {
        $this->queue->pushRaw(json_encode(['id' => 'first']));
        $this->queue->pushRaw(json_encode(['id' => 'second']));

        $rival = null;
        $this->client->beforeWrite = function (string $key) use (&$rival) {
            if ($key !== 'q:default:head' || $rival !== null) {
                return;
            }

            // A second worker, arriving in the window between the winner
            // claiming the job and moving the cursor.
            $rival = $this->queue->pop() ?? false;
        };

        $first = $this->queue->pop();
        $this->client->beforeWrite = null;

        $this->assertNotNull($first);
        $this->assertSame('first', json_decode($first->getRawBody(), true)['id']);

        // Whatever the rival saw, "second" must still be deliverable to
        // somebody - either it took it, or it is still there to take.
        $delivered = array_filter([
            $rival instanceof RostamJob ? json_decode($rival->getRawBody(), true)['id'] : null,
            ($later = $this->queue->pop()) ? json_decode($later->getRawBody(), true)['id'] : null,
        ]);

        $this->assertContains('second', $delivered, 'the job after the claimed one was skipped entirely');
    }
}
