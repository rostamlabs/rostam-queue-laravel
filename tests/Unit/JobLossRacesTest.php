<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use Illuminate\Container\Container;
use Illuminate\Support\Carbon;
use PHPUnit\Framework\TestCase;
use Rostam\Queue\RostamQueue;
use Rostam\Queue\Tests\Support\InterleavingClient;
use Rostam\Queue\Tests\Support\WorkerDied;
use Rostam\Testing\ArrayKvClient;

/**
 * Every way this queue has been found to lose or duplicate a job, as a test.
 *
 * The invariant is one sentence: a job the queue accepted is delivered, and it
 * is not delivered twice by the queue's own doing. Each test below drives the
 * exact interleaving that broke it - a push caught between drawing its id and
 * writing its payload, a worker killed between reading a delayed job and
 * enqueueing it - through a client that can stop the world between any two
 * round trips.
 *
 * They were written red, against the code that had these races, before any of
 * it was changed.
 */
class JobLossRacesTest extends TestCase
{
    private ArrayKvClient $store;

    private InterleavingClient $client;

    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow(Carbon::createFromTimestamp(1_800_000_000));

        $this->store = new ArrayKvClient;
        $this->client = new InterleavingClient($this->store);
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();

        parent::tearDown();
    }

    private function worker(string $owner): RostamQueue
    {
        $queue = new RostamQueue($this->client, 'q:', 'default', retryAfter: 30, owner: $owner);
        $queue->setContainer(new Container);
        $queue->setConnectionName('rostam');

        return $queue;
    }

    private static function job(string $id): string
    {
        return json_encode(['id' => $id, 'attempts' => 0]);
    }

    /**
     * Every lease and every in-flight claim lapses, as it would once the
     * worker that held it is gone for longer than its TTL.
     */
    private function everyWorkerIsGone(): void
    {
        foreach (array_keys($this->store->all()) as $key) {
            if (str_contains($key, ':lease:') || str_contains($key, ':mig:')) {
                $this->store->ageOut($key);
            }
        }
    }

    /**
     * Take everything a healthy worker can take, finishing each job, until the
     * queue has nothing left even after every stale lease has lapsed.
     *
     * @return list<string> the ids delivered, in order
     */
    private function drain(RostamQueue $worker): array
    {
        $delivered = [];

        for ($round = 0; $round < 3; $round++) {
            while ($job = $worker->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }

            $this->everyWorkerIsGone();
        }

        return $delivered;
    }

    /**
     * P0. A push draws its id, and before its payload lands a worker finds
     * that slot empty and steps over it - and a second pass of the redelivery
     * sweep steps over it too. The payload then lands behind both cursors,
     * where nothing will ever look.
     */
    public function test_a_push_caught_between_its_id_and_its_payload_is_not_lost(): void
    {
        $producer = $this->worker('producer');

        $this->client->before('put|setNx|cas', 'q:default:job:1', function () {
            $this->worker('rival-1')->pop();
            $this->worker('rival-2')->pop();
        });

        $producer->pushRaw(self::job('A'));

        $this->assertSame(['A'], $this->drain($this->worker('healthy')));
    }

    /**
     * P0. Migration read a delayed job destructively and the worker died
     * before enqueueing it: the only copy was gone.
     */
    public function test_a_worker_killed_while_moving_a_delayed_job_does_not_take_it_along(): void
    {
        $this->worker('producer')->laterRaw(5, self::job('B'));
        Carbon::setTestNow(Carbon::now()->addSeconds(10));

        $this->client->before('increment', 'q:default:tail', InterleavingClient::dies());

        try {
            $this->worker('doomed')->pop();
            $this->fail('the worker was supposed to die mid-migration');
        } catch (WorkerDied) {
        }

        $this->assertSame(['B'], $this->drain($this->worker('healthy')));
    }

    /**
     * P1, and not even a race. A job due in the past went into a bucket the
     * sweep had already passed, and no sweep ever went back for it.
     */
    public function test_a_job_due_in_the_past_is_still_delivered(): void
    {
        $this->worker('warm-up')->pop();

        $this->worker('producer')->laterRaw(Carbon::now()->subMinute(), self::job('C'));

        $this->assertSame(['C'], $this->drain($this->worker('healthy')));
    }

    /**
     * P1. The delayed-job twin of the first test: the bucket id was drawn,
     * the sweep passed that second while the payload was still on its way,
     * and the payload landed in a bucket nobody would read again.
     */
    public function test_a_delayed_push_caught_by_the_sweep_is_not_lost(): void
    {
        $due = Carbon::now()->getTimestamp() + 5;

        $this->client->before('put|setNx|cas', 'q:default:d:'.$due.':job:', function () use ($due) {
            Carbon::setTestNow(Carbon::createFromTimestamp($due + 1));
            $this->worker('sweeper')->pop();
        });

        $this->worker('producer')->laterRaw(5, self::job('H'));

        Carbon::setTestNow(Carbon::createFromTimestamp($due + 3));

        $this->assertSame(['H'], $this->drain($this->worker('healthy')));
    }

    /**
     * The interleaving the seal exists for, which a tombstone cannot cover.
     *
     * The push checked the watermark and found its second still ahead, then the
     * sweep read that second's id counter, found nothing, and finished it - and
     * only then did the push draw its id. That id was never going to be looked
     * at: the sweep had counted before it existed. Sealing the counter as the
     * sweep passes it means an id drawn afterwards carries the seal, and the
     * push sends its job to the ready queue instead.
     */
    public function test_a_delayed_push_that_draws_its_id_after_the_sweep_finished_that_second_is_not_lost(): void
    {
        $due = Carbon::now()->getTimestamp() + 5;

        $this->client->before('increment', 'q:default:d:'.$due.':tail', function () use ($due) {
            Carbon::setTestNow(Carbon::createFromTimestamp($due + 1));
            $this->worker('sweeper')->pop();
        });

        $this->worker('producer')->laterRaw(5, self::job('late-id'));

        Carbon::setTestNow(Carbon::createFromTimestamp($due + 3));

        $this->assertSame(['late-id'], $this->drain($this->worker('healthy')));
    }

    /**
     * clear() jumps the reader to the writer. Written unconditionally, that
     * jump could land BEHIND a reader that concurrent workers had already moved
     * further, dragging it back over slots that were claimed and finished.
     */
    public function test_clearing_never_drags_the_reader_backwards(): void
    {
        $queue = $this->worker('operator');
        $queue->pushRaw(self::job('before'));

        $this->client->before('delMany', 'q:default:job:', function () {
            $worker = $this->worker('busy');
            $worker->pushRaw(self::job('during-1'));
            $worker->pushRaw(self::job('during-2'));

            while ($job = $worker->pop()) {
                $job->delete();
            }
        });

        $queue->clear();

        $head = unpack('J', (string) $this->store->get('q:default:head'))[1];

        $this->assertGreaterThanOrEqual(3, $head, 'clear() moved the reader back behind jobs already taken');
    }

    /**
     * P1. release() finished the old copy first and pushed the retry second;
     * if the push never happened, neither copy was left.
     */
    public function test_a_release_whose_retry_never_lands_does_not_lose_the_job(): void
    {
        $this->worker('producer')->pushRaw(self::job('D'));
        $job = $this->worker('doomed')->pop();

        $this->client->before('increment', 'q:default:tail', InterleavingClient::dies());

        try {
            $job->release();
            $this->fail('the retry was supposed to fail');
        } catch (WorkerDied) {
        }

        $this->everyWorkerIsGone();

        $this->assertContains('D', $this->drain($this->worker('healthy')));
    }

    /**
     * P1. Clearing an empty queue stored an explicit zero, and a cursor that
     * only knew how to move from "absent" could never move again.
     */
    public function test_clearing_an_empty_queue_does_not_wedge_it(): void
    {
        $queue = $this->worker('operator');
        $queue->clear();

        $queue->pushRaw(self::job('E1'));
        $queue->pushRaw(self::job('E2'));

        $this->assertSame(['E1', 'E2'], $this->drain($this->worker('healthy')));
    }

    /**
     * P1. Two workers both found the same abandoned job before either moved
     * the redelivery cursor, and both put it back: the queue itself ran it
     * twice.
     */
    public function test_an_abandoned_job_is_put_back_once_not_once_per_worker(): void
    {
        $this->worker('producer')->pushRaw(self::job('F'));
        $this->worker('died-holding-it')->pop();
        $this->everyWorkerIsGone();

        $delivered = [];

        $this->client->before('increment', 'q:default:tail', function () use (&$delivered) {
            if ($job = $this->worker('rival')->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
        });

        if ($job = $this->worker('first')->pop()) {
            $delivered[] = json_decode($job->getRawBody(), true)['id'];
            $job->delete();
        }

        $delivered = array_merge($delivered, $this->drain($this->worker('healthy')));

        $this->assertSame(['F'], $delivered);
    }

    /**
     * P1. A job that kills its worker came back with the same attempt count
     * every time, so it could never reach maxTries and be failed - it would
     * take down workers forever.
     */
    public function test_a_job_that_killed_its_worker_counts_that_as_an_attempt(): void
    {
        $this->worker('producer')->pushRaw(self::job('G'));

        $first = $this->worker('killed')->pop();
        $this->assertSame(1, $first->attempts());

        $this->everyWorkerIsGone();

        $second = $this->worker('next')->pop();
        $this->assertNotNull($second, 'the abandoned job was not handed back');
        $this->assertSame(2, $second->attempts());
    }

    /**
     * Found while rewriting the migration, not by the review, and older than
     * any of it. The sweep's watermark used to default to "a second ago"
     * without being stored, so on a fresh queue it moved with the clock: a job
     * delayed before any worker had ever run sat in a bucket for a second the
     * first worker - starting later, from its own "a second ago" - had already
     * put behind it. It was never swept.
     */
    public function test_a_job_delayed_before_any_worker_ever_ran_is_delivered_when_one_does(): void
    {
        $this->worker('producer')->laterRaw(5, self::job('early'));

        Carbon::setTestNow(Carbon::now()->addHour());

        $this->assertSame(['early'], $this->drain($this->worker('starts-an-hour-later')));
    }

    /**
     * clear() promised waiting AND delayed jobs, and deleted only the waiting
     * ones: a delayed job came back after the queue had been cleared.
     */
    public function test_clearing_a_queue_also_clears_its_delayed_jobs(): void
    {
        $queue = $this->worker('operator');
        $queue->pushRaw(self::job('ready'));
        $queue->laterRaw(30, self::job('delayed'));

        $this->assertSame(2, $queue->clear());

        Carbon::setTestNow(Carbon::now()->addMinute());

        $this->assertSame([], $this->drain($this->worker('healthy')));
        $this->assertSame(0, $queue->size());
    }

    /**
     * The count used to scan only the next sweep window, so a job delayed by an
     * hour was not in size() at all.
     */
    public function test_a_job_delayed_far_beyond_the_sweep_window_is_counted(): void
    {
        $queue = $this->worker('producer');
        $queue->laterRaw(3600, self::job('an-hour-away'));
        $queue->pushRaw(self::job('now'));

        $this->assertSame(1, $queue->delayedSize());
        $this->assertSame(1, $queue->pendingSize());
        $this->assertSame(2, $queue->size());
    }

    /**
     * Between reading a payload and taking its lease, another worker can claim
     * the job, finish it and let the lease go. A lease taken after that is a
     * lease on work already done, and running it would be the queue's own
     * duplicate.
     */
    public function test_a_job_finished_between_being_read_and_being_leased_is_not_run_again(): void
    {
        $this->worker('producer')->pushRaw(self::job('once'));

        $delivered = [];

        $this->client->before('setNx', 'q:default:lease:1', function () use (&$delivered) {
            if ($job = $this->worker('faster')->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
        });

        if ($job = $this->worker('slower')->pop()) {
            $delivered[] = json_decode($job->getRawBody(), true)['id'];
            $job->delete();
        }

        $delivered = array_merge($delivered, $this->drain($this->worker('healthy')));

        $this->assertSame(['once'], $delivered);
    }

    /**
     * A slot a worker reached on a stale head, after another worker had already
     * claimed and finished it, is empty - but it was never a hole, and counting
     * it as one would make a busy healthy queue look like one losing jobs.
     */
    public function test_an_empty_slot_that_was_already_dealt_with_is_not_counted_as_a_hole(): void
    {
        $this->worker('producer')->pushRaw(self::job('x'));

        $stale = $this->worker('stale');

        $this->client->before('get', 'q:default:job:1', function () {
            if ($job = $this->worker('fast')->pop()) {
                $job->delete();
            }
        });

        $this->assertNull($stale->pop());
        $this->assertSame(0, $stale->holesSeen());
    }

    /**
     * Only this driver's laterRaw writes a generation into a delayed job. One
     * without it carries nothing to say it was cleared, so it is delivered - a
     * guess the other way would be a job thrown away on no evidence.
     */
    public function test_a_delayed_job_without_a_generation_is_delivered_not_discarded(): void
    {
        $due = Carbon::now()->getTimestamp() + 2;

        $this->store->put('q:default:d:'.$due.':tail', pack('J', 1));
        $this->store->put('q:default:d:'.$due.':job:1', self::job('foreign'));
        $this->store->put('q:default:swept', pack('J', $due - 3));

        Carbon::setTestNow(Carbon::createFromTimestamp($due + 1));

        $this->assertSame(['foreign'], $this->drain($this->worker('healthy')));
    }

    /**
     * The property the head cursor's compare-and-swap exists for. The test this
     * replaces set its rival on a write hook that never fires for `cas` - the
     * only way the head moves - so it passed without a rival ever running.
     */
    public function test_two_workers_on_the_same_slot_do_not_skip_the_next_job(): void
    {
        $producer = $this->worker('producer');
        $producer->pushRaw(self::job('first'));
        $producer->pushRaw(self::job('second'));

        $rivalRan = false;
        $delivered = [];

        $this->client->before('cas', 'q:default:head', function () use (&$rivalRan, &$delivered) {
            $rivalRan = true;

            if ($job = $this->worker('rival')->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
        });

        if ($job = $this->worker('winner')->pop()) {
            $delivered[] = json_decode($job->getRawBody(), true)['id'];
            $job->delete();
        }

        $this->assertTrue($rivalRan, 'the rival never ran, so this test proved nothing');

        $delivered = array_merge($delivered, $this->drain($this->worker('healthy')));
        sort($delivered);

        $this->assertSame(['first', 'second'], $delivered);
    }
}
