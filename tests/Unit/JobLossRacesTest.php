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

    /**
     * Every window a test opened must actually have opened. A hook that never
     * fired means the interleaving never happened, and a green test here would
     * be a test of nothing.
     */
    protected function assertPostConditions(): void
    {
        $this->assertSame([], $this->client->pendingHooks(), 'a hook never fired, so its interleaving never happened');
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();

        parent::tearDown();
    }

    /**
     * A worker on the hooked client, or - for a rival whose own operations
     * must not trip the hooks - straight on the store.
     */
    private function worker(string $owner, bool $hooked = true): RostamQueue
    {
        $queue = new RostamQueue($hooked ? $this->client : $this->store, 'q:', 'default', retryAfter: 30, owner: $owner);
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

        $this->client->before('getset|delMany', 'q:default:job:', function () {
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
     * A sweep used to read the delayed generation once, before its loop. A
     * clear() finishing while that sweep was on its way, followed by a job
     * delayed into the very second being swept, looked to the sweep like a job
     * written before the clear - and it deleted it. The job was accepted after
     * the clear had returned.
     */
    public function test_a_job_delayed_after_a_clear_is_not_taken_for_one_it_cleared(): void
    {
        $this->worker('warm-up')->pop();
        Carbon::setTestNow(Carbon::now()->addSecond());
        $second = Carbon::now()->getTimestamp();

        $this->client->before('get', 'q:default:d:'.$second.':tail', function () {
            $this->worker('operator')->clear();
            $this->worker('producer')->laterRaw(0, self::job('after-clear'));
        });

        $this->worker('sweeper')->pop();

        $this->assertSame(['after-clear'], $this->drain($this->worker('healthy')));
        $this->assertSame(0, $this->worker('monitor')->size());
    }

    /**
     * A clear() in the middle of a move used to zero a gauge that the move
     * then decremented, driving it below zero - and every delayed job after
     * that read as none.
     */
    public function test_a_clear_during_a_move_does_not_hide_later_delayed_jobs(): void
    {
        $this->worker('warm-up')->pop();
        $this->worker('producer')->laterRaw(1, self::job('moving'));
        Carbon::setTestNow(Carbon::now()->addSeconds(2));

        $this->client->before('del', ':d:', function () {
            $this->worker('operator')->clear();
        });

        $this->worker('sweeper')->pop();

        $later = $this->worker('producer-2');
        $later->laterRaw(3600, self::job('an-hour-away'));

        $this->assertSame(1, $later->delayedSize());
    }

    /**
     * A push that had drawn its id when the queue was cleared used to write
     * into a slot the reader had already jumped past - delivered by nobody,
     * cleared by nobody. clear() now kills every slot it passes, so the push
     * draws a fresh id beyond the clear.
     */
    public function test_a_push_straddling_a_clear_is_delivered_not_orphaned(): void
    {
        $this->client->before('setNx', 'q:default:job:1', function () {
            $this->worker('operator')->clear();
            $this->worker('passer')->pop();
        });

        $this->worker('producer')->pushRaw(self::job('straddler'));

        $this->assertSame(['straddler'], $this->drain($this->worker('healthy')));
        $this->assertSame(0, $this->worker('monitor')->size());
    }

    /**
     * Losing lease races is not losing jobs. A pop used to give up after 64
     * steps of any kind and report "jobs were written and then vanished" with
     * zero holes - on a healthy queue, under nothing but contention.
     */
    public function test_losing_lease_races_is_not_reported_as_vanished_jobs(): void
    {
        // The rival works on the store directly, so each hook fires for one of
        // the slow worker's lease attempts and not inside the rival's own.
        $rival = $this->worker('rival', hooked: false);

        for ($i = 0; $i < 70; $i++) {
            $rival->pushRaw(self::job('j'.$i));
        }

        for ($i = 0; $i < 64; $i++) {
            $this->client->before('setNx', ':lease:', static function () use ($rival) {
                $rival->pop();
            });
        }

        $job = $this->worker('slow')->pop();

        $this->assertNotNull($job, 'a worker that lost 64 races found nothing, with jobs waiting');
    }

    /**
     * One job still running used to block the redelivery of every abandoned job
     * above it: the walk stopped at the first live lease.
     */
    public function test_an_abandoned_job_is_redelivered_while_an_older_one_is_still_running(): void
    {
        $producer = $this->worker('producer');
        $producer->pushRaw(self::job('long-running'));
        $producer->pushRaw(self::job('abandoned'));

        $running = $this->worker('alive')->pop();
        $this->worker('dies')->pop();
        $this->store->ageOut('q:default:lease:2');

        $next = $this->worker('next')->pop();

        $this->assertNotNull($running);
        $this->assertNotNull($next, 'the abandoned job waited behind one still running');
        $this->assertSame('abandoned', json_decode($next->getRawBody(), true)['id']);
    }

    /**
     * An idle queue used to leave a sealed counter behind for every second it
     * swept, each living a week. The seal is needed only until the watermark
     * passes its second.
     */
    public function test_sweeping_idle_seconds_leaves_no_keys_behind(): void
    {
        $worker = $this->worker('idle');

        for ($elapsed = 0; $elapsed <= 3600; $elapsed += 30) {
            Carbon::setTestNow(Carbon::createFromTimestamp(1_800_000_000 + $elapsed));
            $worker->pop();
        }

        $buckets = array_filter(array_keys($this->store->all()), static fn (string $key) => str_contains($key, ':d:'));

        $this->assertSame([], array_values($buckets));
    }

    /**
     * The interleaving the seal exists for now that a push checks the watermark
     * after storing: the sweep has counted the second and not yet moved the
     * watermark past it. A push drawing an id in that gap would store a job the
     * sweep never counted and still see the watermark behind its second. The
     * seal makes that id carry SEALED, and the push goes to the ready queue.
     */
    public function test_a_delayed_push_between_the_sweep_counting_a_second_and_passing_it_is_not_lost(): void
    {
        // The sweep starts one second short of $due, so the watermark's move
        // the hook catches is the one past $due itself.
        $due = Carbon::now()->getTimestamp() + 5;
        Carbon::setTestNow(Carbon::createFromTimestamp($due - 1));
        $this->worker('warm-up')->pop();
        Carbon::setTestNow(Carbon::createFromTimestamp($due));

        $this->client->before('cas', 'q:default:swept', function () use ($due) {
            $this->worker('producer')->laterRaw(Carbon::createFromTimestamp($due), self::job('in-the-gap'));
        });

        $this->worker('sweeper')->pop();

        $this->assertSame(['in-the-gap'], $this->drain($this->worker('healthy')));
    }

    /**
     * A second whose jobs are not all moved - one is under another worker's
     * move - keeps its counter, which is the only record of how many ids it
     * handed out. Dropping it then would leave the unmoved job where no later
     * sweep could count to.
     */
    public function test_a_second_still_being_moved_keeps_its_count_until_every_job_is_out(): void
    {
        $this->worker('warm-up')->pop();
        $producer = $this->worker('producer');
        $producer->laterRaw(3, self::job('first'));
        $producer->laterRaw(3, self::job('second'));
        $due = Carbon::now()->getTimestamp() + 3;
        Carbon::setTestNow(Carbon::createFromTimestamp($due + 1));

        // Another worker is part-way through moving the second job, and dies.
        $this->store->put('q:default:mig:'.$due.':2', 'a-worker-that-died', 30);

        $this->worker('sweeper')->pop();
        $this->everyWorkerIsGone();

        $delivered = $this->drain($this->worker('healthy'));
        sort($delivered);

        $this->assertSame(['first', 'second'], $delivered);
    }

    /**
     * Two sweepers reaching the same delayed job: without its move lease both
     * would copy it to the ready queue, and the queue itself would run it twice.
     */
    public function test_two_sweepers_on_one_delayed_job_move_it_once(): void
    {
        $this->worker('warm-up')->pop();
        $this->worker('producer')->laterRaw(1, self::job('moved-once'));
        Carbon::setTestNow(Carbon::now()->addSeconds(2));

        $delivered = [];

        $this->client->before('increment', 'q:default:tail', function () use (&$delivered) {
            if ($job = $this->worker('rival-sweeper')->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
        });

        if ($job = $this->worker('sweeper')->pop()) {
            $delivered[] = json_decode($job->getRawBody(), true)['id'];
            $job->delete();
        }

        $this->assertSame(['moved-once'], array_merge($delivered, $this->drain($this->worker('healthy'))));
    }

    /**
     * Between a sweeper looking at a delayed job and taking its move lease,
     * another can move it entirely. Moving what was read before the lease would
     * put it on the ready queue a second time.
     */
    public function test_a_delayed_job_moved_by_another_worker_before_the_lease_is_not_moved_again(): void
    {
        $this->worker('warm-up')->pop();
        $this->worker('producer')->laterRaw(1, self::job('moved-once'));
        Carbon::setTestNow(Carbon::now()->addSeconds(2));

        $delivered = [];

        $this->client->before('setNx', ':mig:', function () use (&$delivered) {
            if ($job = $this->worker('faster')->pop()) {
                $delivered[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
        });

        if ($job = $this->worker('slower')->pop()) {
            $delivered[] = json_decode($job->getRawBody(), true)['id'];
            $job->delete();
        }

        $this->assertSame(['moved-once'], array_merge($delivered, $this->drain($this->worker('healthy'))));
    }

    /**
     * A worker whose lease lapsed can still finish its job, late - between
     * redelivery seeing the job unheld and taking its lease. Putting back what
     * was seen before the lease would run a finished job again.
     */
    public function test_a_job_finished_late_is_not_redelivered(): void
    {
        $this->worker('producer')->pushRaw(self::job('finished-late'));
        $slow = $this->worker('slow')->pop();
        $this->store->ageOut('q:default:lease:1');

        $this->client->before('setNx', 'q:default:lease:1', static function () use ($slow) {
            $slow->delete();
        });

        $this->assertNull($this->worker('reclaimer')->pop());
        $this->assertSame([], $this->drain($this->worker('healthy')));
    }

    /**
     * A worker killed while putting an abandoned job back must not take it
     * along: the copy is written before the original goes.
     */
    public function test_a_worker_killed_while_putting_back_an_abandoned_job_does_not_lose_it(): void
    {
        $this->worker('producer')->pushRaw(self::job('abandoned'));
        $this->worker('died-holding-it')->pop();
        $this->everyWorkerIsGone();

        $this->client->before('increment', 'q:default:tail', InterleavingClient::dies());

        try {
            $this->worker('also-dies')->pop();
            $this->fail('the worker was supposed to die while putting the job back');
        } catch (WorkerDied) {
        }

        $this->everyWorkerIsGone();

        $this->assertSame(['abandoned'], $this->drain($this->worker('healthy')));
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
