<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use Illuminate\Container\Container;
use Illuminate\Support\Carbon;
use PHPUnit\Framework\TestCase;
use Rostam\Contracts\KvClient;
use Rostam\Contracts\ReportsKvMetrics;
use Rostam\Exceptions\ServerException;
use Rostam\Kv\Metrics\KvMetrics;
use Rostam\Kv\Protocol\Status;
use Rostam\Queue\EvictionWatch;
use Rostam\Queue\Exceptions\UnsafeQueueStore;
use Rostam\Queue\RostamQueue;
use Rostam\Queue\Tests\Support\InterleavingClient;
use Rostam\Testing\ArrayKvClient;

/**
 * The one thing about durability the wire can actually tell a queue.
 *
 * A single-node rostam-server evicts at capacity without an error, and nothing
 * reports whether a node is the kind that evicts. What v0.7.0-beta3 reports is
 * how many live records it has already thrown away, and for a node holding
 * queued jobs the only acceptable number is zero. These tests pin that the
 * number stops a queue, that a server unable to give it is handled as the
 * declaration demands, and that a failure to answer is never read as a zero.
 */
class EvictionWatchTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow(Carbon::createFromTimestamp(1_800_000_000));
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();

        parent::tearDown();
    }

    private function queue(KvClient $client, bool $required = false, int $every = 60): RostamQueue
    {
        $queue = new RostamQueue(
            $client, 'q:', 'default',
            watch: new EvictionWatch($client, required: $required, everySeconds: $every),
        );
        $queue->setContainer(new Container);
        $queue->setConnectionName('rostam');

        return $queue;
    }

    /**
     * A client whose server does not know `__kv_metrics__`: what a rostam
     * older than v0.7.0-beta3 answers is the generic error.
     */
    private static function olderServer(ArrayKvClient $store, int $status = Status::ERROR): KvClient
    {
        return new class($store, $status) extends InterleavingClient implements ReportsKvMetrics
        {
            public function __construct(ArrayKvClient $inner, private readonly int $status)
            {
                parent::__construct($inner);
            }

            public function kvMetrics(): KvMetrics
            {
                throw new ServerException($this->status, 'internal error', '__kv_metrics__');
            }
        };
    }

    public function test_a_node_that_has_evicted_live_records_is_refused_before_a_job_is_accepted(): void
    {
        $store = new ArrayKvClient;
        $store->simulateLiveEvictions(165);

        try {
            $this->queue($store)->pushRaw('{"id":"a"}');
            $this->fail('a job was accepted by a node that has already evicted live records');
        } catch (UnsafeQueueStore $exception) {
            $this->assertStringContainsString('evicted 165 live record', $exception->getMessage());
        }

        $this->assertSame([], array_keys($store->all()), 'something was written before the refusal');
    }

    public function test_a_node_that_has_evicted_live_records_is_refused_before_a_job_is_taken(): void
    {
        $store = new ArrayKvClient;
        $this->queue($store)->pushRaw('{"id":"a"}');
        $store->simulateLiveEvictions(1);

        $this->expectException(UnsafeQueueStore::class);

        $this->queue($store)->pop();
    }

    /**
     * Evictions that begin after the queue started are caught at the next
     * check, not only at the first one - a node fills up over time.
     */
    public function test_evictions_that_start_while_running_stop_the_queue_at_the_next_check(): void
    {
        $store = new ArrayKvClient;
        $queue = $this->queue($store, every: 60);

        $queue->pushRaw('{"id":"a"}');
        $store->simulateLiveEvictions(2);

        // Inside the interval nothing is re-read, so the queue carries on.
        Carbon::setTestNow(Carbon::now()->addSeconds(30));
        $queue->pushRaw('{"id":"b"}');

        Carbon::setTestNow(Carbon::now()->addSeconds(31));

        $this->expectException(UnsafeQueueStore::class);

        $queue->pushRaw('{"id":"c"}');
    }

    public function test_the_count_is_not_re_read_inside_the_interval(): void
    {
        $store = new ArrayKvClient;
        $queue = $this->queue($store, every: 60);

        $queue->pushRaw('{"id":"a"}');
        $queue->pushRaw('{"id":"b"}');
        $queue->pop();

        $this->assertSame(1, count(array_keys($store->ops, 'kvMetrics', true)));
    }

    /**
     * A watch told the count is optional runs on a server too old to report
     * it. (The connector never builds one: "headroom" requires the count.)
     */
    public function test_a_watch_that_does_not_need_the_count_runs_without_it(): void
    {
        $store = new ArrayKvClient;
        $queue = $this->queue(self::olderServer($store), required: false);

        $queue->pushRaw('{"id":"a"}');

        $this->assertNotNull($queue->pop());
    }

    /**
     * "headroom" names a single node that must never lose a live record. The
     * count is the only thing that would ever notice it did, so a server that
     * cannot give it is refused.
     */
    public function test_headroom_is_refused_on_a_server_that_cannot_be_checked(): void
    {
        $this->expectException(UnsafeQueueStore::class);
        $this->expectExceptionMessageMatches('/"headroom" needs the node to report its evictions/');

        $this->queue(self::olderServer(new ArrayKvClient), required: true)->pushRaw('{"id":"a"}');
    }

    public function test_headroom_is_refused_by_a_client_that_cannot_ask(): void
    {
        $this->expectException(UnsafeQueueStore::class);

        $this->queue(new InterleavingClient(new ArrayKvClient), required: true)->pushRaw('{"id":"a"}');
    }

    /**
     * A refused token or a dead connection is not an answer about evictions.
     * Reading it as "this server cannot report" would, for a watch that does
     * not require the count, quietly switch the check off.
     */
    public function test_a_failure_to_answer_is_never_read_as_nothing_to_report(): void
    {
        $queue = $this->queue(self::olderServer(new ArrayKvClient, Status::UNAUTHORIZED), required: false);

        try {
            $queue->pushRaw('{"id":"a"}');
            $this->fail('an unauthorised metrics read was swallowed');
        } catch (ServerException $exception) {
            $this->assertTrue($exception->isUnauthorized());
        }
    }

    /**
     * Laravel's worker catches the refusal and pops again a moment later. The
     * check used to stamp its interval before throwing, so the very next call
     * - inside the interval - went through, and the worker took jobs from a
     * node that had evicted. A refusal for evictions now stands.
     */
    public function test_a_refusal_for_evictions_stands_for_every_later_operation(): void
    {
        $store = new ArrayKvClient;
        $store->simulateLiveEvictions(5);
        $queue = $this->queue($store, every: 60);

        foreach (['push', 'push again', 'pop', 'push after the interval'] as $attempt) {
            if ($attempt === 'push after the interval') {
                Carbon::setTestNow(Carbon::now()->addMinutes(5));
            }

            try {
                $attempt === 'pop' ? $queue->pop() : $queue->pushRaw('{"id":"a"}');
                $this->fail("the {$attempt} went through on a node that had evicted live records");
            } catch (UnsafeQueueStore $exception) {
                $this->assertStringContainsString('evicted 5 live record', $exception->getMessage());
            }
        }

        $this->assertSame([], array_keys($store->all()), 'something was written after a refusal');
    }

    /**
     * The same one-shot hole, for a server that cannot report the count at
     * all: it was remembered as "unsupported" before the refusal was thrown,
     * and every later check returned early.
     */
    public function test_headroom_on_a_server_that_cannot_be_checked_is_refused_every_time(): void
    {
        $store = new ArrayKvClient;
        $queue = $this->queue(self::olderServer($store), required: true);

        foreach ([1, 2, 3] as $attempt) {
            try {
                $queue->pushRaw('{"id":"a"}');
                $this->fail("push {$attempt} went through without the count headroom requires");
            } catch (UnsafeQueueStore) {
            }
        }

        $this->assertSame([], array_keys($store->all()));
    }

    /**
     * Not being able to read the count is asked again on the next operation,
     * not remembered: one failed answer must not refuse a worker for good.
     */
    public function test_a_count_that_could_not_be_read_once_is_read_again_next_time(): void
    {
        $store = new ArrayKvClient;
        $flaky = new class($store) extends InterleavingClient implements ReportsKvMetrics
        {
            public int $calls = 0;

            public function kvMetrics(): KvMetrics
            {
                if (++$this->calls === 1) {
                    throw new ServerException(Status::ERROR, 'internal error', '__kv_metrics__');
                }

                return $this->inner->kvMetrics();
            }
        };
        $queue = $this->queue($flaky, required: true);

        try {
            $queue->pushRaw('{"id":"a"}');
            $this->fail('headroom ran without the count');
        } catch (UnsafeQueueStore) {
        }

        $queue->pushRaw('{"id":"b"}');

        $this->assertSame(2, $flaky->calls);
        $this->assertSame(1, $queue->size());
    }
}
