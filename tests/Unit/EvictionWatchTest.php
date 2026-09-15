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
     * "reject_writes" names a node that refuses at capacity. Its safety does
     * not depend on the count, so a server too old to report it runs on the
     * declaration alone.
     */
    public function test_a_declaration_that_does_not_need_the_count_runs_without_it(): void
    {
        $store = new ArrayKvClient;
        $queue = $this->queue(self::olderServer($store), required: false);

        $queue->pushRaw('{"id":"a"}');

        $this->assertNotNull($queue->pop());
    }

    /**
     * "headroom" names a single node that must simply never fill up. The count
     * is the only thing that would ever notice it did, so a server that cannot
     * give it is refused.
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
     * Reading it as "this server cannot report" would, under "reject_writes",
     * quietly switch the check off.
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
}
