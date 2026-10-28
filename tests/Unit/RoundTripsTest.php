<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use Illuminate\Container\Container;
use Illuminate\Support\Carbon;
use PHPUnit\Framework\TestCase;
use Rostam\Queue\RostamQueue;
use Rostam\Queue\Tests\Support\InterleavingClient;
use Rostam\Testing\ArrayKvClient;

/**
 * What the queue's common operations cost on the wire, pinned.
 *
 * Every KvClient call is one round trip - a batch call included, since the
 * client pipelines it - so the client's log is the count. Each figure here was
 * reached by changing the code and is held by this test: a change that brings
 * a sequential round trip back fails it rather than slipping past as a
 * slowdown nobody measures.
 */
class RoundTripsTest extends TestCase
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

    private function queue(string $owner = 'worker'): RostamQueue
    {
        $queue = new RostamQueue($this->client, 'q:', 'default', retryAfter: 30, owner: $owner);
        $queue->setContainer(new Container);
        $queue->setConnectionName('rostam');

        return $queue;
    }

    /**
     * The ops one call made, as [op, key] pairs.
     *
     * @return list<array{0: string, 1: string}>
     */
    private function opsOf(callable $call): array
    {
        $this->client->log = [];
        $call();

        return $this->client->log;
    }

    public function test_size_costs_two_round_trips(): void
    {
        $queue = $this->queue();
        $queue->pushRaw('{"id":"a"}');
        $queue->laterRaw(60, '{"id":"b"}');

        $ops = $this->opsOf(fn () => $this->assertSame(2, $queue->size()));

        $this->assertSame(['getMany', 'get'], array_column($ops, 0));
    }

    public function test_pending_size_costs_one_round_trip(): void
    {
        $queue = $this->queue();
        $queue->pushRaw('{"id":"a"}');

        $ops = $this->opsOf(fn () => $this->assertSame(1, $queue->pendingSize()));

        $this->assertSame(['getMany'], array_column($ops, 0));
    }

    public function test_claiming_a_waiting_job_costs_four_round_trips(): void
    {
        $queue = $this->queue();
        $queue->pop();                                   // pins the sweep watermark
        $queue->pushRaw('{"id":"a"}');

        $ops = $this->opsOf(fn () => $this->assertNotNull($queue->pop()));

        // The sweep's watermark and the redelivery cursor first - nothing to
        // do for either here - then the claim itself: reader and writer
        // together, the lease, the payload under it, the reader moved on.
        $this->assertSame(
            ['get', 'get', 'get', 'getMany', 'setNx', 'get', 'cas'],
            array_column($ops, 0),
        );
        $this->assertStringContainsString(':lease:1', $ops[4][1]);
        $this->assertStringContainsString(':job:1', $ops[5][1]);
    }

    public function test_an_empty_queue_answers_without_touching_a_slot(): void
    {
        $queue = $this->queue();
        $queue->pop();

        $ops = $this->opsOf(fn () => $this->assertNull($queue->pop()));

        $this->assertSame(['get', 'get', 'get', 'getMany'], array_column($ops, 0));
    }

    public function test_the_redelivery_cursor_crosses_a_finished_run_in_one_move(): void
    {
        $queue = $this->queue();
        $queue->pop();

        for ($i = 0; $i < 32; $i++) {
            $queue->pushRaw('{"id":"'.$i.'"}');
        }

        $held = [];

        for ($i = 0; $i < 32; $i++) {
            $held[] = $queue->pop();
        }

        foreach ($held as $job) {
            $job->delete();
        }

        $ops = $this->opsOf(fn () => $queue->pop());
        $moves = array_filter($ops, static fn (array $op) => $op[0] === 'cas' && str_ends_with($op[1], ':reclaim'));

        $this->assertCount(1, $moves, '32 finished slots must not cost 32 compare-and-swaps');
        $this->assertSame(32, unpack('J', (string) $this->store->get('q:default:reclaim'))[1]);
    }

    public function test_an_idle_minute_is_swept_in_one_round_trip_per_second(): void
    {
        $queue = $this->queue();
        $queue->pop();

        Carbon::setTestNow(Carbon::createFromTimestamp(1_800_000_000 + 60));

        $ops = $this->opsOf(fn () => $queue->pop());
        $seals = array_filter($ops, static fn (array $op) => $op[0] === 'setNx' && str_contains($op[1], ':d:'));
        $sealReads = array_filter($ops, static fn (array $op) => in_array($op[0], ['get', 'cas'], true) && str_contains($op[1], ':d:'));
        $cleanups = array_filter($ops, static fn (array $op) => $op[0] === 'delMany');

        $this->assertCount(60, $seals, 'each empty second is sealed with one op');
        $this->assertSame([], array_values($sealReads), 'an empty second needs no read and no compare-and-swap');
        $this->assertCount(1, $cleanups, 'the counters go in one round trip, not one per second');
        $this->assertLessThanOrEqual(60 + 8, count($ops));
    }

    public function test_a_second_a_push_reached_first_is_still_sealed_around_it(): void
    {
        $queue = $this->queue();
        $queue->pop();
        $queue->laterRaw(1, '{"id":"due"}');

        Carbon::setTestNow(Carbon::createFromTimestamp(1_800_000_000 + 2));

        $job = $queue->pop();

        $this->assertNotNull($job);
        $this->assertSame('{"id":"due"}', $job->getRawBody());
    }
}
