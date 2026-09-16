<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Chaos;

use Illuminate\Container\Container;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use Rostam\Exceptions\ServerException;
use Rostam\Kv\TcpClient;
use Rostam\Queue\RostamQueue;

/**
 * The claim itself, against the kind of failure no hand-written interleaving
 * covers: many processes, and a parent killing workers at arbitrary instants.
 *
 * The interleaving tests pin every race that has been found. This looks for the
 * ones that have not. Producers enqueue through every path - ready, delayed,
 * due in the past - while workers take, finish, release, or die holding a job;
 * on top of that, the parent terminates a random worker every few hundred
 * milliseconds, wherever it happens to be, including between two round trips of
 * a single push, pop, migration or redelivery. Then every lease is allowed to
 * lapse and a last worker drains what is left.
 *
 * The assertion is the queue's promise and nothing weaker: every job a producer
 * saw accepted was FINISHED - deleted by a worker that handled it - at least
 * once, and nothing is left behind. Taken is not enough: a job handed out and
 * then dropped by a release or a redelivery that lost it would pass a check
 * that only asks whether it was ever handed out. Duplicates are counted and
 * reported, not failed - at-least-once allows them.
 *
 * It needs a real server (ROSTAM_TEST_SERVER) and takes about half a minute.
 * It is outside the default suites, so name the file:
 * `vendor/bin/phpunit tests/Chaos/ChaosTest.php`.
 */
#[Group('chaos')]
class ChaosTest extends TestCase
{
    private const PRODUCERS = 3;

    private const JOBS_PER_PRODUCER = 200;

    private const WORKERS = 4;

    private string $dir;

    protected function setUp(): void
    {
        parent::setUp();

        $target = getenv('ROSTAM_TEST_SERVER');

        if (! is_string($target) || $target === '') {
            $this->markTestSkipped('the chaos test needs a real server: set ROSTAM_TEST_SERVER');
        }

        $this->dir = sys_get_temp_dir().'/rostam-chaos-'.bin2hex(random_bytes(4));
        mkdir($this->dir);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir.'/*') ?: [] as $file) {
            @unlink($file);
        }

        @rmdir($this->dir);

        parent::tearDown();
    }

    /**
     * @return array{0: resource, 1: array<int, resource>}
     */
    private function spawn(array $args): array
    {
        $process = proc_open(
            array_merge([PHP_BINARY, __DIR__.'/chaos-worker.php'], $args),
            [1 => ['file', $this->dir.'/stdout.log', 'a'], 2 => ['file', $this->dir.'/stderr.log', 'a']],
            $pipes,
        );

        $this->assertIsResource($process, 'could not start a chaos process');

        return [$process, $pipes];
    }

    /**
     * @return list<string> every complete line of every log whose name starts with $kind
     */
    private function logged(string $kind): array
    {
        $lines = [];

        foreach (glob($this->dir.'/'.$kind.'-*.log') ?: [] as $file) {
            $content = (string) file_get_contents($file);

            // A process killed mid-append leaves a partial last line without
            // its newline; only complete lines count.
            $complete = substr($content, 0, (int) strrpos($content, "\n"));

            foreach (explode("\n", $complete) as $line) {
                if ($line !== '') {
                    $lines[] = $line;
                }
            }
        }

        return $lines;
    }

    public function test_every_accepted_job_is_delivered_while_workers_are_killed_at_random(): void
    {
        $target = (string) getenv('ROSTAM_TEST_SERVER');
        $prefix = 'chaos:'.bin2hex(random_bytes(4)).':';
        $stop = $this->dir.'/stop';

        // The parent's own choices - which worker dies, and when - replay from
        // this seed. The workers roll their own dice.
        $seed = (int) (getenv('CHAOS_SEED') ?: random_int(1, mt_getrandmax()));
        mt_srand($seed);
        fwrite(STDERR, "\n[chaos] seed {$seed} (replay with CHAOS_SEED={$seed})\n");

        $evictedBefore = $this->evictionsLive($target);

        $producers = [];
        for ($p = 1; $p <= self::PRODUCERS; $p++) {
            $producers[] = $this->spawn(['produce', $target, $prefix, $this->dir, 'p'.$p, (string) self::JOBS_PER_PRODUCER]);
        }

        $workers = [];
        $spawned = 0;
        for ($w = 1; $w <= self::WORKERS; $w++) {
            $workers[$w] = $this->spawn(['work', $target, $prefix, $this->dir, 'w'.(++$spawned), $stop]);
        }

        $kills = 0;
        $deadline = microtime(true) + 90;

        while (microtime(true) < $deadline) {
            usleep(mt_rand(100_000, 400_000));

            // A worker that exited on its own (the 12% that "die holding a job")
            // is replaced; one picked at random is killed wherever it stands.
            foreach ($workers as $slot => [$process]) {
                if (! proc_get_status($process)['running']) {
                    proc_close($process);
                    $workers[$slot] = $this->spawn(['work', $target, $prefix, $this->dir, 'w'.(++$spawned), $stop]);
                }
            }

            $victim = array_rand($workers);
            proc_terminate($workers[$victim][0]);
            proc_close($workers[$victim][0]);
            $workers[$victim] = $this->spawn(['work', $target, $prefix, $this->dir, 'w'.(++$spawned), $stop]);
            $kills++;

            $producing = array_filter($producers, static fn ($p) => proc_get_status($p[0])['running']);
            $produced = array_unique($this->logged('produced'));

            if ($producing === [] && $this->neverHandled($target, $prefix, $produced) === []) {
                break;
            }
        }

        touch($stop);

        foreach (array_merge($producers, array_values($workers)) as [$process]) {
            $status = proc_get_status($process);
            if ($status['running']) {
                proc_terminate($process);
            }
            proc_close($process);
        }

        // Every lease lapses (retry_after is 2s), then one last, unkilled worker
        // takes whatever is still owed.
        sleep(3);

        [$host, $port] = explode(':', $target);
        $final = new RostamQueue(TcpClient::fromArray(['host' => $host, 'port' => (int) $port]), $prefix, 'default', retryAfter: 2);
        $final->setContainer(new Container);
        $final->setConnectionName('rostam');

        $drained = [];
        for ($round = 0; $round < 5; $round++) {
            while ($job = $final->pop()) {
                $id = json_decode($job->getRawBody(), true)['id'];
                $final->getClient()->put($prefix.'handled:'.$id, '1', 3600);
                $job->delete();
                $drained[] = $id;
            }
            sleep(3);
        }

        $produced = array_unique($this->logged('produced'));
        $taken = array_merge($this->logged('taken'), $drained);
        $missing = $this->neverHandled($target, $prefix, $produced);
        $evictedNow = $this->evictionsLive($target);
        $evicted = ($evictedNow ?? 0) - ($evictedBefore ?? 0);

        fwrite(STDERR, sprintf(
            "[chaos] produced %d, handed out %d (%d more than once), workers killed %d, drained at the end %d, never handled %d\n",
            count($produced), count($taken), count($taken) - count(array_unique($taken)), $kills, count($drained), count($missing),
        ));

        // A child's own failure - a fatal, a refused connection - lands in its
        // stderr, and without it a failure here would blame the queue for it.
        $childErrors = trim((string) @file_get_contents($this->dir.'/stderr.log'));
        $context = $childErrors === '' ? '' : "\nchild stderr:\n".substr($childErrors, -2000);

        // A server that threw records away during the run has already broken the
        // premise: this test asks whether the QUEUE loses jobs, and the driver
        // refuses to run on such a node in the first place (the chaos workers
        // deliberately have no such guard). Saying so beats reporting the
        // server's loss as the queue's.
        if ($evicted > 0) {
            $this->markTestSkipped(
                "the server evicted {$evicted} live record(s) while this test ran, so nothing here is about the "
                .'queue. Give it more memory, start it with -relocating-eviction, or point the test at an emptier node.'
            );
        }

        if ($missing !== []) {
            // On a server too old to count evictions there is no way to tell the
            // engine's loss from the queue's, and saying nothing would let this
            // read as the queue's every time.
            if ($evictedNow === null) {
                $context .= "\nthis server cannot report evictions (rostam_kv_evictions_live_total arrived in "
                    .'v0.7.0-beta3), so a loss here may be the engine throwing records away rather than the queue.';
            }

            $context .= "\n".$this->whereTheJobsAre($target, $prefix, $missing);
        }

        $this->assertSame(self::PRODUCERS * self::JOBS_PER_PRODUCER, count($produced), 'a producer did not finish'.$context);
        $this->assertSame([], $missing, 'accepted jobs were never handled'.$context);
        $this->assertNull($final->pop(), 'jobs were still waiting after the final drain'.$context);
    }

    /**
     * Live records the node says it has thrown away, or null from a server too
     * old to count them - where a run cannot tell the engine's loss from the
     * queue's, and has to say so rather than pick one.
     */
    private function evictionsLive(string $target): ?int
    {
        [$host, $port] = explode(':', $target);

        try {
            return TcpClient::fromArray(['host' => $host, 'port' => (int) $port])->kvMetrics()->evictionsLive();
        } catch (ServerException) {
            // The generic error, which is what a server without the op answers.
            return null;
        }
    }

    /**
     * The produced jobs no worker has marked handled.
     *
     * The mark is written to the store by the worker holding the job, before it
     * deletes it, so it survives that worker being killed a moment later - which
     * a line appended to a log file does not.
     *
     * @param  list<string>  $produced
     * @return list<string>
     */
    private function neverHandled(string $target, string $prefix, array $produced): array
    {
        [$host, $port] = explode(':', $target);
        $client = TcpClient::fromArray(['host' => $host, 'port' => (int) $port]);
        $missing = [];

        foreach (array_chunk($produced, 200) as $chunk) {
            $keys = array_map(static fn (string $id) => $prefix.'handled:'.$id, $chunk);

            foreach ($client->getMany($keys) as $key => $value) {
                if ($value === null) {
                    $missing[] = substr($key, strlen($prefix.'handled:'));
                }
            }
        }

        return $missing;
    }

    /**
     * Where a job that was never finished actually is, for a failure that has
     * to be diagnosed rather than re-run: the cursors, and every key still
     * holding one of the missing payloads.
     *
     * @param  list<string>  $missing
     */
    private function whereTheJobsAre(string $target, string $prefix, array $missing): string
    {
        [$host, $port] = explode(':', $target);
        $client = TcpClient::fromArray(['host' => $host, 'port' => (int) $port]);

        $counter = static function (string $key) use ($client): int {
            $raw = $client->get($key);

            return ($raw !== null && strlen($raw) === 8) ? unpack('J', $raw)[1] : 0;
        };

        $head = $counter($prefix.'default:head');
        $tail = $counter($prefix.'default:tail');
        $swept = $counter($prefix.'default:swept');
        $report = [sprintf(
            'head=%d tail=%d reclaim=%d swept=%d (now %d) dgen=%d missing=%s',
            $head, $tail, $counter($prefix.'default:reclaim'), $swept, time(),
            $counter($prefix.'default:dgen'), implode(',', $missing),
        )];

        // Every ready slot, and every delayed bucket from two minutes before the
        // watermark to a little after now: a job that is still somewhere is in
        // one of them, and one that is in none of them is gone from the store.
        $keys = [];

        for ($id = 1; $id <= $tail; $id++) {
            $keys[] = $prefix.'default:job:'.$id;
        }

        for ($second = $swept - 120; $second <= time() + 5; $second++) {
            $ids = $counter($prefix.'default:d:'.$second.':tail') & ~RostamQueue::SEALED;

            for ($id = 1; $id <= $ids; $id++) {
                $keys[] = $prefix.'default:d:'.$second.':job:'.$id;
            }
        }

        foreach (array_chunk($keys, 200) as $chunk) {
            foreach ($client->getMany($chunk) as $key => $value) {
                if ($value === null || $value === RostamQueue::TOMBSTONE) {
                    continue;
                }

                foreach ($missing as $id) {
                    if (str_contains($value, '"'.$id.'"')) {
                        $report[] = $id.' is still at '.$key.' (lease: '
                            .var_export($client->get(str_replace(':job:', ':lease:', $key)), true).')';
                    }
                }
            }
        }

        return implode("\n", $report);
    }
}
