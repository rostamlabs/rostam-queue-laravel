<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Chaos;

use Illuminate\Container\Container;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
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
 * saw accepted was handed to a worker at least once, and nothing is left behind.
 * Duplicates are counted and reported, not failed - at-least-once allows them.
 *
 * It needs a real server (ROSTAM_TEST_SERVER) and takes about a minute, so it
 * runs in its own group: `vendor/bin/phpunit --group chaos`.
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

            if ($producing === [] && count(array_diff($produced, $this->logged('taken'))) === 0) {
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
                $drained[] = json_decode($job->getRawBody(), true)['id'];
                $job->delete();
            }
            sleep(3);
        }

        $produced = array_unique($this->logged('produced'));
        $taken = array_merge($this->logged('taken'), $drained);
        $missing = array_values(array_diff($produced, $taken));
        $duplicates = count($taken) - count(array_unique($taken));

        fwrite(STDERR, sprintf(
            "[chaos] produced %d, deliveries %d (%d duplicate), workers killed %d, drained at the end %d, missing %d\n",
            count($produced), count($taken), $duplicates, $kills, count($drained), count($missing),
        ));

        // A child's own failure - a fatal, a refused connection - lands in its
        // stderr, and without it a failure here would blame the queue for it.
        $childErrors = trim((string) @file_get_contents($this->dir.'/stderr.log'));
        $context = $childErrors === '' ? '' : "\nchild stderr:\n".substr($childErrors, -2000);

        $this->assertSame(self::PRODUCERS * self::JOBS_PER_PRODUCER, count($produced), 'a producer did not finish'.$context);
        $this->assertSame([], $missing, 'accepted jobs were never delivered'.$context);
        $this->assertNull($final->pop(), 'jobs were still waiting after the final drain'.$context);
    }
}
