<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Support;

use Closure;
use Rostam\Contracts\KvClient;
use Rostam\TimeUnit;

/**
 * A KvClient that lets a test stop the world between any two operations.
 *
 * Every race in a queue lives in the gap between two round trips: a push that
 * has drawn its id but not yet written its payload, a migration that has read a
 * delayed job but not yet enqueued it. A single-threaded test cannot reach those
 * gaps on its own, so this wraps a real in-memory client and runs a hook just
 * before a chosen operation on a chosen key - a rival worker, or a worker that
 * dies right there.
 *
 * Unlike the in-memory fake's own write hook, which fires on `put` and `setNx`
 * only, this fires on EVERY operation. A queue's cursors move by compare-and-
 * swap, and an interleaving test whose hook never fires on `cas` passes by
 * testing nothing at all.
 *
 * Each hook runs once and is removed before it is called, so a hook that drives
 * the same queue again cannot re-enter itself. With no hooks it is a plain
 * pass-through, which a test can extend to change how one operation answers.
 */
class InterleavingClient implements KvClient
{
    /** @var list<array{0: string, 1: string}> every operation, in order: [op, key] */
    public array $log = [];

    /** @var array<int, array{0: string, 1: string, 2: Closure}> */
    private array $hooks = [];

    public function __construct(public readonly KvClient $inner) {}

    /**
     * Run $hook just before the next `$op` on a key containing $keyContains.
     *
     * `$op` may name several operations separated by `|`, so a test can say
     * "before this key is written" without depending on which write the code
     * under test happens to use - a race test tied to `put` would go quiet the
     * moment the code switched to `setNx`, and pass by testing nothing.
     */
    public function before(string $op, string $keyContains, Closure $hook): void
    {
        $this->hooks[] = [$op, $keyContains, $hook];
    }

    /**
     * Hooks that never ran, as "op on key" - for a test to assert there are
     * none. A hook that never fired opened no window, and the test around it
     * proved nothing: that is how a race test once passed against a cursor
     * with no compare in it.
     *
     * @return list<string>
     */
    public function pendingHooks(): array
    {
        return array_values(array_map(static fn (array $hook) => $hook[0].' on '.$hook[1], $this->hooks));
    }

    /**
     * A worker process that dies at this point: nothing after it in the
     * calling code runs.
     */
    public static function dies(): Closure
    {
        return static fn () => throw new WorkerDied;
    }

    private function fire(string $op, string $key): void
    {
        $this->log[] = [$op, $key];

        foreach ($this->hooks as $index => [$hookOp, $needle, $hook]) {
            if (in_array($op, explode('|', $hookOp), true) && str_contains($key, $needle)) {
                unset($this->hooks[$index]);
                $hook();

                return;
            }
        }
    }

    public function get(string $key): ?string
    {
        $this->fire('get', $key);

        return $this->inner->get($key);
    }

    public function getMany(array $keys): array
    {
        $this->fire('getMany', implode(',', $keys));

        return $this->inner->getMany($keys);
    }

    public function put(string $key, string $value, int $ttl = 0, TimeUnit $unit = TimeUnit::Seconds): void
    {
        $this->fire('put', $key);
        $this->inner->put($key, $value, $ttl, $unit);
    }

    public function putMany(array $entries, TimeUnit $unit = TimeUnit::Seconds): void
    {
        $this->fire('putMany', implode(',', array_column($entries, 0)));
        $this->inner->putMany($entries, $unit);
    }

    public function setNx(string $key, string $value, int $ttl = 0, TimeUnit $unit = TimeUnit::Seconds): bool
    {
        $this->fire('setNx', $key);

        return $this->inner->setNx($key, $value, $ttl, $unit);
    }

    public function cas(string $key, string $value, ?string $expected, int $ttl = 0, TimeUnit $unit = TimeUnit::Seconds): bool
    {
        $this->fire('cas', $key);

        return $this->inner->cas($key, $value, $expected, $ttl, $unit);
    }

    public function cad(string $key, string $expected): bool
    {
        $this->fire('cad', $key);

        return $this->inner->cad($key, $expected);
    }

    public function caex(string $key, string $expected, int $ttl, TimeUnit $unit = TimeUnit::Seconds): bool
    {
        $this->fire('caex', $key);

        return $this->inner->caex($key, $expected, $ttl, $unit);
    }

    public function getdel(string $key): ?string
    {
        $this->fire('getdel', $key);

        return $this->inner->getdel($key);
    }

    public function getset(string $key, string $value, int $ttl = 0, TimeUnit $unit = TimeUnit::Seconds): ?string
    {
        $this->fire('getset', $key);

        return $this->inner->getset($key, $value, $ttl, $unit);
    }

    public function exists(string $key): bool
    {
        $this->fire('exists', $key);

        return $this->inner->exists($key);
    }

    public function del(string $key): bool
    {
        $this->fire('del', $key);

        return $this->inner->del($key);
    }

    public function delMany(array $keys): array
    {
        $this->fire('delMany', implode(',', $keys));

        return $this->inner->delMany($keys);
    }

    public function increment(string $key, int $delta = 1, int $ttl = 0, TimeUnit $unit = TimeUnit::Seconds): int
    {
        $this->fire('increment', $key);

        return $this->inner->increment($key, $delta, $ttl, $unit);
    }

    public function expire(string $key, int $ttl, TimeUnit $unit = TimeUnit::Seconds): bool
    {
        $this->fire('expire', $key);

        return $this->inner->expire($key, $ttl, $unit);
    }

    public function persist(string $key): bool
    {
        $this->fire('persist', $key);

        return $this->inner->persist($key);
    }

    public function ttl(string $key, TimeUnit $unit = TimeUnit::Seconds): int
    {
        $this->fire('ttl', $key);

        return $this->inner->ttl($key, $unit);
    }

    public function flush(): void
    {
        $this->fire('flush', '');
        $this->inner->flush();
    }

    public function ping(): bool
    {
        return $this->inner->ping();
    }

    public function disconnect(): void
    {
        $this->inner->disconnect();
    }
}
