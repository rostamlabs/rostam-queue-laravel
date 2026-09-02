# rostam-queue-laravel

A Laravel queue driver for [Rostam](https://github.com/rostamlabs/rostam), built on
its key-value engine over the native binary TCP protocol.

## What you are actually getting

Read this part before the installation instructions.

**At-least-once delivery.** A job is handed to exactly one worker at a time and is
not lost if that worker dies — but a worker that is merely *slow*, slow enough for
its lease to lapse, can have the same job handed to somebody else while it is still
running. **Your handlers must be idempotent.** This is the same guarantee the Redis
and database drivers give, for the same reason, and it is not a limitation of this
engine.

**`retry_after` must exceed your longest job.** It is the lease: hold a job longer
than that and the queue assumes you died. Set it too low and work runs twice; set it
too high and a genuinely dead worker's job waits that long to come back.

**The server must not be configured to evict.** Rostam's default `AtCapPolicy` is
`PolicyRingbufEvict`, which at capacity overwrites the oldest entries — write order,
not LRU. A queued job is written once and read once, which is exactly the shape
eviction reaches first, and losing one is not a cache miss but work that was
accepted and never done.

Nothing on the wire reports the server's policy, so this driver **cannot check it**
and will not pretend to. It requires you to declare it and refuses to start
otherwise:

```php
'at_cap_policy' => 'reject_writes',
```

That is a promise you make, not a check that was performed. What the driver *can*
see is the damage afterwards — the ids are dense, so a run of holes where jobs
should be is unmistakable, and it throws rather than quietly skipping.

**Nothing may flush the server.** Rostam v0.6.0 added a `flush` op, and it is not
Redis's `FLUSHDB`: it has no unit smaller than the whole keyspace, so one call
destroys every queued job along with everything else on that server. Measured
against v0.6.0, a `flush` sent carrying the key `app:` still removed `session:b`
— the argument scopes nothing.

That is reachable by accident rather than only by malice: the Rostam **cache**
driver can be configured with `'flush' => 'server'`, and then an ordinary
`php artisan cache:clear` issues exactly this op. Jobs the queue had already
accepted disappear, and no worker ever learns they existed. Keep the queue on a
server nothing flushes, or leave the cache driver on its default generational
flush, which does not touch these keys.

Like the eviction policy above, this driver **cannot detect it**: nothing on the
wire reports that a flush happened, and a wiped queue is indistinguishable from
an empty one.

## Requirements

- PHP 8.2+
- Laravel 12 or 13
- **Rostam v0.5.0 or newer**, started with a `-tcp` listener and
  `PolicyRejectWrites`

## Install

```bash
composer require rostamlabs/rostam-queue-laravel
```

`config/queue.php`:

```php
'rostam' => [
    'driver'        => 'rostam',
    'connection'    => 'default',        // an entry under rostam.connections
    'queue'         => 'default',
    'retry_after'   => 90,               // the lease; longer than your longest job
    'at_cap_policy' => 'reject_writes',  // your declaration; the driver cannot verify it
],
```

The server itself is described once, under `rostam.connections`, shared with
[`rostamlabs/rostam-cache-laravel`](https://github.com/rostamlabs/rostam-cache-laravel)
if you use it. A `host`/`port` given inline here wins over that.

## How it works

Rostam's key-value engine has no list and no sorted set, so the queue is a dense id
space with two server-side counters:

```
{prefix}{queue}:tail        allocates the next id to write
{prefix}{queue}:head        the next id to read
{prefix}{queue}:job:{id}    the payload, with no TTL — a job must not expire
{prefix}{queue}:lease:{id}  who holds it, with a TTL — the engine expires it
{prefix}{queue}:reclaim     how far redelivery has swept
```

**Claiming takes a lease, not the job.** The payload stays put for as long as a
worker holds it, so a worker that dies does not take the work with it. That gives
three states, readable from two keys and no scan at all:

| payload | lease | means |
| --- | --- | --- |
| present | present | still being worked on |
| present | absent | the worker died — redeliver |
| absent | — | finished |

The middle row is what every other driver needs a reservation index for — a sorted
set in Redis, a column in the database — and finding expired reservations normally
needs a scan, which this engine does not have. It does not need one: **the ids are
dense, so walking them is the index.**

**The reader advances by compare-and-swap, never by increment.** Drawing from the
cursor with an unconditional increment is tempting — one op, perfectly distributed —
and wrong. Two workers examining the same slot would both advance it, and the very
next job would be stepped over without being handed to anyone.

**Delayed jobs wait in per-second buckets.** `later()` puts a job in the bucket for
the second it comes due, and each `pop` migrates the buckets between where it last
swept and now — a handful of keys in steady state, instead of the sorted set this
engine does not have. A queue idle for a long time catches up over several pops
rather than sweeping a day of buckets in one.

## Monitoring

```php
$queue->size();               // waiting, delayed included
$queue->pendingSize();        // ready right now
$queue->delayedSize();        // waiting for their second
$queue->reservedSize();       // always 0 — see below
$queue->redeliveredCount();   // jobs handed back after a worker died
$queue->holesSeen();          // empty slots stepped over; non-zero means eviction
```

`reservedSize()` is always zero, and that is a statement rather than a stub. A
reserved job is one a worker holds but has not finished; reporting them needs an
index that can be counted, and here the only record is a lease key whose id nobody
is tracking centrally. A number this driver cannot know would be worse than none —
`queue:monitor` would show a reassuring zero either way, but only one of them is
honest about why.

## Testing

```bash
composer install
composer test
```

The suite includes a worker actually being killed mid-job: a child process claims
work over a real socket and exits without finishing it, and the test asserts both
halves of the guarantee — that nothing is handed out while the dead worker's lease
is still live, and that the job comes back once it lapses.

## License

Apache-2.0, the same licence as [Rostam](https://github.com/rostamlabs/rostam)
itself — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
