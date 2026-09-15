# rostam-queue-laravel

A Laravel queue driver for [Rostam](https://github.com/rostamlabs/rostam), built on
its key-value engine over the native binary TCP protocol.

## What you are actually getting

Read this part before the installation instructions.

**At-least-once delivery.** A job the queue accepted is delivered, including when
the worker holding it dies. A worker that is merely *slow* — slow enough for its
lease to lapse — can have the same job handed to somebody else while it is still
running, so **your handlers must be idempotent**. This is the same guarantee the
Redis and database drivers give, for the same reason.

A worker dying while it holds a job counts as an attempt, so a job that crashes
its worker every time reaches `maxTries` and is failed, instead of taking workers
down forever.

**`retry_after` must exceed your longest job.** It is the lease: hold a job longer
than that and the queue assumes you died. Set it too low and work runs twice; set it
too high and a genuinely dead worker's job waits that long to come back.

### A node that evicts loses jobs — and a single node always evicts

At capacity a Rostam node either **evicts** records or **refuses** the write, and
which one is decided by topology, not by any flag:

- a single-node `rostam-server` **always evicts**, silently — every write still
  answers success — and nothing in its flags or config changes that;
- only replicated shards (`-cluster`) refuse writes instead.

Measured on v0.7.0-beta6, a single node with a 256 MiB budget: 400 one-megabyte
writes all succeeded, **235 read back**, and the node's own counter said 165 live
records had been evicted. For a cache that is a miss rate. For a queue it is work
that was accepted and never done.

Which setup a server is cannot be read off the wire, so the connection declares it,
and the driver refuses to start without the declaration:

```php
'at_cap_policy' => 'reject_writes',   // a -cluster: at capacity, pushes fail loudly
'at_cap_policy' => 'headroom',        // a single node whose max_memory stays well above the backlog
```

What **can** be read — on rostam **v0.7.0-beta3 and newer** — is whether a node has
already evicted live records (`rostam_kv_evictions_live_total`). The driver reads it
before the first job is accepted or taken, and again every `verify_every` seconds,
and **refuses to run on a node that has evicted any**. Be clear about what that is:

- it **catches a false declaration**, it never proves a true one — zero before a
  node ever fills up says nothing about the day it does;
- it is **node-wide**: evictions of anyone's keys on that node count, not only jobs,
  which is right, because on a node declared never to evict, any eviction
  contradicts the declaration;
- the count **resets when the server restarts**, and restarting to clear it does not
  bring back what was lost.

`headroom` has nothing else standing between a full node and silent loss, so it is
**refused on a server that cannot report the count**. `reject_writes` runs on the
declaration alone against an older server, because there the node itself refuses.

### Nothing may flush the server

Rostam v0.6.0 added a `flush` op, and it is not Redis's `FLUSHDB`: it has no unit
smaller than the whole keyspace, so one call destroys every queued job along with
everything else on that server — including this queue's own cursors. Measured
against v0.6.0, a `flush` sent carrying the key `app:` still removed `session:b`.

That is reachable by accident: **`rostamlabs/rostam-cache-laravel` v0.2.0 and newer**
can be configured with `'flush' => 'server'`, and then an ordinary
`php artisan cache:clear` issues exactly this op. Every job that was waiting — not
yet taken by a worker — disappears, and nothing reports it; a worker already running
a job keeps its copy and finishes it. Keep the queue on a server nothing flushes, or
leave the cache driver on its default generational flush, which does not touch
these keys.

### How large a job can be

A job's payload is one value, and a value has to fit in one page of the server's
cache. The page size follows from `max_memory` spread across the shards, and on a
default single-node server it is small: the largest value stored was **about 1 MiB**
on v0.6.0 and v0.7.0-beta6. A larger job fails at `push` with the server's generic
`internal error`. Keep payloads small — pass ids, not models — or give the server
fewer shards or more memory.

## Requirements

- PHP 8.2+
- Laravel 12 or 13
- **Rostam v0.5.0 or newer**, started with a `-tcp` listener
- **Rostam v0.7.0-beta3 or newer** for eviction checks — required for `headroom`
- `rostamlabs/rostam-client-php` ^0.3

## Install

```bash
composer require rostamlabs/rostam-queue-laravel
```

`config/queue.php`:

```php
'rostam' => [
    'driver'        => 'rostam',
    'connection'    => 'default',     // an entry under rostam.connections
    'queue'         => 'default',
    'retry_after'   => 90,            // the lease; longer than your longest job
    'at_cap_policy' => 'headroom',    // or 'reject_writes' — see above; required
    'verify_every'  => 60,            // seconds between eviction checks
    'sweep_seconds' => 60,            // delayed seconds one pop may move
    'tombstone_ttl' => 604800,        // how long a killed slot stays killed
],
```

The server itself is described once, under `rostam.connections`, shared with
[`rostamlabs/rostam-cache-laravel`](https://github.com/rostamlabs/rostam-cache-laravel)
if you use it. A `host`/`port` given inline here wins over that.

## How it works

Rostam's key-value engine has no list and no sorted set, so the queue is a dense id
space with server-side counters:

```
{prefix}{queue}:tail                allocates the next id to write
{prefix}{queue}:head                the next id to read
{prefix}{queue}:job:{id}            the payload (no TTL), or a tombstone
{prefix}{queue}:lease:{id}          who holds it, with a TTL the engine expires
{prefix}{queue}:reclaim             how far redelivery has swept
{prefix}{queue}:d:{second}:tail     ids handed out for a delayed second; sealed once swept
{prefix}{queue}:d:{second}:job:{id} a delayed job, or a tombstone
{prefix}{queue}:swept               the last delayed second fully moved
{prefix}{queue}:delayed             a gauge of delayed jobs waiting
```

**Claiming takes a lease, not the job.** The payload stays put for as long as a
worker holds it, so a worker that dies does not take the work with it:

| payload | lease | means |
| --- | --- | --- |
| present | present | still being worked on |
| present | absent | the worker died — redeliver |
| absent or tombstone | — | finished, or a slot that was killed |

The middle row is what every other driver needs a reservation index for, and finding
expired reservations normally needs a scan this engine does not have. It does not
need one: **the ids are dense, so walking them is the index.**

**Every step that decides something is one atomic op.** Each job this driver has
ever lost was lost in the gap between two round trips, and each gap is closed by
deciding with a single op rather than checking and then acting — a check always
leaves a window between reading and acting.

- **A push draws its id, then writes its payload.** A worker reaching that slot in
  between finds it empty. It does not step over it: it kills the slot with `set_nx`
  — a tombstone — and the push writes with `set_nx` too, so exactly one of them owns
  the slot. If the worker won, the push draws another id.
- **The reader advances by compare-and-swap, never by increment.** Two workers
  examining one slot would otherwise both advance, and the next job would be stepped
  over without being handed to anyone.
- **Redelivery is claimed with the job's own lease** before the job is put back, so
  two workers passing one abandoned job do not both requeue it — and the copy is
  written before the original is removed, so a worker dying in between leaves two,
  never none.
- **`release()` writes the retry before finishing the original**, for the same
  reason.

**Delayed jobs wait in per-second buckets**, and each `pop` moves the seconds that
have come due — a handful of keys in steady state, instead of the sorted set this
engine does not have. The same rules apply there:

- a second's id counter is **sealed** as the sweep passes it, and a push that draws
  a sealed id — or finds its slot killed — sends its job to the ready queue instead;
- a job due in the past goes straight to the ready queue;
- a delayed job is **copied, then deleted**, under a per-job lease, and a second is
  marked done only once every job in it has moved.

A queue idle for a long time catches up over several pops rather than sweeping a day
of seconds in one.

### The limits this does not cover

- **A push paused longer than `tombstone_ttl`** (a week by default) between drawing
  its id and writing its payload can land in a slot whose tombstone has expired, behind
  every cursor. A tombstone that never expired would keep every killed slot in memory
  for ever; this is the trade.
- **The server losing records** — eviction or a flush. See above.
- **Duplicates.** A crash between writing a copy and removing an original leaves
  both, and a slow worker's job may be redelivered. At-least-once allows both.

## Monitoring

```php
$queue->size();               // pending + delayed
$queue->pendingSize();        // between the reader and the writer — an upper bound
$queue->delayedSize();        // delayed jobs waiting, however far out
$queue->reservedSize();       // always 0 — see below
$queue->redeliveredCount();   // jobs this instance handed back after a worker died
$queue->holesSeen();          // empty slots this instance killed
```

`pendingSize()` counts every id between the reader and the writer, so jobs being
worked on and killed slots the reader has not yet passed are in it.

`delayedSize()` is a gauge kept beside the buckets, since the buckets cannot be
enumerated; a worker killed between storing a delayed job and counting it leaves it
off by one.

`holesSeen()` counts empty slots this instance killed and moved past. A push caught
mid-write leaves one and re-routes itself, so a steady trickle is normal. A **run**
of them in a single pop throws `JobVanished`: that is records written and then gone.

`reservedSize()` is always zero, and that is a statement rather than a stub. A
reserved job is one a worker holds but has not finished; the only record of that is a
lease key per job, and nothing on this engine can count keys by pattern. A number
this driver cannot know would be worse than none.

## Testing

```bash
composer install
composer test
```

Every race this driver has lost jobs to has a test that drives that exact
interleaving — a push caught between its id and its payload, a worker killed while
moving a delayed job, a sweep that finished a second before a push drew its id —
through a client that can stop the world between any two round trips. Each was
written red against the code that had the race, and each fix is checked by
reverting it.

Against a real server, a chaos test runs producers and workers as separate
processes and kills workers at random instants, then asserts that every job a
producer saw accepted was delivered:

```bash
ROSTAM_TEST_SERVER=127.0.0.1:7000 vendor/bin/phpunit tests/Chaos/ChaosTest.php
```

## License

Apache-2.0, the same licence as [Rostam](https://github.com/rostamlabs/rostam)
itself — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
