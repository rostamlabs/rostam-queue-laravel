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

### The server has to keep every record — and by default it does not

A queue is only as durable as the node never throwing away a record it still holds.
A single-node `rostam-server` throws them away in two ways, both measured:

- **At capacity**, silently — every write still answers success. On v0.7.0-beta6
  with a 256 MiB budget on one shard, 400 writes of 1,000,000 bytes all succeeded
  and **235 read back**.
- **With room to spare**, by default. Eviction follows write order: a record that
  sits still while newer writes churn past it is evicted when the buffer wraps,
  however little else is live. On v0.7.0-beta7 with a 32 MiB budget, put-then-delete
  churn of ten times the budget **evicted both of two small keys** that were never
  touched again, with nothing else on the server. A queue is churn, and a delayed
  job, or a backlog nobody is working, is exactly a record that sits still.

Started with **`-relocating-eviction`**, the server copies a page's still-live
records forward instead of dropping them with it. The same churn left both keys in
place, and a first-in-first-out backlog churned eight times over held with **no
live evictions at a quarter of the budget** — while at half it evicted 1,714 live
records along the way.

It is best-effort by design, and the server says so: it never allocates a page,
never triggers another eviction and never fails a write, so a record that does not
fit the room left over is dropped like any other. That is why the flag is one of
three things this driver asks for, not the whole answer — the live set has to stay
well under the budget, and the eviction count is the backstop. The flag does
nothing under `-cluster`, which this driver does not support anyway. So:

```php
'at_cap_policy' => 'headroom',
```

declares the one setup this driver runs on, and the connector refuses to start
without it:

- **a single `rostam-server` started with `-relocating-eviction`**;
- **`max_memory` well above everything live on it** — the backlog at its worst, the
  delayed jobs, and anything else sharing the server (a quarter held in the
  measurement above; half did not);
- **rostam v0.7.0-beta3 or newer.**

The first two cannot be read off the wire — that part is your declaration. What
**can** be read is whether the node has already evicted live records
(`rostam_kv_evictions_live_total`). The driver reads it before the first job is
accepted or taken and again every `verify_every` seconds, and **refuses to run on a
node that has evicted any**. Be clear about what that is:

- it **catches a node that is losing records**, it never proves one will not;
- it is **node-wide**: evictions of anyone's keys count, not only jobs;
- once it has refused, **that worker stays refused until it is restarted** — the
  count cannot go back down, and a worker that caught the exception and popped again
  a moment later must not find the door open;
- a server that cannot report the count is refused on every operation;
- the count **resets when the server restarts**, and restarting to clear it does not
  bring back what was lost;
- all of that is what `on_evictions => 'refuse'` means, and it is the default;
  `'ignore'` skips the check entirely — see the configuration below for when that
  is a reasonable trade and what it costs.

### A replicated cluster is not supported

A `-cluster` refuses writes at capacity instead of evicting, which is what a queue
wants — but it answers a read from whichever replica received it, and this driver
reads a job's absence as that job being finished. A lagging replica would drop a
job. `'at_cap_policy' => 'reject_writes'` is refused with that explanation.

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

A job's payload is one value, and an entry has to fit in one page of the server's
cache — where the page bounds **the key and the value together**. The page follows
the PER-SHARD budget: `floorPow2(max_memory / shards / 16)`, clamped to 1 MiB…1 GiB.
On a default server that is the 1 MiB floor, and `strlen($key) + strlen($value)`
reached **1,048,550 bytes on v0.6.0** and **1,048,546 on v0.7.0-beta6 and beta7**,
constant across key lengths; the key here is the queue's own
(`{prefix}{queue}:job:{id}`), so nearly all of it is yours. Measured at other
geometries on beta7: 2,097,122 at 32 MiB on one shard, 4,194,274 at 256 MiB on
four, 8,388,578 at 128 MiB on one — about 30 bytes under the page each time. Most
deployments sit on the floor, so halving the shard count raises the limit where
raising `max_memory` alone does not. A larger job fails at `push` with the server's
generic `internal error`. Keep payloads small — pass ids, not models.

## Requirements

- PHP 8.2+
- Laravel 12 or 13
- **Rostam v0.7.0-beta3 or newer**, a single node, started with a `-tcp` listener
  and `-relocating-eviction`
- `rostamlabs/rostam-client-php` ^0.3

```bash
rostam-server -tcp 127.0.0.1:7000 -relocating-eviction -config rostam.json
```

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
    'at_cap_policy' => 'headroom',    // required - see above
    'on_evictions'  => 'refuse',      // or 'ignore' - see below
    'after_commit'  => false,         // Laravel's own option
    'verify_every'  => 60,            // seconds between eviction checks
    'sweep_seconds' => 60,            // delayed seconds one pop may move
    'reclaim_batch' => 32,            // ids one pop checks for jobs whose worker died
    'tombstone_ttl' => 604800,        // how long a killed slot stays killed
],
```

**`tombstone_ttl` must be longer than `retry_after`**, and the connector refuses a
pair that is not. A killed slot is what stops a push from landing in a slot the
queue has moved past, and a lease — including a dead worker's — lasts
`retry_after`; a tombstone that expires first lets a slot be re-used while its
lease is still held.

**`on_evictions`** decides what the eviction check does when it finds something.
`refuse` (the default) is the safe answer and an unforgiving one: the count is
node-wide, so a co-tenant's evicted cache key stops this queue too, and a worker
that has refused keeps refusing until it restarts — while the count itself only
resets when the *server* restarts, which on a node without `-data` takes the
backlog with it. Set it to `ignore` on a node whose eviction count is somebody
else's, and accept that this queue will not notice the day it loses a job.

The server itself is described once, under `rostam.connections`, shared with
[`rostamlabs/rostam-cache-laravel`](https://github.com/rostamlabs/rostam-cache-laravel)
if you use it. A `host`/`port` given inline here wins over that.

`php artisan queue:clear rostam` works, and queue names may be backed enums or
forwarded with Laravel's queue routes.

## How it works

Rostam's key-value engine has no list and no sorted set, so the queue is a dense id
space with server-side counters:

```
{prefix}{queue}:tail                allocates the next id to write
{prefix}{queue}:head                the next id to read
{prefix}{queue}:job:{id}            the payload (no TTL), or a tombstone
{prefix}{queue}:lease:{id}          who holds it, with a TTL the engine expires
{prefix}{queue}:reclaim             how far redelivery has settled
{prefix}{queue}:d:{second}:tail     ids handed out for a delayed second, sealed while it is swept
{prefix}{queue}:d:{second}:job:{id} a delayed job, or a tombstone
{prefix}{queue}:mig:{second}:{id}   the lease on moving one delayed job
{prefix}{queue}:swept               the last delayed second fully moved
{prefix}{queue}:dgen                the delayed generation, moved on by clear()
{prefix}{queue}:delayed:{gen}       a gauge of delayed jobs waiting in that generation
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
need one: **the ids are dense, so walking them is the index.** Each pop reads a
window of `reclaim_batch` ids in two round trips and looks past jobs still
running, so one long job does not hold back the abandoned ones above it *within
that window*. The cursor stops behind anything unsettled, so work more than
`reclaim_batch` ids above a job that is still running waits for it to finish.
Nothing is lost by waiting: the payload and its lapsed lease stay as they are.

**Every step that decides something is one atomic op.** Each job this driver has
ever lost was lost in the gap between two round trips, and each gap is closed by
deciding with a single op rather than checking and then acting.

- **A push draws its id, then writes its payload.** A worker reaching that slot in
  between finds it empty. It does not step over it: it kills the slot with `set_nx`
  — a tombstone — and the push writes with `set_nx` too, so exactly one of them owns
  the slot. If the worker won, the push draws another id.
- **The reader advances by compare-and-swap, never by increment.** Two workers
  examining one slot would otherwise both advance, and the next job would be stepped
  over without being handed to anyone.
- **Redelivery is claimed with the job's own lease**, and the job is read again
  under it — its worker may have finished late. The copy is written before the
  original is removed, so a worker dying in between leaves two, never none.
- **`release()` writes the retry before finishing the original**, for the same
  reason.
- **`clear()` overwrites each slot with a tombstone** instead of deleting it, so a
  push that drew one of those ids finds it killed and lands after the clear, where
  it is delivered. It starts at the redelivery cursor rather than the reader, so a
  job whose worker died — which sits *below* the reader, waiting to be handed back
  — is cleared and counted like any other, instead of reappearing minutes after the
  queue reported itself empty. It is the one place this driver overwrites a payload,
  which is what clearing a queue is; a job a worker is running finishes normally,
  since that worker already holds it. The walk costs two round trips per 512 ids.

**Delayed jobs wait in per-second buckets**, and each `pop` moves the seconds that
have come due. The same rules apply there:

- the sweep **seals** a second's id counter before counting it, so a push drawing an
  id meanwhile carries the seal and sends its job to the ready queue;
- a job due in the past goes straight to the ready queue;
- a push that stored its job **checks the watermark afterwards**, and if the sweep
  has passed its second, moves the job itself — so no pause of a producer, however
  long, strands a delayed job. If that move loses its lease to a worker that died
  mid-move, the push puts the job on the ready queue instead rather than trusting
  the lease holder to come back;
- a delayed job is **copied, then deleted**, under a per-job move lease, and read
  again under it; a second is marked done only once every job in it has moved, and
  its counter is deleted then;
- `clear()` moves the **delayed generation** on: a job stored under an older one is
  deleted instead of delivered when its second comes, and the generation is read
  under the move lease, so a job delayed after a clear is never taken for one it
  cleared.

A queue idle for a long time catches up over several pops rather than sweeping a day
of seconds in one.

### The limits this does not cover

- **A ready push paused longer than `tombstone_ttl`** (a week by default) between
  drawing its id and writing its payload can land in a slot whose tombstone has
  expired, behind every cursor. A tombstone that never expired would keep every
  killed slot in memory forever; this is the trade, and `tombstone_ttl => 0` takes
  the other side of it. A `clear()` passing over such a slot does not re-arm it:
  below the reader it kills only slots that hold something, which is what stops
  clearing an empty queue from writing a key per id.
- **The server losing records** — eviction or a flush. See above.
- **Duplicates.** A crash between writing a copy and removing an original leaves
  both, and a slow worker's job may be redelivered. At-least-once allows both.
- **A job enqueued while `clear()` is running** may land on either side of it.
- **Keys that stay.** The cursors (`head`, `tail`, `reclaim`, `swept`, `dgen`) and
  a delayed generation's gauge have no TTL — they are the queue. A worker killed
  between marking a delayed second done and deleting its counter leaves that one
  small key behind too.

## Monitoring

```php
$queue->size();               // pending + delayed
$queue->pendingSize();        // between the reader and the writer — an upper bound
$queue->delayedSize();        // delayed jobs waiting, however far out
$queue->reservedSize();       // always 0 — see below
$queue->redeliveredCount();   // jobs this instance handed back after a worker died
$queue->holesSeen();          // empty slots this instance killed and moved past
```

`pendingSize()` counts the ids between the reader and the writer: waiting jobs, plus
killed slots the reader has not yet passed. Jobs being worked on are not in it — the
reader has passed them.

`delayedSize()` is a gauge kept beside the buckets, since the buckets cannot be
enumerated; a worker killed between storing a delayed job and counting it leaves it
off by one.

`holesSeen()` counts empty slots this instance killed and moved past. A push caught
mid-write leaves one and re-routes itself, so a steady trickle is normal. A **run**
of 64 in a single pop throws `JobVanished`: that is records written and then gone.
Losing races to other workers never counts; a pop that loses every race returns
nothing, and the worker polls again.

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
moving a delayed job, a clear finishing under a sweep — through a client that can
stop the world between any two round trips. Each of those tests fails if its hook
never fires, and every fix is checked by reverting it and watching a test go red —
22 of them at the last count. Where two guards overlap by design (a delayed job is
both sealed out of a swept second and moved by its own push), reverting one alone
leaves the other holding, and the pair is checked together.

Against a real server, a chaos test runs producers and workers as separate
processes and kills workers at random instants, then asserts that every job a
producer saw accepted was **finished** by a worker — not merely handed out:

```bash
ROSTAM_TEST_SERVER=127.0.0.1:7000 vendor/bin/phpunit tests/Chaos/ChaosTest.php
```

## License

Apache-2.0, the same licence as [Rostam](https://github.com/rostamlabs/rostam)
itself — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
