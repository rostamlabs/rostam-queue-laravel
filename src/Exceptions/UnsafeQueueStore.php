<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Exceptions;

use RuntimeException;

/**
 * The store behind this queue cannot be trusted to keep the jobs it accepts.
 *
 * Raised before a job is accepted or taken: when the connection has not
 * declared the one setup that keeps records, when it names a deployment this
 * driver cannot run on safely, and - on rostam v0.7.0-beta3 and newer - when the
 * node has already thrown live records away. A queue that runs and then loses
 * work is worse than one that will not run, so these refuse rather than warn.
 */
class UnsafeQueueStore extends RuntimeException
{
    public static function policyNotDeclared(?string $declared): self
    {
        return new self(sprintf(
            'the rostam queue connection must declare at_cap_policy "headroom"%s.'
            ."\n\n"
            .'A queue on Rostam is only as durable as the node never throwing away a record it still '
            .'holds, and a single-node rostam-server does that in two ways: at capacity, silently, with '
            .'every write still answering success; and by default even with room to spare, because '
            .'eviction follows write order - a job that waits while other jobs churn past it is evicted '
            .'when the buffer wraps. Losing a cache entry is a miss; losing a job is work that was '
            .'accepted and never done.'
            ."\n\n"
            .'"headroom" declares the setup that keeps them:'
            ."\n\n"
            .'  - a single rostam-server started with -relocating-eviction, which copies a page\'s '
            .'still-live records forward instead of dropping them with it - best-effort, since it never '
            .'allocates a page or fails a write, so a record that does not fit the room left over goes;'
            ."\n"
            .'  - its max_memory well above everything live on it - the backlog, delayed jobs, and '
            .'anything else sharing the server. Measured, a live set of a quarter of the budget held; '
            .'half did not;'
            ."\n"
            .'  - rostam v0.7.0-beta3 or newer, so the node can report evictions.'
            ."\n\n"
            .'The first two cannot be read off the wire - that part is your declaration. What can be read '
            .'is a node that has already evicted live records, and this driver will not run on one.',
            $declared === null ? '' : sprintf(' (got "%s")', $declared),
        ));
    }

    public static function clusterUnsupported(): self
    {
        return new self(
            'at_cap_policy "reject_writes" names what a replicated -cluster does, and this driver does '
            .'not support one. A cluster refuses '
            .'writes at capacity instead of evicting, but it answers a read from whichever replica '
            .'received it, and this driver reads a job\'s absence as that job being finished: a lagging '
            .'replica would drop a job without a sound. Run the queue on a single rostam-server with '
            .'-relocating-eviction and declare "headroom".'
        );
    }

    public static function liveRecordsEvicted(int $count): self
    {
        return new self(sprintf(
            'the rostam node behind this queue has evicted %d live record(s) since it started '
            .'(rostam_kv_evictions_live_total), so jobs may already be gone. Start the server with '
            .'-relocating-eviction, keep its max_memory well above everything live on it, or move the '
            .'queue to a node of its own. This worker will not take or accept jobs again until it is '
            .'restarted. The count resets only when the server restarts, and restarting to clear it '
            .'does not bring back what was lost.',
            $count,
        ));
    }

    public static function cannotVerify(): self
    {
        return new self(
            'at_cap_policy "headroom" needs the node to report its evictions, and this one did not: '
            .'rostam_kv_evictions_live_total arrived in rostam v0.7.0-beta3. A single node that is never '
            .'checked can evict jobs without a sound. Upgrade the server. (This is asked again on the '
            .'next operation, so a server that failed to answer once is not refused for good.)'
        );
    }

    public static function tombstonesOutliveLeases(int $tombstoneTtl, int $retryAfter): self
    {
        return new self(sprintf(
            'rostam queue: tombstone_ttl (%d) must be longer than retry_after (%d).'
            ."\n\n"
            .'A killed slot is what stops a push, or a delayed job\'s bucket slot, from coming back to '
            .'life after the queue has moved past it - and a lease lasts retry_after, including the '
            .'lease of a worker that died holding it. With the shorter of the two on the tombstone, a '
            .'slot can be re-used while that lease is still held, and the job written into it is the '
            .'one nothing comes back for.',
            $tombstoneTtl,
            $retryAfter,
        ));
    }

    public static function unknownEvictionPolicy(string $given): self
    {
        return new self(sprintf(
            'rostam queue: on_evictions must be "refuse" or "ignore", got "%s". "refuse" (the default) '
            .'stops the queue when the node reports it has evicted live records; "ignore" runs without '
            .'the check, which is only reasonable on a node whose eviction count is somebody else\'s - '
            .'and accepts that this queue will not notice the day it loses a job.',
            $given,
        ));
    }

    public static function noConnection(string $name): self
    {
        return new self(sprintf(
            'rostam queue: no connection named [%s]. Add it under rostam.connections, or put '
            .'host and port directly on the queue connection.',
            $name,
        ));
    }
}
