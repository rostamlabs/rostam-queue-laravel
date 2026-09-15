<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Exceptions;

use RuntimeException;

/**
 * The store behind this queue cannot be trusted to keep the jobs it accepts.
 *
 * Raised before a job is accepted or taken: when the connection has not said
 * which safe setup the server is, and - on rostam v0.7.0-beta3 and newer - when
 * the node has already thrown live records away. A queue that runs and then
 * loses work is worse than one that will not run, so these refuse rather than
 * warn.
 */
class UnsafeQueueStore extends RuntimeException
{
    public static function policyNotDeclared(?string $declared): self
    {
        return new self(sprintf(
            'the rostam queue connection must declare at_cap_policy%s.'
            ."\n\n"
            .'A queue on Rostam is only as durable as the node never throwing records away. At '
            .'capacity a node either evicts - silently, with every write still answering success - '
            .'or refuses the write. A single-node rostam-server always evicts, and no flag changes '
            .'that; only replicated shards (-cluster) refuse. Losing a cache entry is a miss; losing '
            .'a job is work that was accepted and never done.'
            ."\n\n"
            .'Declare which of the two safe setups this is:'
            ."\n\n"
            .'  "reject_writes"  a -cluster deployment, or an embedded store configured with '
            .'PolicyRejectWrites. At capacity the node refuses and the push fails loudly.'
            ."\n"
            .'  "headroom"       a single node whose max_memory stays well above the most this queue '
            .'will ever hold, so it never reaches capacity. Needs rostam v0.7.0-beta3 or newer.'
            ."\n\n"
            .'Which setup the server is cannot be read off the wire - that part is your declaration. '
            .'What can be read, on v0.7.0-beta3 and newer, is a node that has already evicted live '
            .'records, and this driver will not run on one.',
            $declared === null ? '' : sprintf(' as "reject_writes" or "headroom" (got "%s")', $declared),
        ));
    }

    public static function liveRecordsEvicted(int $count): self
    {
        return new self(sprintf(
            'the rostam node behind this queue has evicted %d live record(s) since it started '
            .'(rostam_kv_evictions_live_total). This connection is declared never to lose records, so '
            .'either that declaration is wrong or the node ran out of memory - and jobs may already '
            .'be gone. Give the node a max_memory well above the backlog, move the queue to a node of '
            .'its own, or run a -cluster, which refuses writes instead of evicting. The count resets '
            .'only when the server restarts, and restarting to clear it does not bring back what was lost.',
            $count,
        ));
    }

    public static function cannotVerify(): self
    {
        return new self(
            'at_cap_policy "headroom" needs the node to report its evictions, and this one does not: '
            .'rostam_kv_evictions_live_total arrived in rostam v0.7.0-beta3. A single node that is never '
            .'checked can fill up and evict jobs without a sound. Upgrade the server, or run a -cluster '
            .'and declare "reject_writes".'
        );
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
