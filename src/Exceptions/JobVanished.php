<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Exceptions;

use RuntimeException;

/**
 * Raised when the queue meets far more empty slots than a healthy one produces.
 *
 * An empty slot between the reader and the writer has two causes. One is
 * harmless: a push that has drawn its id and not yet written its payload. The
 * worker kills the slot, the push finds it taken and draws another id, and
 * nothing is lost. The other is not: a record that was written and then
 * vanished - evicted because the node reached capacity, or wiped by a flush -
 * which is a job the caller was promised and will not get.
 *
 * Nothing on the wire tells the two apart for one slot. But a push is caught
 * mid-write rarely and briefly; a run of empty slots in a single pop is the
 * store dropping jobs, and the only useful thing left is to say so loudly.
 */
class JobVanished extends RuntimeException
{
    public static function tooManyHoles(string $queue, int $holes): self
    {
        return new self(sprintf(
            'queue [%s]: met %d empty slots in a single pop. A push caught mid-write leaves one and '
            .'re-routes itself; a run like this means jobs were written and then vanished - evicted '
            .'because the node reached capacity, or wiped by a flush. On rostam v0.7.0-beta3 and newer, '
            .'check rostam_kv_evictions_live_total on the node: anything above zero is live records '
            .'thrown away. A single-node rostam-server always evicts at capacity; keep its max_memory '
            .'well above the backlog, or run a -cluster, which refuses writes instead.',
            $queue,
            $holes,
        ));
    }

    public static function cannotPlace(string $queue): self
    {
        return new self(sprintf(
            'queue [%s]: drew 64 ids in a row whose slots had already been killed. A push loses its '
            .'slot only when a worker reaches it between the push drawing the id and writing the '
            .'payload; sixty-four in a row means pushes are stalling far longer than a round trip, '
            .'or something other than a worker is writing into this queue\'s keys.',
            $queue,
        ));
    }
}
