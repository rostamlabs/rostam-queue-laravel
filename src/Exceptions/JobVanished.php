<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Exceptions;

use RuntimeException;

/**
 * Raised when a pop steps over more empty slots than any healthy queue has.
 *
 * A slot below the tail that holds nothing has two causes. One is benign: a
 * worker claimed the job and died before it could move the cursor, so the next
 * pop steps over the space it left. The other is not: the engine evicted the
 * job, which on `PolicyRingbufEvict` is what happens to anything that has been
 * sitting long enough to become the oldest entry.
 *
 * Nothing on the wire tells the two apart, and one worker dying leaves one
 * hole. A run of them does not; that is eviction, and it means jobs the caller
 * was promised are gone. Failing loudly is the only useful thing left to do -
 * a queue that quietly skips missing work looks healthy while it loses.
 */
class JobVanished extends RuntimeException
{
    public static function tooManyHoles(string $queue, int $holes): self
    {
        return new self(sprintf(
            'queue [%s]: stepped over %d empty slots in a single pop. One is a worker that '
            .'died mid-claim; this many is the store dropping jobs. Rostam evicts by write '
            .'order under its default PolicyRingbufEvict, and a queued job - written once, '
            .'read once - is exactly what it reaches first. Run the server with '
            .'PolicyRejectWrites and enough memory for the backlog.',
            $queue,
            $holes,
        ));
    }
}
