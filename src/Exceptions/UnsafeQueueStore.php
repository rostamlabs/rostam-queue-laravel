<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Exceptions;

use RuntimeException;

/**
 * Raised at connect time, before a single job can be accepted.
 *
 * A queue that starts and then loses work is worse than one that will not
 * start, so both of these refuse rather than warn.
 */
class UnsafeQueueStore extends RuntimeException
{
    public static function policyNotDeclared(?string $declared): self
    {
        return new self(sprintf(
            'the rostam queue connection must declare at_cap_policy = "reject_writes"%s. '
            ."\n\n"
            .'Rostam evicts by write order under its default PolicyRingbufEvict, and a queued '
            .'job - written once, read once - is the first thing that reaches. Losing a cache '
            .'entry is a miss; losing a job is work that was accepted and never done.'
            ."\n\n"
            .'No op reports the server\'s cache policy, so this driver cannot check it and will '
            .'not pretend to. Start the server with PolicyRejectWrites and enough memory for '
            .'the backlog, then say so here - the declaration is yours, and it is the only '
            .'thing standing between a full queue and silent loss.',
            $declared === null ? '' : sprintf(' (got "%s")', $declared),
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
