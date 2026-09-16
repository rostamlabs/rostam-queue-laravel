<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Support;

use RuntimeException;

/**
 * Thrown by a hook to stand in for a worker process killed at that instant:
 * nothing after that point in the calling code gets to run.
 */
final class WorkerDied extends RuntimeException {}
