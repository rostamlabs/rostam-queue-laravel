<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Rostam\Queue\Cursor;
use Rostam\Testing\ArrayKvClient;

/**
 * The counters a queue is made of.
 *
 * Zero has two spellings on the wire - a key that was never written, and eight
 * zero bytes - and a cursor that knew only the first was wedged for good the
 * first time anything wrote the second. clear() no longer writes it, but the
 * cursor does not get to depend on that.
 */
class CursorTest extends TestCase
{
    public function test_it_moves_from_a_zero_that_was_never_written(): void
    {
        $client = new ArrayKvClient;
        $cursor = new Cursor($client, 'c');

        $this->assertTrue($cursor->advanceFrom(0));
        $this->assertSame(1, $cursor->value());
    }

    public function test_it_moves_from_a_zero_that_was_written_explicitly(): void
    {
        $client = new ArrayKvClient;
        $client->put('c', pack('J', 0));
        $cursor = new Cursor($client, 'c');

        $this->assertTrue($cursor->advanceFrom(0), 'an explicit zero wedged the cursor');
        $this->assertSame(1, $cursor->value());
    }

    public function test_only_one_of_two_callers_moves_it(): void
    {
        $client = new ArrayKvClient;
        $cursor = new Cursor($client, 'c');

        $this->assertTrue($cursor->advanceFrom(0));
        $this->assertFalse($cursor->advanceFrom(0), 'a stale caller moved the cursor a second time');
        $this->assertSame(1, $cursor->value());
    }

    /** A jump forward, never back: a stale target must not drag a cursor over ground already covered. */
    public function test_advancing_to_a_target_never_moves_it_back(): void
    {
        $client = new ArrayKvClient;
        $cursor = new Cursor($client, 'c');

        $cursor->advanceTo(10);
        $cursor->advanceTo(4);

        $this->assertSame(10, $cursor->value());
    }

    /** Nothing to do is nothing written - which is how clear() stopped storing an explicit zero. */
    public function test_advancing_to_where_it_already_is_writes_nothing(): void
    {
        $client = new ArrayKvClient;
        $cursor = new Cursor($client, 'c');

        $cursor->advanceTo(0);

        $this->assertArrayNotHasKey('c', $client->all());
    }
}
