<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

/**
 * Every fix in this driver is a claim that some job would otherwise be lost.
 * A test that passes proves nothing on its own - it has to FAIL when the fix is
 * taken away, or it is only keeping the current behaviour company.
 *
 * Each entry below undoes one fix. `run.php` applies it, runs the suite, and
 * expects the suite to go red. `find` must match exactly once, so a mutant that
 * stops applying is an error rather than a silent pass: the harness cannot rot
 * into a green number while the code moves out from under it.
 *
 * `expect` is 'killed' unless the mutation cannot change observable behaviour,
 * in which case it is 'survives' WITH the argument for why. A surviving mutant
 * with no such argument is a hole in the tests.
 *
 * @return list<array{id: string, why: string, file: string, find: string, replace: string, expect?: string}>
 */
return [
    // ---- pop: who owns an empty slot ------------------------------------
    [
        'id' => 'M01',
        'why' => 'a pop that steps over an empty slot without killing it lets a slow push land behind the reader',
        'file' => 'src/RostamQueue.php',
        'find' => 'if ($this->client->setNx($key, self::TOMBSTONE, $this->tombstoneTtl) && $head->advanceFrom($at)) {',
        'replace' => 'if ($head->advanceFrom($at)) {',
    ],
    [
        'id' => 'M02',
        'why' => 'a run of vanished records must be reported, not absorbed one slot at a time',
        'file' => 'src/RostamQueue.php',
        'find' => 'if (++$killed >= self::HOLE_RUN) {',
        'replace' => 'if (++$killed >= PHP_INT_MAX) {',
    ],
    [
        'id' => 'M03',
        'why' => 'without the re-read under the lease, a job finished between the look and the lease runs twice',
        'file' => 'src/RostamQueue.php',
        'find' => '            $current = $this->client->get($key);',
        'replace' => '            $current = $payload;',
    ],
    [
        'id' => 'M04',
        'why' => 'skipping a tombstone early saves a lease; it is not what stops one being handed out. '
            .'The read again under the lease sees the tombstone too and lets the slot go, so removing this '
            .'costs a round trip rather than a wrong delivery - M03 takes the guard that matters',
        'file' => 'src/RostamQueue.php',
        'find' => '            if ($payload === self::TOMBSTONE) {
                $head->advanceFrom($at);',
        'replace' => '            if (false) {
                $head->advanceFrom($at);',
        'expect' => 'survives',
    ],

    // ---- reclaim: the window and its cursor ------------------------------
    [
        'id' => 'M05',
        'why' => 'a reclaim window reaching above the reader takes jobs nobody has been given yet',
        'file' => 'src/RostamQueue.php',
        'find' => '$end = min($start + $this->reclaimBatch, $this->head($queue)->value());',
        'replace' => '$end = $start + $this->reclaimBatch;',
    ],
    [
        'id' => 'M06',
        'why' => 'the reclaim cursor must stop at the first job still in flight, or that slot is never looked at again',
        'file' => 'src/RostamQueue.php',
        'find' => '            if (! $settled) {',
        'replace' => '            if (false) {',
    ],
    [
        'id' => 'M06b',
        'why' => '$settledSoFar saves round trips; it is not what keeps the cursor behind an unsettled slot. '
            .'An unsettled id returns before reaching this line, and advanceFrom() is a compare-and-swap from '
            .'the slot below, so the cursor can only ever step, never jump over one. Both guards have to go '
            .'before anything is lost - M06 takes the one that matters',
        'file' => 'src/RostamQueue.php',
        'find' => '            if ($settledSoFar) {
                $cursor->advanceFrom($id - 1);',
        'replace' => '            if (true) {
                $cursor->advanceFrom($id - 1);',
        'expect' => 'survives',
    ],

    // ---- redelivery: putting an abandoned job back -----------------------
    [
        'id' => 'M07',
        'why' => 'two workers passing the same abandoned job both requeue it unless the lease decides',
        'file' => 'src/RostamQueue.php',
        'find' => '        if (! $lease->take($this->retryAfter)) {
            return false;
        }',
        'replace' => '        if (false) {
            return false;
        }',
    ],
    [
        'id' => 'M08',
        'why' => 'a redelivery that does not count as an attempt retries a worker-killing job forever',
        'file' => 'src/RostamQueue.php',
        'find' => '$this->enqueue($queue, self::withAnotherAttempt($payload));',
        'replace' => '$this->enqueue($queue, $payload);',
    ],
    [
        'id' => 'M09',
        'why' => 'deleting before copying loses the job outright if the worker dies between the two',
        'file' => 'src/RostamQueue.php',
        'find' => '            $this->enqueue($queue, self::withAnotherAttempt($payload));
            $this->client->del($key);',
        'replace' => '            $this->client->del($key);
            $this->enqueue($queue, self::withAnotherAttempt($payload));',
    ],

    // ---- delayed jobs: the seal and the watermark ------------------------
    [
        'id' => 'M10',
        'why' => 'a push ignoring the seal stores its job in a second the sweep has already counted',
        'file' => 'src/RostamQueue.php',
        'find' => '        if (($id & self::SEALED) !== 0) {',
        'replace' => '        if (false) {',
    ],
    [
        'id' => 'M11',
        'why' => 'without the post-store watermark check, a job stored just behind the sweep is never moved',
        'file' => 'src/RostamQueue.php',
        'find' => '        if ($due <= $this->swept($queue)) {
            $moved = $this->moveDelayed($queue, $due, $id);',
        'replace' => '        if (false) {
            $moved = $this->moveDelayed($queue, $due, $id);',
    ],
    [
        'id' => 'M12',
        'why' => 'the ready-queue fallback is what saves a job whose self-move lost to another worker',
        'file' => 'src/RostamQueue.php',
        'find' => '            if (! $moved) {',
        'replace' => '            if (false) {',
    ],
    [
        'id' => 'M13',
        'why' => 'an unwritten bucket slot left alive lets the push store a job into a swept second',
        'file' => 'src/RostamQueue.php',
        'find' => '                if ($this->client->setNx($key, self::TOMBSTONE, $this->tombstoneTtl)) {
                    continue;
                }',
        'replace' => '                if (true) {
                    continue;
                }',
    ],
    [
        'id' => 'M14',
        'why' => 'marking a second swept while a job in it belongs to another worker abandons that job',
        'file' => 'src/RostamQueue.php',
        'find' => '            if (! $this->migrateSecond($queue, $second)) {',
        'replace' => '            if (false) {',
    ],
    [
        'id' => 'M15',
        'why' => 'moving a delayed job without its lease lets two sweeps deliver it twice',
        'file' => 'src/RostamQueue.php',
        'find' => '        if (! $move->take($this->retryAfter)) {
            return false;
        }',
        'replace' => '        if (false) {
            return false;
        }',
    ],
    [
        'id' => 'M16',
        'why' => 'the second of two guards that fail differently, and today the first one covers it. '
            .'The generation is read HERE, under the lease, so it can never be older than the one the job '
            .'was written under - which makes `<` and `!==` the same comparison. It stays `<` because the '
            .'day that generation is read earlier, or once per sweep, `!==` would delete a job delayed '
            .'AFTER the clear and `<` still would not',
        'file' => 'src/RostamQueue.php',
        'find' => 'if ($writtenUnder !== null && $writtenUnder < $this->delayedGeneration($queue)) {',
        'replace' => 'if ($writtenUnder !== null && $writtenUnder !== $this->delayedGeneration($queue)) {',
        'expect' => 'survives',
    ],
    [
        'id' => 'M17',
        'why' => 'copy-then-delete is the whole reason a crash mid-move leaves two jobs and never none',
        'file' => 'src/RostamQueue.php',
        'find' => '                $this->enqueue($queue, $payload);
                $this->client->del($key);',
        'replace' => '                $this->client->del($key);
                $this->enqueue($queue, $payload);',
    ],

    // ---- clear ------------------------------------------------------------
    [
        'id' => 'M18',
        'why' => 'killing only written slots lets a push in flight land behind the reader, after the clear',
        'file' => 'src/RostamQueue.php',
        'find' => 'if ($slot > $reader || $payload !== null) {',
        'replace' => 'if ($payload !== null) {',
    ],
    [
        'id' => 'M19',
        'why' => 'tombstoning every slot since reclaim writes a key per finished job on an empty queue',
        'file' => 'src/RostamQueue.php',
        'find' => 'if ($slot > $reader || $payload !== null) {',
        'replace' => 'if (true) {',
    ],
    [
        'id' => 'M20',
        'why' => 'reading the batch positionally trusts the client to answer in the order it was asked',
        'file' => 'src/RostamQueue.php',
        'find' => '                $payload = $payloads[$key] ?? null;',
        'replace' => '                $payload = array_values($payloads)[$slot - $id] ?? null;',
    ],
    [
        'id' => 'M21',
        'why' => 'without a new generation, jobs delayed before the clear still come due after it',
        'file' => 'src/RostamQueue.php',
        'find' => "        \$this->client->increment(\$this->key(\$queue, 'dgen'));",
        'replace' => '        // generation not bumped',
    ],
    [
        'id' => 'M22',
        'why' => 'the reader sits ON the slot it last handed out, so that slot is already spoken for; '
            .'tombstoning it when it holds no payload costs one key that outlives nothing and then expires',
        'file' => 'src/RostamQueue.php',
        'find' => 'if ($slot > $reader || $payload !== null) {',
        'replace' => 'if ($slot >= $reader || $payload !== null) {',
        'expect' => 'survives',
    ],
];
