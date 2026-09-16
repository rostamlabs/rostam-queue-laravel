<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

/*
 * One process in the chaos test: a producer or a worker, talking to a real
 * server over a real socket, and liable to be killed by the parent at any
 * instant - including between two round trips of a single queue operation.
 *
 * Usage:
 *   php chaos-worker.php produce <host:port> <prefix> <logDir> <name> <count>
 *   php chaos-worker.php work    <host:port> <prefix> <logDir> <name> <stopFile>
 */

use Illuminate\Container\Container;
use Illuminate\Support\Carbon;
use Rostam\Kv\TcpClient;
use Rostam\Queue\RostamQueue;

require dirname(__DIR__, 2).'/vendor/autoload.php';

[, $role, $target, $prefix, $logDir, $name] = $argv;
[$host, $port] = explode(':', $target);

$queue = new RostamQueue(
    TcpClient::fromArray(['host' => $host, 'port' => (int) $port, 'timeout' => 10.0]),
    $prefix,
    'default',
    retryAfter: 2,
    sweepSeconds: 60,
);
$queue->setContainer(new Container);
$queue->setConnectionName('rostam');

// Appends one line and flushes it before returning. What matters is only that
// a line, once returned from, is on disk: a process killed mid-write leaves at
// most a partial last line, which the parent ignores.
$log = static function (string $file, string $line) use ($logDir): void {
    file_put_contents($logDir.'/'.$file, $line."\n", FILE_APPEND | LOCK_EX);
};

if ($role === 'produce') {
    $count = (int) $argv[6];

    for ($i = 1; $i <= $count; $i++) {
        $id = $name.'-'.$i;
        $payload = json_encode(['id' => $id, 'attempts' => 0]);
        $roll = mt_rand(1, 100);

        // A spread of every enqueue path: straight to ready, due shortly,
        // and due in the past - the case that used to be lost outright.
        match (true) {
            $roll <= 55 => $queue->pushRaw($payload),
            $roll <= 85 => $queue->laterRaw(mt_rand(0, 2), $payload),
            default => $queue->laterRaw(Carbon::now()->subSeconds(mt_rand(1, 30)), $payload),
        };

        // Logged only after the enqueue returned: a job is "produced" once the
        // queue has accepted it, and never before.
        $log('produced-'.$name.'.log', $id);

        if (mt_rand(1, 10) === 1) {
            usleep(mt_rand(0, 20_000));
        }
    }

    exit(0);
}

$stopFile = $argv[6];

while (! file_exists($stopFile)) {
    $job = $queue->pop();

    if ($job === null) {
        usleep(30_000);

        continue;
    }

    $id = json_decode($job->getRawBody(), true)['id'];

    // Logged the moment it is handed over. A kill between pop() returning and
    // this line leaves the job leased and unlogged; it comes back after the
    // lease and is logged then, so a kill here cannot fake a loss.
    $log('taken-'.$name.'.log', $id);

    $roll = mt_rand(1, 100);

    if ($roll <= 12) {
        // Dies holding it, without finishing: exactly what a crashed worker leaves.
        exit(3);
    }

    if ($roll <= 22) {
        $job->release(mt_rand(0, 1));

        continue;
    }

    // The record that this job was handled goes into the STORE, and before the
    // job is deleted rather than after. A log file cannot carry it: a worker
    // killed while appending its line loses the record for a job it really did
    // finish, and the parent cannot tell that from a job the queue dropped.
    // Here a kill between the mark and the delete leaves the job to be
    // redelivered and handled again, which is what at-least-once allows.
    // With a TTL: on a server shared with anything else these marks are the
    // test's litter, and this driver refuses a node that has ever evicted a live
    // record - so it must not be the thing filling one up.
    $queue->getClient()->put($prefix.'handled:'.$id, '1', 3600);

    $job->delete();
}

exit(0);
