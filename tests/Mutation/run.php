<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

/**
 * Runs the mutation campaign in mutants.php.
 *
 *     php tests/Mutation/run.php            all of them
 *     php tests/Mutation/run.php M07 M18    just these
 *
 * Exit status is 0 only when every mutant did what it said it would: the ones
 * that claim to break something made the suite fail, and the ones documented as
 * unobservable did not. Anything else - including a `find` that no longer
 * matches exactly once - is a failure, because the point of this file is that
 * it cannot quietly stop testing.
 */
$root = dirname(__DIR__, 2);
$only = array_slice($argv, 1);

/** @var list<array{id: string, why: string, file: string, find: string, replace: string, expect?: string}> $mutants */
$mutants = require __DIR__.'/mutants.php';

if ($only !== []) {
    $mutants = array_values(array_filter($mutants, fn (array $m) => in_array($m['id'], $only, true)));

    if (count($mutants) !== count($only)) {
        fwrite(STDERR, 'no such mutant in: '.implode(' ', $only)."\n");
        exit(2);
    }
}

$phpunit = $root.'/vendor/bin/phpunit';

if (! is_file($phpunit)) {
    fwrite(STDERR, "vendor/bin/phpunit is missing - run composer install first\n");
    exit(2);
}

$originals = [];
$failures = [];

// Whatever happens - a mutant that will not apply, a fatal, Ctrl-C - the tree
// goes back exactly as it was. A mutation harness that can leave a mutation
// behind is worse than none.
$restore = function () use (&$originals): void {
    foreach ($originals as $path => $source) {
        file_put_contents($path, $source);
    }

    $originals = [];
};

register_shutdown_function($restore);

if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, function () use ($restore): void {
        $restore();
        exit(130);
    });
}

printf("%d mutants\n\n", count($mutants));

foreach ($mutants as $mutant) {
    $path = $root.'/'.$mutant['file'];
    $source = file_get_contents($path);

    if ($source === false) {
        $failures[] = $mutant['id'].': cannot read '.$mutant['file'];
        printf("%-5s ERROR  cannot read %s\n", $mutant['id'], $mutant['file']);

        continue;
    }

    $occurrences = substr_count($source, $mutant['find']);

    if ($occurrences !== 1) {
        $failures[] = $mutant['id'].': `find` matched '.$occurrences.' times, expected exactly 1';
        printf("%-5s STALE  matched %d times - this mutant no longer applies\n", $mutant['id'], $occurrences);

        continue;
    }

    $originals[$path] = $source;
    file_put_contents($path, str_replace($mutant['find'], $mutant['replace'], $source));

    $status = 0;
    exec(escapeshellarg(PHP_BINARY).' '.escapeshellarg($phpunit).' 2>&1', $output, $status);

    file_put_contents($path, $source);
    unset($originals[$path]);

    $survived = $status === 0;
    $expected = ($mutant['expect'] ?? 'killed') === 'survives';

    if ($survived === $expected) {
        printf("%-5s %-8s %s\n", $mutant['id'], $survived ? 'survives' : 'killed', $mutant['why']);

        continue;
    }

    $failures[] = $survived
        ? $mutant['id'].': SURVIVED - nothing failed when '.$mutant['why']
        : $mutant['id'].': killed, but documented as unobservable';

    printf("%-5s %-8s %s\n", $mutant['id'], $survived ? 'SURVIVED' : 'KILLED??', $mutant['why']);
}

$restore();

printf("\n%d mutants, %d unexpected\n", count($mutants), count($failures));

foreach ($failures as $failure) {
    printf("  - %s\n", $failure);
}

exit($failures === [] ? 0 : 1);
