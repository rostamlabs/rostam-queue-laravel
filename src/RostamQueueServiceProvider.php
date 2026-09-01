<?php

// SPDX-License-Identifier: Apache-2.0

declare(strict_types=1);

namespace Rostam\Queue;

use Illuminate\Contracts\Config\Repository as Config;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Rostam\Queue\Connectors\RostamConnector;

class RostamQueueServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        /** @var QueueManager $manager */
        $manager = $this->app->make('queue');

        $manager->addConnector('rostam', function () {
            /** @var Config|null $config */
            $config = $this->app->bound('config') ? $this->app->make('config') : null;

            return new RostamConnector($config);
        });
    }
}
