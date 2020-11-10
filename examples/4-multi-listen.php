#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;
use Amp\Pipeline;
use function Amp\defer;
use function Amp\delay;

$config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

$pool = Postgres\pool($config);

$channel1 = "test1";
$channel2 = "test2";

$listener1 = $pool->listen($channel1);

\printf("Listening on channel '%s'\n", $listener1->getChannel());

$listener2 = $pool->listen($channel2);

\printf("Listening on channel '%s'\n", $listener2->getChannel());

defer(function () use ($pool, $listener1, $listener2, $channel1, $channel2): void {
    $pool->notify($channel1, "Data 1.1");

    delay(500);

    $pool->notify($channel2, "Data 2.1");

    delay(500);

    $pool->notify($channel2, "Data 2.2");

    delay(500);

    \printf("Unlistening from channel '%s'\n", $listener2->getChannel());
    $listener2->unlisten();

    delay(500);

    $pool->notify($channel1, "Data 1.2");

    delay(500);

    \printf("Unlistening from channel '%s'\n", $listener1->getChannel());
    $listener1->unlisten();
});

$listener = Pipeline\merge([$listener1, $listener2]); // Merge both listeners into single pipeline.

foreach ($listener as $notification) {
    \printf(
        "Received notification from PID %d on channel '%s' with payload: %s\n",
        $notification->pid,
        $notification->channel,
        $notification->payload
    );
}

$pool->close();
