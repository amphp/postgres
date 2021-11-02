#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;
use function Amp\defer;
use function Amp\delay;

$config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

$pool = Postgres\pool($config);

$channel = "test";

$listener = $pool->listen($channel);

\printf("Listening on channel '%s'\n", $listener->getChannel());

defer(function () use ($pool, $channel, $listener): void {
    delay(1000);

    $pool->notify($channel, "Data 1"); // Send first notification.

    delay(1000);

    $pool->notify($channel, "Data 2"); // Send second notification.

    delay(1000);

    $listener->unlisten();
});

foreach ($listener as $notification) {
    \printf(
        "Received notification from PID %d on channel '%s' with payload: %s\n",
        $notification->pid,
        $notification->channel,
        $notification->payload
    );
}

$pool->close();