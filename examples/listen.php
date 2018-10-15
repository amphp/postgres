#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Loop;
use Amp\Postgres;

Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

    $pool = Postgres\pool($config);

    $channel = "test";

    /** @var \Amp\Postgres\Listener $listener */
    $listener = yield $pool->listen($channel);

    \printf("Listening on channel '%s'\n", $listener->getChannel());

    Loop::delay(3000, function () use ($listener) { // Unlisten in 3 seconds.
        \printf("Unlistening from channel '%s'\n", $listener->getChannel());
        return $listener->unlisten();
    });

    Loop::delay(1000, function () use ($pool, $channel) {
        return $pool->notify($channel, "Data 1"); // Send first notification.
    });

    Loop::delay(2000, function () use ($pool, $channel) {
        return $pool->notify($channel, "Data 2"); // Send second notification.
    });

    while (yield $listener->advance()) {
        $notification = $listener->getCurrent();
        \printf(
            "Received notification from PID %d on channel '%s' with payload: %s\n",
            $notification->pid,
            $notification->channel,
            $notification->payload
        );
    }
});
