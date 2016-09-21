#!/usr/bin/env php
<?php

require dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\execute(function () {
    $pool = Postgres\pool('host=localhost user=postgres');
    
    $channel = "test";
    
    /** @var \Amp\Postgres\Listener $listener */
    $listener = yield $pool->listen("test");
    
    printf("Listening on channel '%s'\n", $listener->getChannel());
    
    Amp\delay(3000, function () use ($listener) {
        printf("Unlistening from channel '%s'\n", $listener->getChannel());
        yield $listener->unlisten();
    });
    
    Amp\delay(1000, function () use ($pool, $channel) {
        yield $pool->notify($channel, "Data 1");
    });
    
    Amp\delay(2000, function () use ($pool, $channel) {
        yield $pool->notify($channel, "Data 2");
    });

    while (yield $listener->next()) {
        /** @var \Amp\Postgres\Notification $notification */
        $notification = $listener->getCurrent();
        printf(
            "Received notification from PID %d on channel '%s' with payload: %s\n",
            $notification->pid,
            $notification->channel,
            $notification->payload
        );
    }
});
