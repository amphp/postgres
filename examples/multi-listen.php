#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Iterator;
use Amp\Loop;
use Amp\Postgres;

Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

    $pool = Postgres\pool($config);

    $channel1 = "test1";
    $channel2 = "test2";

    /** @var \Amp\Postgres\Listener $listener1 */
    $listener1 = yield $pool->listen($channel1);

    \printf("Listening on channel '%s'\n", $listener1->getChannel());

    /** @var \Amp\Postgres\Listener $listener2 */
    $listener2 = yield $pool->listen($channel2);

    \printf("Listening on channel '%s'\n", $listener2->getChannel());

    Loop::delay(6000, function () use ($listener1) { // Unlisten in 6 seconds.
        \printf("Unlistening from channel '%s'\n", $listener1->getChannel());
        return $listener1->unlisten();
    });

    Loop::delay(4000, function () use ($listener2) { // Unlisten in 4 seconds.
        \printf("Unlistening from channel '%s'\n", $listener2->getChannel());
        return $listener2->unlisten();
    });

    Loop::delay(1000, function () use ($pool, $channel1) {
        return $pool->notify($channel1, "Data 1.1");
    });

    Loop::delay(2000, function () use ($pool, $channel2) {
        return $pool->notify($channel2, "Data 2.1");
    });

    Loop::delay(3000, function () use ($pool, $channel2) {
        return $pool->notify($channel2, "Data 2.2");
    });

    Loop::delay(5000, function () use ($pool, $channel1) {
        return $pool->notify($channel1, "Data 1.2");
    });

    $iterator = Iterator\merge([$listener1, $listener2]); // Merge both listeners into single iterator.

    while (yield $iterator->advance()) {
        $notification = $iterator->getCurrent();
        \printf(
            "Received notification from PID %d on channel '%s' with payload: %s\n",
            $notification->pid,
            $notification->channel,
            $notification->payload
        );
    }
});
