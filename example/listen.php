#!/usr/bin/env php
<?php

require dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\execute(function () {
    /** @var \Amp\Postgres\Connection $connection */
    $connection = yield Postgres\connect('host=localhost user=postgres');
    
    /** @var \Amp\Postgres\Listener $listener */
    $listener = yield $connection->listen("test");
    
    yield $connection->query("NOTIFY test, 'Data 1'");
    yield $connection->query("NOTIFY test, 'Data 2'");
    
    while (yield $listener->next()) {
        /** @var \Amp\Postgres\Notification $notification */
        $notification = $listener->getCurrent();
        \printf(
            "Received notification from PID %d on channel %s with payload: %s\n",
            $notification->pid,
            $notification->channel,
            $notification->payload
        );
    }
});
