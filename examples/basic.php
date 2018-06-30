#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    /** @var \Amp\Postgres\Connection $connection */
    $connection = yield Postgres\connect(new Postgres\ConnectionConfig('host=localhost user=postgres'));

    /** @var \Amp\Postgres\ResultSet $result */
    $result = yield $connection->query('SHOW ALL');

    while (yield $result->advance()) {
        $row = $result->getCurrent();
        \printf("%-35s = %s (%s)\n", $row['name'], $row['setting'], $row['description']);
    }
});
