#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;
use Amp\Postgres\Connection;
use Amp\Sql\Result;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

    /** @var Connection $connection */
    $connection = yield Postgres\connect($config);

    /** @var Result $result */
    $result = yield $connection->query('SHOW ALL');

    while ($row = yield $result->continue()) {
        \printf("%-35s = %s (%s)\n", $row['name'], $row['setting'], $row['description']);
    }
});
