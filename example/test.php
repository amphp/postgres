#!/usr/bin/env php
<?php

require dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\execute(function () {
    /** @var \Amp\Postgres\Connection $connection */
    $connection = yield Postgres\connect('host=localhost user=postgres');
    
    /** @var \Amp\Postgres\Statement $statement */
    $statement = yield $connection->prepare('SHOW ALL');
    
    /** @var \Amp\Postgres\TupleResult $result */
    $result = yield $statement->execute();
    
    while (yield $result->next()) {
        $row = $result->getCurrent();
        \printf("%-35s = %s (%s)\n", $row['name'], $row['setting'], $row['description']);
    }
});
