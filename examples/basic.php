#!/usr/bin/env php
<?php

require dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $pool = Postgres\pool('host=localhost user=postgres');
    
    /** @var \Amp\Postgres\Statement $statement */
    $statement = yield $pool->prepare('SHOW ALL');
    
    /** @var \Amp\Postgres\TupleResult $result */
    $result = yield $statement->execute();
    
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        \printf("%-35s = %s (%s)\n", $row['name'], $row['setting'], $row['description']);
    }
});
