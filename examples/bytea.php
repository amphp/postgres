#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;
use Amp\Postgres\ByteA;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

    $pool = Postgres\pool($config);

    yield $pool->query('DROP TABLE IF EXISTS test');

    /** @var \Amp\Postgres\Transaction $transaction */
    $transaction = yield $pool->beginTransaction();

    yield $transaction->query('CREATE TABLE test (val BYTEA)');

    /** @var \Amp\Sql\Statement $statement */
    $statement = yield $transaction->prepare('INSERT INTO test VALUES (?)');

    yield $statement->execute([new ByteA($a = \random_bytes(10))]);
    yield $statement->execute([new ByteA($b = \random_bytes(10))]);
    yield $statement->execute([new ByteA($c = \random_bytes(10))]);

    /** @var \Amp\Postgres\ResultSet $result */
    $result = yield $transaction->execute('SELECT * FROM test WHERE val = :val', ['val' => new ByteA($a)]);

    while (yield $result->advance()) {
        $row = $result->getCurrent();
        \assert($row['val'] === $a);
    }

    yield $transaction->rollback();
});
