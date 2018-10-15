#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres');

    $pool = Postgres\pool($config);

    yield $pool->query('DROP TABLE IF EXISTS test');

    /** @var \Amp\Postgres\Transaction $transaction */
    $transaction = yield $pool->beginTransaction();

    yield $transaction->query('CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))');

    /** @var \Amp\Sql\Statement $statement */
    $statement = yield $transaction->prepare('INSERT INTO test VALUES (?, ?)');

    yield $statement->execute(['amphp', 'org']);
    yield $statement->execute(['google', 'com']);
    yield $statement->execute(['github', 'com']);

    /** @var \Amp\Postgres\ResultSet $result */
    $result = yield $transaction->execute('SELECT * FROM test WHERE tld = :tld', ['tld' => 'com']);

    $format = "%-20s | %-10s\n";
    \printf($format, 'TLD', 'Domain');
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        \printf($format, $row['domain'], $row['tld']);
    }

    yield $transaction->rollback();
});
