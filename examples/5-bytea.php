#!/usr/bin/env php
<?php

require \dirname(__DIR__) . '/vendor/autoload.php';

use Amp\Postgres\ByteA;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;

$config = PostgresConfig::fromString('host=localhost user=postgres');
$pool = new PostgresConnectionPool($config);

$pool->query('DROP TABLE IF EXISTS test');

$transaction = $pool->beginTransaction();

$transaction->query('CREATE TABLE test (value BYTEA)');

$statement = $transaction->prepare('INSERT INTO test VALUES (?)');

$statement->execute([new ByteA($a = \random_bytes(10))]);
$statement->execute([new ByteA($b = \random_bytes(10))]);
$statement->execute([new ByteA($c = \random_bytes(10))]);

$result = $transaction->execute('SELECT * FROM test WHERE value = :value', ['value' => new ByteA($a)]);

foreach ($result as $row) {
    assert($row['value'] === $a);
    var_dump(bin2hex($row['value']));
}

$transaction->rollback();
